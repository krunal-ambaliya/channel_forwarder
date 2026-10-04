import os
import re
import asyncio
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List, Union, Callable
from telethon import TelegramClient, events
from telethon.network.connection import ConnectionTcpAbridged
from telethon.tl.types import (
    Channel, Chat, User, MessageMediaPoll, MessageMediaWebPage,
    MessageMediaUnsupported, MessageMediaContact, MessageMediaGeo,
    ChatInviteAlready, ChatInvite
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest
from telethon.utils import get_peer_id
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeInvalidError, PhoneCodeExpiredError,
    PasswordHashInvalidError, FloodWaitError, ChatAdminRequiredError,
    ChatWriteForbiddenError, ChannelPrivateError, UserAlreadyParticipantError
)
from config import SESSION_NAME, TEMP_DOWNLOAD_DIR, load_config, save_config
from fast_transfer import fast_download_file, fast_upload_file
from wzgram_service import wzgram_engine

try:
    import cryptg
    logger_msg = "[TeleForward] cryptg acceleration: ACTIVE (Hardware-accelerated C MTProto AES-IGE)"
except ImportError:
    logger_msg = "[TeleForward] cryptg acceleration: NOT INSTALLED (falling back to slow pure-python AES)"

logger = logging.getLogger("telegram_forwarder")
print(logger_msg)

def parse_telegram_message_link(text: str) -> Dict[str, Any]:
    """
    Parses a telegram message link or message ID.
    Supports:
    - https://t.me/c/4321038948/13906 (private channel message link)
    - https://t.me/c/4321038948/13906?single
    - https://t.me/c/4321038948/2/13906 (forum topic thread message link)
    - https://t.me/username/13906 (public channel message link)
    - t.me/c/4321038948/13906
    - tg://privatepost?channel=4321038948&post=13906
    - tg://resolve?domain=username&post=13906
    - 13906 (standalone ID)
    Returns:
    {
        "channel_identifier": str or None,
        "channel_id": int or None,
        "message_id": int
    }
    """
    text = str(text or "").strip()
    if not text:
        return {"channel_identifier": None, "channel_id": None, "message_id": 0}

    if text.isdigit():
        return {"channel_identifier": None, "channel_id": None, "message_id": int(text)}

    clean = text.split("?")[0].split("#")[0].rstrip("/")

    for prefix in ["https://", "http://"]:
        if clean.startswith(prefix):
            clean = clean[len(prefix):]

    if text.startswith("tg://"):
        m_priv = re.search(r"channel=(\d+)&post=(\d+)", text)
        if m_priv:
            cid = int(f"-100{m_priv.group(1)}")
            return {"channel_identifier": str(cid), "channel_id": cid, "message_id": int(m_priv.group(2))}
        m_pub = re.search(r"domain=([^&]+)&post=(\d+)", text)
        if m_pub:
            return {"channel_identifier": m_pub.group(1), "channel_id": None, "message_id": int(m_pub.group(2))}

    # Match t.me/c/<channel_id>/<optional_topic_id>/<msg_id>
    m_c = re.search(r"(?:^|t\.me\/)c\/(\d+)(?:\/\d+)?\/(\d+)$", clean)
    if m_c:
        cid_str = m_c.group(1)
        cid = int(f"-100{cid_str}")
        msg_id = int(m_c.group(2))
        return {"channel_identifier": str(cid), "channel_id": cid, "message_id": msg_id}

    # Match t.me/<username>/<optional_topic_id>/<msg_id>
    m_u = re.search(r"(?:^|t\.me\/)([a-zA-Z0-9_]{3,})(?:\/\d+)?\/(\d+)$", clean)
    if m_u:
        uname = m_u.group(1)
        msg_id = int(m_u.group(2))
        return {"channel_identifier": uname, "channel_id": None, "message_id": msg_id}

    return {"channel_identifier": None, "channel_id": None, "message_id": 0}

class TelegramService:
    def __init__(self):
        self.client: Optional[TelegramClient] = None
        self.api_id: Optional[int] = None
        self.api_hash: Optional[str] = None
        self.phone: Optional[str] = None
        self.phone_code_hash: Optional[str] = None
        self.is_connected = False
        self.cached_dialogs: List[Dict[str, Any]] = []
        self._live_handlers: Dict[int, Any] = {} # task_id -> event handler

    def _create_client(self, session_name: str, api_id: int, api_hash: str) -> TelegramClient:
        """Create high-performance TelegramClient with abridged TCP connection and resilient reconnects."""
        return TelegramClient(
            session_name,
            api_id,
            api_hash,
            connection=ConnectionTcpAbridged,
            auto_reconnect=True,
            connection_retries=None, # Reconnect indefinitely without dropping
            retry_delay=1,
            flood_sleep_threshold=120, # Automatically sleep on rate-limits under 120s
            request_retries=10,
            timeout=30
        )

    async def initialize_from_saved(self) -> Dict[str, Any]:
        """Try to initialize client using saved credentials & session."""
        cfg = load_config()
        api_id = cfg.get("api_id")
        api_hash = cfg.get("api_hash")
        
        if not api_id or not api_hash:
            return {"authenticated": False, "reason": "No API credentials configured"}

        try:
            self.api_id = int(api_id)
            self.api_hash = str(api_hash)
            
            if self.client is None or not self.client.is_connected():
                self.client = self._create_client(SESSION_NAME, self.api_id, self.api_hash)
                await self.client.connect()
                self.is_connected = True

            if await self.client.is_user_authorized():
                me = await self.client.get_me()
                return {
                    "authenticated": True,
                    "user": self._format_user(me)
                }
            else:
                return {"authenticated": False, "reason": "Session exists but not authorized"}
        except Exception as e:
            logger.error(f"Error initializing client: {e}")
            return {"authenticated": False, "error": str(e)}

    async def setup_client(self, api_id: int, api_hash: str):
        """Setup or reset TelegramClient with provided API credentials."""
        self.api_id = int(api_id)
        self.api_hash = str(api_hash).strip()
        save_config({"api_id": self.api_id, "api_hash": self.api_hash})

        if self.client and self.client.is_connected():
            await self.client.disconnect()

        self.client = self._create_client(SESSION_NAME, self.api_id, self.api_hash)
        await self.client.connect()
        self.is_connected = True

    async def send_code(self, phone: str) -> Dict[str, Any]:
        """Send verification code to phone number."""
        if not self.client or not self.client.is_connected():
            raise Exception("Telegram client is not connected. Configure API ID and Hash first.")
        
        self.phone = phone.strip()
        save_config({"phone": self.phone})
        sent_code = await self.client.send_code_request(self.phone)
        self.phone_code_hash = sent_code.phone_code_hash
        return {
            "status": "code_sent",
            "phone": self.phone,
            "timeout": getattr(sent_code, "timeout", None)
        }

    async def sign_in_code(self, code: str) -> Dict[str, Any]:
        """Verify code received via SMS/Telegram."""
        if not self.client or not self.phone or not self.phone_code_hash:
            raise Exception("No active sign-in request. Please request a code first.")

        try:
            await self.client.sign_in(self.phone, code.strip(), phone_code_hash=self.phone_code_hash)
            me = await self.client.get_me()
            return {
                "status": "success",
                "authenticated": True,
                "user": self._format_user(me)
            }
        except SessionPasswordNeededError:
            return {
                "status": "2fa_required",
                "authenticated": False,
                "message": "Two-factor authentication (2FA) password required"
            }
        except PhoneCodeInvalidError:
            raise Exception("The code you entered is invalid. Please double check.")
        except PhoneCodeExpiredError:
            raise Exception("The code has expired. Please request a new code.")

    async def sign_in_2fa(self, password: str) -> Dict[str, Any]:
        """Complete 2FA sign in."""
        if not self.client:
            raise Exception("Client not initialized.")
        try:
            await self.client.sign_in(password=password)
            me = await self.client.get_me()
            return {
                "status": "success",
                "authenticated": True,
                "user": self._format_user(me)
            }
        except PasswordHashInvalidError:
            raise Exception("Incorrect 2FA password.")

    async def log_out(self) -> Dict[str, Any]:
        """Log out and delete session file."""
        if self.client and await self.client.is_user_authorized():
            await self.client.log_out()
        self.is_connected = False
        self.cached_dialogs = []
        session_file = Path(f"{SESSION_NAME}.session")
        if session_file.exists():
            try:
                session_file.unlink()
            except Exception:
                pass
        return {"status": "logged_out"}

    async def get_status(self) -> Dict[str, Any]:
        """Return current authentication status and user details."""
        if not self.client or not self.client.is_connected():
            return {"authenticated": False, "has_credentials": bool(self.api_id and self.api_hash)}
        
        try:
            auth = await self.client.is_user_authorized()
            if auth:
                me = await self.client.get_me()
                return {
                    "authenticated": True,
                    "user": self._format_user(me),
                    "api_id": self.api_id
                }
            return {"authenticated": False, "has_credentials": bool(self.api_id and self.api_hash)}
        except Exception as e:
            return {"authenticated": False, "error": str(e)}

    async def fetch_dialogs(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Fetch all channels, supergroups, and chats where the user is a member."""
        if not self.client or not await self.client.is_user_authorized():
            raise Exception("User not logged in.")

        if self.cached_dialogs and not force_refresh:
            return self.cached_dialogs

        dialogs = []
        async for dialog in self.client.iter_dialogs():
            entity = dialog.entity
            chat_type = "chat"
            username = getattr(entity, "username", None)
            is_channel = isinstance(entity, Channel)
            is_group = isinstance(entity, Chat) or (is_channel and getattr(entity, "megagroup", False))
            
            if is_channel and not getattr(entity, "megagroup", False):
                chat_type = "channel"
            elif is_group:
                chat_type = "group"
            elif isinstance(entity, User):
                if entity.is_self:
                    chat_type = "saved_messages"
                else:
                    chat_type = "user"

            # Check if user can send messages
            can_send = True
            if is_channel and not is_group:
                # In broadcast channels, only admins can post
                admin_rights = getattr(entity, "admin_rights", None)
                creator = getattr(entity, "creator", False)
                if not creator and not admin_rights:
                    can_send = False

            # Format ID for telethon (ensure string representation)
            dialog_id = str(dialog.id)

            dialogs.append({
                "id": dialog_id,
                "title": dialog.name or "Untitled",
                "username": f"@{username}" if username else None,
                "type": chat_type,
                "can_send": can_send,
                "unread_count": dialog.unread_count
            })

        self.cached_dialogs = dialogs
        return dialogs

    async def add_or_resolve_channel(self, identifier: str) -> Dict[str, Any]:
        """Join or resolve a channel/group by @username, t.me link, message link, invite hash, or ID."""
        if not self.client or not await self.client.is_user_authorized():
            raise Exception("User not logged in.")

        # Extract message link info if user pasted a message URL like https://t.me/c/4321038948/13906
        parsed_link = parse_telegram_message_link(identifier)
        parsed_msg_id = parsed_link["message_id"]

        if parsed_link["channel_identifier"]:
            ident = parsed_link["channel_identifier"]
        elif parsed_link["channel_id"]:
            ident = str(parsed_link["channel_id"])
        else:
            ident = identifier.strip()

        entity = None

        try:
            # 1. Handle invite links (e.g. https://t.me/+xxxx, t.me/joinchat/xxxx, +xxxx)
            is_invite = ("+" in ident) or ("joinchat" in ident) or (ident.startswith("+"))
            if is_invite:
                if "+" in ident:
                    hash_part = ident.split("+")[-1]
                elif "joinchat/" in ident:
                    hash_part = ident.split("joinchat/")[-1]
                else:
                    hash_part = ident.lstrip("+")
                
                hash_part = hash_part.split("/")[0].split("?")[0].strip()

                try:
                    invite_res = await self.client(CheckChatInviteRequest(hash_part))
                    if isinstance(invite_res, ChatInviteAlready):
                        entity = invite_res.chat
                    else:
                        # Join via invite link
                        updates = await self.client(ImportChatInviteRequest(hash_part))
                        entity = updates.chats[0] if updates.chats else None
                except UserAlreadyParticipantError:
                    # User already joined, retrieve chat entity via CheckChatInviteRequest
                    invite_res = await self.client(CheckChatInviteRequest(hash_part))
                    entity = getattr(invite_res, "chat", None)

            # 2. If not an invite link or if entity not yet resolved
            if not entity:
                clean_ident = ident
                if clean_ident.startswith("https://t.me/"):
                    clean_ident = clean_ident.replace("https://t.me/", "")
                elif clean_ident.startswith("t.me/"):
                    clean_ident = clean_ident.replace("t.me/", "")

                if clean_ident.startswith("c/"):
                    # Internal private channel link: t.me/c/1234567890/...
                    cid_str = clean_ident.split("/")[1]
                    channel_id = int(f"-100{cid_str}") if not cid_str.startswith("-100") else int(cid_str)
                    entity = await self.client.get_entity(channel_id)
                else:
                    target = clean_ident.split("/")[0].split("?")[0]
                    # Check if integer ID
                    try:
                        num_id = int(target)
                        entity = await self.client.get_entity(num_id)
                    except ValueError:
                        # Username (with or without @)
                        entity = await self.client.get_entity(target)

                # If public channel and not joined yet, attempt joining
                if isinstance(entity, Channel):
                    try:
                        await self.client(JoinChannelRequest(entity))
                    except Exception:
                        pass

            if entity:
                peer_id = get_peer_id(entity)
                username = getattr(entity, "username", None)
                chat_type = "channel" if isinstance(entity, Channel) and not getattr(entity, "megagroup", False) else "group"
                formatted = {
                    "id": str(peer_id),
                    "title": getattr(entity, "title", "Channel"),
                    "username": f"@{username}" if username else None,
                    "type": chat_type,
                    "can_send": True,
                    "unread_count": 0,
                    "message_id": parsed_msg_id
                }
                # Prepend to cached dialogs if not present
                if not any(d["id"] == formatted["id"] for d in self.cached_dialogs):
                    self.cached_dialogs.insert(0, formatted)
                return formatted
            else:
                raise Exception("Could not resolve entity.")
        except Exception as e:
            raise Exception(f"Failed to resolve channel: {str(e)}")

    async def resolve_message_link(self, link_or_identifier: str) -> Dict[str, Any]:
        """Resolve a full Telegram message link into channel info and message ID."""
        parsed = parse_telegram_message_link(link_or_identifier)
        target = parsed["channel_identifier"] or (str(parsed["channel_id"]) if parsed["channel_id"] else None)
        channel_info = None

        if target:
            try:
                channel_info = await self.add_or_resolve_channel(target)
            except Exception as e:
                logger.warning(f"Could not resolve channel '{target}' from message link: {e}")

        return {
            "channel": channel_info,
            "message_id": parsed["message_id"],
            "channel_identifier": target
        }

    async def clean_copy_message(
        self,
        dest_peer: Any,
        message: Any,
        force_reupload: bool = False,
        progress_callback: Optional[Callable[[str], Any]] = None
    ) -> Any:
        """
        Sends message to destination WITHOUT FORWARD HEADER!
        Prevents deletion even if source channel gets banned or media removed.
        Preserves all bold/italic/code formatting entities and media attributes.
        Optimized with cryptg and multi-DC parallel transfers for 500MB-2GB+ files.
        """
        if not self.client:
            raise Exception("Client not ready")

        # In Telethon, message.message / message.raw_text contains the exact unadulterated text.
        # Do NOT use message.text, because message.text runs markdown.unparse(), turning bold entities
        # into literal **asterisks** which breaks entity offsets and causes literal ** to appear in Telegram.
        raw_text = getattr(message, "message", None) or getattr(message, "raw_text", None) or ""
        entities = getattr(message, "entities", None)

        # 1. Handle Polls
        if message.media and isinstance(message.media, MessageMediaPoll):
            return await self.client.send_message(
                dest_peer,
                file=message.media, # In telethon, sending poll media directly creates independent poll
            )

        # Delegate to wzgram engine for maximum download/upload speed (WarpCrypto + 24 workers)
        try:
            dest_id = get_peer_id(dest_peer) if hasattr(dest_peer, "id") or hasattr(dest_peer, "channel_id") else dest_peer
            src_id = get_peer_id(message.peer_id) if hasattr(message, "peer_id") else getattr(message, "chat_id", None)

            wz_res = await wzgram_engine.transfer_message(
                dest_chat_id=dest_id,
                src_chat_id=src_id,
                message_id=message.id,
                force_reupload=force_reupload,
                progress_callback=progress_callback
            )
            if wz_res:
                return wz_res
        except Exception as wz_err:
            logger.info(f"[wzgram] notice ({wz_err}); using parallel fallback engine...")

        # 2. Text-only message (or webpage preview)
        if not message.media or isinstance(message.media, MessageMediaWebPage):
            if not raw_text:
                return None
            return await self.client.send_message(
                dest_peer,
                raw_text,
                formatting_entities=entities,
                link_preview=bool(message.media)
            )

        # 3. Media Message (Photo, Video, Document, Audio, Voice, Sticker)
        # FAST CLEAN COPY (Instant, Zero Disk I/O, Server-Side Clone)
        # Try server-side copy first if user hasn't explicitly forced physical re-upload
        if not force_reupload:
            try:
                sent_msg = await self.client.send_file(
                    dest_peer,
                    file=message.media,
                    caption=raw_text,
                    formatting_entities=entities,
                    supports_streaming=True
                )
                return sent_msg
            except Exception as e:
                logger.info(f"Direct clone unavailable ({e}); initiating high-speed parallel re-upload...")

        # 4. High-Speed Parallel Download & Re-upload (Optimized for cryptg + 500MB-2GB+)
        temp_path = None
        try:
            # Extract file attributes, filename, and size
            file_name = None
            attributes = []
            file_size = 0
            supports_streaming = True

            if hasattr(message, "file") and message.file and getattr(message.file, "size", None):
                file_size = message.file.size
            elif hasattr(message, "document") and message.document and getattr(message.document, "size", None):
                file_size = message.document.size

            if hasattr(message, "file") and message.file and getattr(message.file, "name", None):
                file_name = message.file.name

            if hasattr(message, "document") and message.document:
                attributes = list(getattr(message.document, "attributes", []))

            if not file_name:
                ext = getattr(message.file, "ext", "") if hasattr(message, "file") and message.file else ""
                ext = ext or ".bin"
                file_name = f"media_{message.id}{ext}"

            # Sanitize file_name for Windows filesystem
            safe_file_name = "".join([c for c in file_name if c not in '<>:"/\\|?*']).strip()
            temp_path = os.path.join(str(TEMP_DOWNLOAD_DIR), f"fast_{message.id}_{safe_file_name}")

            # 4A. High-Speed Download (Parallel multi-connection DC transfer with cryptg)
            target_location = getattr(message, "document", None) or getattr(message, "photo", None) or message.media
            if hasattr(target_location, "document"):
                target_location = target_location.document

            if file_size > 2 * 1024 * 1024 and target_location and hasattr(target_location, "id"):
                if progress_callback:
                    await progress_callback(f"Starting parallel download ({file_size / (1024*1024):.1f} MB)...")
                await fast_download_file(
                    self.client,
                    target_location,
                    temp_path,
                    file_size,
                    progress_callback=progress_callback
                )
            else:
                # Direct stream download for smaller media files
                temp_path = await self.client.download_media(message, file=temp_path)

            if not temp_path or not os.path.exists(temp_path):
                raise Exception("Failed to download media for re-upload.")

            # 4B. High-Speed Upload (Parallel chunk upload with cryptg)
            uploaded_file = None
            if file_size > 2 * 1024 * 1024:
                if progress_callback:
                    await progress_callback(f"Starting parallel upload ({file_size / (1024*1024):.1f} MB)...")
                uploaded_file = await fast_upload_file(
                    self.client,
                    temp_path,
                    progress_callback=progress_callback
                )
            else:
                uploaded_file = temp_path

            # 4C. Send with all original attributes (video streams, names, duration) and formatting preserved
            sent_msg = await self.client.send_file(
                dest_peer,
                file=uploaded_file,
                caption=raw_text,
                formatting_entities=entities,
                attributes=attributes if attributes else None,
                supports_streaming=supports_streaming
            )
            return sent_msg
        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception as ex:
                    logger.warning(f"Could not remove temp file {temp_path}: {ex}")

    def _format_user(self, user) -> Dict[str, Any]:
        if not user:
            return {}
        return {
            "id": user.id,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "username": f"@{user.username}" if user.username else None,
            "phone": user.phone
        }

telegram_service = TelegramService()
