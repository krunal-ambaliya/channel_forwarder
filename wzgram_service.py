import os
import time
import math
import struct
import zlib
import sqlite3
import asyncio
import logging
from typing import Optional, Dict, Any, Callable

import wzgram
from wzgram.storage.storage import Storage, WZ_PREFIX
from pyrogram.errors import (
    ChatForwardsRestricted, FloodWait, ChannelPrivate, ChatWriteForbidden
)
from config import BASE_DIR, TEMP_DOWNLOAD_DIR, load_config

logger = logging.getLogger("wzgram_transfer")

def get_wzgram_session_string() -> Optional[str]:
    """Derive wzgram session string automatically from the active session."""
    cfg = load_config()
    sess_file = BASE_DIR / "telegram_forwarder.session"
    if not sess_file.exists():
        return None
    
    try:
        conn = sqlite3.connect(str(sess_file))
        row = conn.cursor().execute("SELECT dc_id, server_address, port, auth_key FROM sessions").fetchone()
        if not row or not row[3]:
            conn.close()
            return None
        
        dc_id, server_address, port, auth_key = row
        addr_bytes = (server_address or "").encode("ascii").ljust(48, b"\x00")
        
        # Get user_id if present
        user_id = 0
        try:
            user_row = conn.cursor().execute("SELECT id FROM entities WHERE id > 0 LIMIT 1").fetchone()
            if user_row:
                user_id = user_row[0]
        except Exception:
            pass
        conn.close()

        packed = struct.pack(
            Storage.SESSION_STRING_FORMAT_V3,
            3, dc_id, cfg.get("api_id", 0), False, auth_key, user_id, False, port or 443, addr_bytes
        )
        crc = struct.pack("<I", zlib.crc32(packed))
        return WZ_PREFIX + Storage._encode(packed + crc)
    except Exception as e:
        logger.error(f"Error deriving wzgram session string: {e}")
        return None

class WzgramSpeedTracker:
    def __init__(self, label: str = "Transfer"):
        self.label = label
        self.start_time = time.time()
        self.last_update_time = 0

    def format_status(self, current: int, total: int) -> Optional[str]:
        now = time.time()
        if now - self.last_update_time < 0.8 and current < total:
            return None
        self.last_update_time = now

        elapsed = now - self.start_time
        if elapsed <= 0:
            elapsed = 0.001
        
        speed = current / elapsed
        speed_mb = speed / (1024 * 1024)
        speed_str = f"{speed_mb:.1f} MB/s" if speed_mb >= 0.1 else f"{speed / 1024:.0f} KB/s"

        pct = (current / total * 100) if total > 0 else 0
        cur_mb = current / (1024 * 1024)
        tot_mb = total / (1024 * 1024)

        rem_bytes = max(0, total - current)
        eta_sec = int(rem_bytes / speed) if speed > 0 else 0
        if eta_sec > 60:
            eta_str = f"{eta_sec // 60}m {eta_sec % 60}s"
        else:
            eta_str = f"{eta_sec}s"

        return f"[wzgram] {self.label}: {cur_mb:.1f}/{tot_mb:.1f} MB ({pct:.1f}%) • {speed_str} • ETA: {eta_str}"

class WzgramEngine:
    def __init__(self):
        self.client: Optional[wzgram.Client] = None
        self._lock = asyncio.Lock()

    async def ensure_started(self) -> bool:
        async with self._lock:
            if self.client and self.client.is_connected:
                return True

            sess_str = get_wzgram_session_string()
            if not sess_str:
                return False

            cfg = load_config()
            self.client = wzgram.Client(
                name="wzgram_engine",
                session_string=sess_str,
                in_memory=True,
                api_id=cfg.get("api_id"),
                api_hash=cfg.get("api_hash"),
                workers=16,
                max_concurrent_transmissions=24
            )
            await self.client.start()
            logger.info("[wzgram] High-speed MTProto engine started with WarpCrypto!")
            return True

    async def stop(self):
        async with self._lock:
            if self.client and self.client.is_connected:
                try:
                    await self.client.stop()
                except Exception:
                    pass
                self.client = None

    async def transfer_message(
        self,
        dest_chat_id: Any,
        src_chat_id: Any,
        message_id: int,
        force_reupload: bool = False,
        progress_callback: Optional[Callable[[str], Any]] = None
    ) -> Any:
        """
        Transfer message using wzgram for maximum speed (WarpCrypto + parallel streaming).
        - If not forced reupload: tries copy_message (instant, 0s, 0 bytes disk/network).
        - If protected or forced: downloads using wzgram parallel engine, uploads at full line speed.
        """
        await self.ensure_started()
        if not self.client:
            raise Exception("wzgram engine could not be initialized.")

        # Ensure integer IDs for channels if numeric
        d_id = int(dest_chat_id) if str(dest_chat_id).lstrip("-").isdigit() else str(dest_chat_id)
        s_id = int(src_chat_id) if str(src_chat_id).lstrip("-").isdigit() else str(src_chat_id)

        # 1. Fast Clean Copy (Instant Server-Side Clone without Forward Header)
        if not force_reupload:
            try:
                copied = await self.client.copy_message(
                    chat_id=d_id,
                    from_chat_id=s_id,
                    message_id=message_id
                )
                return copied
            except ChatForwardsRestricted:
                if progress_callback:
                    await progress_callback("[wzgram] Content protected (noforwards); switching to high-speed re-upload...")
            except Exception as e:
                logger.info(f"[wzgram] copy_message failed ({e}), falling back to parallel re-upload...")

        # 2. High-Speed Parallel Download & Upload with wzgram + WarpCrypto
        msg = await self.client.get_messages(chat_id=s_id, message_ids=message_id)
        if not msg:
            raise Exception(f"Message #{message_id} not found in source chat.")

        # If text-only message
        if not msg.media:
            if not msg.text:
                return None
            return await self.client.send_message(
                chat_id=d_id,
                text=msg.text,
                entities=msg.entities
            )

        # Handle Media (Video, Document, Photo, Audio, Voice, Animation)
        temp_path = None
        try:
            # Setup download progress tracker
            dl_tracker = WzgramSpeedTracker(label="Downloading")

            async def wz_dl_progress(current, total):
                status_text = dl_tracker.format_status(current, total)
                if status_text and progress_callback:
                    try:
                        if asyncio.iscoroutinefunction(progress_callback):
                            await progress_callback(status_text)
                        else:
                            progress_callback(status_text)
                    except Exception:
                        pass

            # Download using wzgram parallel engine
            temp_dir = str(TEMP_DOWNLOAD_DIR)
            file_path = await self.client.download_media(
                msg,
                file_name=temp_dir + os.sep,
                progress=wz_dl_progress
            )

            if not file_path or not os.path.exists(file_path):
                raise Exception("wzgram download failed: file not written.")

            temp_path = file_path

            # Setup upload progress tracker
            up_tracker = WzgramSpeedTracker(label="Uploading")

            async def wz_up_progress(current, total):
                status_text = up_tracker.format_status(current, total)
                if status_text and progress_callback:
                    try:
                        if asyncio.iscoroutinefunction(progress_callback):
                            await progress_callback(status_text)
                        else:
                            progress_callback(status_text)
                    except Exception:
                        pass

            caption = msg.caption or ""
            caption_entities = msg.caption_entities

            # Dispatch upload based on media type
            if msg.video:
                return await self.client.send_video(
                    chat_id=d_id,
                    video=temp_path,
                    caption=caption,
                    caption_entities=caption_entities,
                    duration=msg.video.duration or 0,
                    width=msg.video.width or 0,
                    height=msg.video.height or 0,
                    supports_streaming=True,
                    progress=wz_up_progress
                )
            elif msg.audio:
                return await self.client.send_audio(
                    chat_id=d_id,
                    audio=temp_path,
                    caption=caption,
                    caption_entities=caption_entities,
                    duration=msg.audio.duration or 0,
                    performer=msg.audio.performer,
                    title=msg.audio.title,
                    progress=wz_up_progress
                )
            elif msg.photo:
                return await self.client.send_photo(
                    chat_id=d_id,
                    photo=temp_path,
                    caption=caption,
                    caption_entities=caption_entities,
                    progress=wz_up_progress
                )
            elif msg.voice:
                return await self.client.send_voice(
                    chat_id=d_id,
                    voice=temp_path,
                    caption=caption,
                    caption_entities=caption_entities,
                    duration=msg.voice.duration or 0,
                    progress=wz_up_progress
                )
            elif msg.animation:
                return await self.client.send_animation(
                    chat_id=d_id,
                    animation=temp_path,
                    caption=caption,
                    caption_entities=caption_entities,
                    duration=msg.animation.duration or 0,
                    width=msg.animation.width or 0,
                    height=msg.animation.height or 0,
                    progress=wz_up_progress
                )
            else:
                # Default: Send as Document / File
                orig_name = None
                if msg.document and msg.document.file_name:
                    orig_name = msg.document.file_name
                return await self.client.send_document(
                    chat_id=d_id,
                    document=temp_path,
                    caption=caption,
                    caption_entities=caption_entities,
                    file_name=orig_name,
                    force_document=True,
                    progress=wz_up_progress
                )

        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception as ex:
                    logger.warning(f"Could not remove temp file {temp_path}: {ex}")

wzgram_engine = WzgramEngine()
