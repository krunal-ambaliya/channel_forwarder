import asyncio
import logging
import time
from typing import Dict, Any, Optional, Set, Callable
from telethon import events
from telethon.errors import FloodWaitError, ChannelPrivateError, ChatWriteForbiddenError
from database import (
    get_db, is_message_forwarded, record_forwarded_message
)
from telegram_service import telegram_service

logger = logging.getLogger("worker")

class ForwardingWorker:
    def __init__(self):
        self.active_tasks: Dict[int, asyncio.Task] = {}
        self.paused_tasks: Set[int] = set()
        self.stop_requested: Set[int] = set()
        self.live_event_handlers: Dict[int, Any] = {}
        self.log_subscribers: Set[Callable[[Dict[str, Any]], None]] = set()

    def subscribe_logs(self, callback: Callable[[Dict[str, Any]], None]):
        self.log_subscribers.add(callback)

    def unsubscribe_logs(self, callback: Callable[[Dict[str, Any]], None]):
        self.log_subscribers.discard(callback)

    async def broadcast_log(self, task_id: Optional[int], level: str, message: str, meta: Optional[Dict] = None):
        """Emit log entry to all connected UI clients."""
        log_entry = {
            "timestamp": time.strftime("%H:%M:%S"),
            "task_id": task_id,
            "level": level,
            "message": message,
            "meta": meta or {}
        }
        for sub in list(self.log_subscribers):
            try:
                if asyncio.iscoroutinefunction(sub):
                    await sub(log_entry)
                else:
                    sub(log_entry)
            except Exception as e:
                logger.error(f"Error notifying log subscriber: {e}")

    async def broadcast_task_update(self, task_id: int):
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
        row = cursor.fetchone()
        conn.close()
        if row:
            data = dict(row)
            await self.broadcast_log(task_id, "TASK_UPDATE", f"Task {task_id} state updated", meta={"task": data})

    async def start_task(self, task_id: int) -> bool:
        """Start or resume a forwarding task."""
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
        task_row = cursor.fetchone()
        if not task_row:
            conn.close()
            raise Exception("Task not found.")

        task_data = dict(task_row)
        conn.close()

        # If already running
        if task_id in self.active_tasks and not self.active_tasks[task_id].done():
            if task_id in self.paused_tasks:
                self.paused_tasks.remove(task_id)
                self._update_task_status(task_id, "running")
                await self.broadcast_log(task_id, "INFO", f"Task '{task_data['name']}' resumed.")
                await self.broadcast_task_update(task_id)
                return True
            return True

        self.stop_requested.discard(task_id)
        self.paused_tasks.discard(task_id)

        # Launch async worker task
        worker_coro = self._run_task_loop(task_data)
        async_task = asyncio.create_task(worker_coro)
        self.active_tasks[task_id] = async_task
        self._update_task_status(task_id, "running")
        await self.broadcast_log(task_id, "INFO", f"Started forwarding task: {task_data['name']}")
        await self.broadcast_task_update(task_id)
        return True

    def pause_task(self, task_id: int):
        if task_id in self.active_tasks and not self.active_tasks[task_id].done():
            self.paused_tasks.add(task_id)
            self._update_task_status(task_id, "paused")
            asyncio.create_task(self.broadcast_log(task_id, "WARN", f"Task {task_id} paused."))
            asyncio.create_task(self.broadcast_task_update(task_id))
            return True
        return False

    def stop_task(self, task_id: int):
        self.stop_requested.add(task_id)
        if task_id in self.paused_tasks:
            self.paused_tasks.remove(task_id)

        # Remove live handler if active
        if task_id in self.live_event_handlers:
            handler = self.live_event_handlers.pop(task_id)
            if telegram_service.client:
                telegram_service.client.remove_event_handler(handler)

        if task_id in self.active_tasks:
            task = self.active_tasks[task_id]
            if not task.done():
                task.cancel()

        self._update_task_status(task_id, "idle")
        asyncio.create_task(self.broadcast_log(task_id, "INFO", f"Task {task_id} stopped."))
        asyncio.create_task(self.broadcast_task_update(task_id))
        return True

    def _update_task_status(self, task_id: int, status: str):
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("UPDATE tasks SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", (status, task_id))
        conn.commit()
        conn.close()

    def _update_task_counts(self, task_id: int, processed: int, forwarded: int, failed: int, last_msg_id: int):
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE tasks 
            SET processed_messages = ?, forwarded_messages = ?, failed_messages = ?, 
                last_source_msg_id = ?, updated_at = CURRENT_TIMESTAMP 
            WHERE id = ?
        """, (processed, forwarded, failed, last_msg_id, task_id))
        conn.commit()
        conn.close()

    def _matches_filter(self, msg: Any, filter_type: str) -> bool:
        if filter_type == "all":
            return True
        if filter_type == "text_only":
            return not bool(msg.media)
        if filter_type == "media_only":
            return bool(msg.media)
        if filter_type == "photos":
            return bool(msg.photo)
        if filter_type == "videos":
            return bool(msg.video)
        if filter_type == "documents":
            return bool(msg.document and not msg.video and not msg.audio and not msg.voice)
        return True

    async def _run_task_loop(self, task: Dict[str, Any]):
        task_id = task["id"]
        source_id = int(task["source_id"]) if task["source_id"].lstrip("-").isdigit() else task["source_id"]
        destination_id = int(task["destination_id"]) if task["destination_id"].lstrip("-").isdigit() else task["destination_id"]
        reupload_mode = bool(task.get("reupload_mode", 0))
        media_filter = task.get("media_filter", "all")
        delay_seconds = float(task.get("delay_seconds", 1.5))
        mode = task.get("mode", "history")

        processed = task.get("processed_messages", 0)
        forwarded = task.get("forwarded_messages", 0)
        failed = task.get("failed_messages", 0)
        last_id = task.get("last_source_msg_id", 0)

        client = telegram_service.client
        if not client or not await client.is_user_authorized():
            await self.broadcast_log(task_id, "ERROR", "Telegram client not authorized.")
            self._update_task_status(task_id, "error")
            return

        try:
            # Resolve destination & source entities
            await self.broadcast_log(task_id, "INFO", f"Connecting to source ({task['source_title']}) & destination ({task['destination_title']})...")
            src_entity = await client.get_entity(source_id)
            dest_entity = await client.get_entity(destination_id)

            # Register live handler if requested
            if mode in ["live", "both"] and task_id not in self.live_event_handlers:
                await self._setup_live_listener(task_id, src_entity, dest_entity, reupload_mode, media_filter, delay_seconds)
                await self.broadcast_log(task_id, "SUCCESS", "Live listener registered for real-time forwarding!")

            # If mode includes history
            if mode in ["history", "both"]:
                await self.broadcast_log(task_id, "INFO", "Scanning historical messages...")
                
                # Estimate total messages
                total_count = 0
                async for _ in client.iter_messages(src_entity, limit=0):
                    pass
                try:
                    total_count = (await client.get_messages(src_entity, limit=1)).total or 0
                except Exception:
                    total_count = 0

                conn = get_db()
                cursor = conn.cursor()
                cursor.execute("UPDATE tasks SET total_messages = ? WHERE id = ?", (total_count, task_id))
                conn.commit()
                conn.close()

                # Iterate messages: reverse=True gets oldest to newest (natural chronology)
                reverse_order = bool(task.get("reverse_order", 1))
                min_id = last_id if reverse_order and last_id > 0 else (task.get("min_msg_id") or 0)
                max_id = task.get("max_msg_id") or 0
                
                iter_kwargs = {"reverse": reverse_order}
                if min_id > 0:
                    iter_kwargs["min_id"] = min_id
                if max_id > 0:
                    iter_kwargs["max_id"] = max_id

                async for message in client.iter_messages(src_entity, **iter_kwargs):
                    # Check stop
                    if task_id in self.stop_requested:
                        await self.broadcast_log(task_id, "WARN", "Task cancelled by user.")
                        self._update_task_status(task_id, "idle")
                        await self.broadcast_task_update(task_id)
                        return

                    # Check pause
                    while task_id in self.paused_tasks:
                        await asyncio.sleep(1)
                        if task_id in self.stop_requested:
                            self._update_task_status(task_id, "idle")
                            return

                    # Skip empty / service messages
                    if message.action:
                        processed += 1
                        last_id = message.id
                        continue

                    # Deduplication check
                    if is_message_forwarded(task_id, str(task["source_id"]), message.id):
                        processed += 1
                        last_id = message.id
                        continue

                    # Filter check
                    if not self._matches_filter(message, media_filter):
                        processed += 1
                        last_id = message.id
                        continue

                    # Forwarding attempt with rate-limit / flood-wait handling
                    retry_limit = 5
                    success = False
                    for attempt in range(retry_limit):
                        try:
                            m_type = "text"
                            if message.photo: m_type = "photo"
                            elif message.video: m_type = "video"
                            elif message.document: m_type = "document"
                            elif message.audio: m_type = "audio"
                            elif message.voice: m_type = "voice"

                            await self.broadcast_log(
                                task_id, "INFO",
                                f"Forwarding msg #{message.id} ({m_type}) without forward header..."
                            )

                            async def on_speed_progress(status_text: str):
                                await self.broadcast_log(task_id, "INFO", f"[Msg #{message.id}] {status_text}")

                            sent_msg = await telegram_service.clean_copy_message(
                                dest_entity,
                                message,
                                force_reupload=reupload_mode,
                                progress_callback=on_speed_progress
                            )

                            if sent_msg:
                                dest_msg_id = getattr(sent_msg, "id", 0)
                                record_forwarded_message(task_id, str(task["source_id"]), message.id, str(task["destination_id"]), dest_msg_id)
                                forwarded += 1
                                success = True
                                await self.broadcast_log(
                                    task_id, "SUCCESS",
                                    f"Msg #{message.id} copied successfully as destination #{dest_msg_id}!"
                                )
                            else:
                                failed += 1
                            break

                        except FloodWaitError as e:
                            wait_time = e.seconds
                            await self.broadcast_log(
                                task_id, "WARN",
                                f"Telegram FloodWait encountered: sleeping for {wait_time}s..."
                            )
                            # Sleep in increments so pause/stop can break out if needed
                            for _ in range(wait_time):
                                if task_id in self.stop_requested:
                                    break
                                await asyncio.sleep(1)

                        except (ChatWriteForbiddenError, ChannelPrivateError) as perm_err:
                            await self.broadcast_log(task_id, "ERROR", f"Permission error: {perm_err}")
                            failed += 1
                            break
                        except Exception as ex:
                            await self.broadcast_log(task_id, "ERROR", f"Error on msg #{message.id}: {str(ex)}")
                            if attempt == retry_limit - 1:
                                failed += 1
                            await asyncio.sleep(2)

                    processed += 1
                    last_id = message.id
                    self._update_task_counts(task_id, processed, forwarded, failed, last_id)
                    await self.broadcast_task_update(task_id)

                    # Configurable throttle delay between posts
                    await asyncio.sleep(delay_seconds)

                await self.broadcast_log(task_id, "SUCCESS", f"Historical forwarding completed! Processed: {processed}, Forwarded: {forwarded}, Failed: {failed}")

                if mode == "history":
                    self._update_task_status(task_id, "completed")
                else:
                    self._update_task_status(task_id, "live_active")

                await self.broadcast_task_update(task_id)

        except asyncio.CancelledError:
            await self.broadcast_log(task_id, "WARN", "Task cancelled.")
            self._update_task_status(task_id, "idle")
            await self.broadcast_task_update(task_id)
        except Exception as e:
            logger.error(f"Fatal error in task {task_id}: {e}", exc_info=True)
            await self.broadcast_log(task_id, "ERROR", f"Task fatal error: {str(e)}")
            self._update_task_status(task_id, "error")
            await self.broadcast_task_update(task_id)

    async def _setup_live_listener(
        self,
        task_id: int,
        src_entity: Any,
        dest_entity: Any,
        reupload_mode: bool,
        media_filter: str,
        delay_seconds: float
    ):
        """Register live event handler for real-time clean forwarding."""
        client = telegram_service.client

        @client.on(events.NewMessage(chats=src_entity))
        async def handler(event):
            msg = event.message
            if task_id in self.paused_tasks or task_id in self.stop_requested:
                return

            if not self._matches_filter(msg, media_filter):
                return

            if is_message_forwarded(task_id, str(src_entity.id), msg.id):
                return

            try:
                async def on_live_speed_progress(status_text: str):
                    await self.broadcast_log(task_id, "INFO", f"[LIVE Msg #{msg.id}] {status_text}")

                sent = await telegram_service.clean_copy_message(
                    dest_entity,
                    msg,
                    force_reupload=reupload_mode,
                    progress_callback=on_live_speed_progress
                )
                if sent:
                    dest_msg_id = getattr(sent, "id", 0)
                    record_forwarded_message(task_id, str(src_entity.id), msg.id, str(dest_entity.id), dest_msg_id)
                    
                    # Update counts
                    conn = get_db()
                    cursor = conn.cursor()
                    cursor.execute("""
                        UPDATE tasks 
                        SET processed_messages = processed_messages + 1,
                            forwarded_messages = forwarded_messages + 1,
                            last_source_msg_id = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (msg.id, task_id))
                    conn.commit()
                    conn.close()
                    
                    await self.broadcast_log(task_id, "SUCCESS", f"[LIVE] Message #{msg.id} forwarded cleanly to destination #{dest_msg_id}!")
                    await self.broadcast_task_update(task_id)
                await asyncio.sleep(delay_seconds)
            except FloodWaitError as e:
                await self.broadcast_log(task_id, "WARN", f"[LIVE] Flood wait for {e.seconds}s")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                await self.broadcast_log(task_id, "ERROR", f"[LIVE] Failed to forward msg #{msg.id}: {e}")

        self.live_event_handlers[task_id] = handler

worker = ForwardingWorker()
