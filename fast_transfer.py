import os
import math
import time
import inspect
import asyncio
import hashlib
import logging
from typing import Optional, List, Tuple, BinaryIO, Union, Callable, Any

from telethon import utils, helpers, TelegramClient
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import (
    GetFileRequest, SaveFilePartRequest, SaveBigFilePartRequest
)
from telethon.tl.types import (
    Document, InputFileLocation, InputDocumentFileLocation,
    InputPhotoFileLocation, InputPeerPhotoFileLocation, TypeInputFile,
    InputFileBig, InputFile, MessageMediaDocument, MessageMediaPhoto
)

logger = logging.getLogger("fast_transfer")

TypeLocation = Union[
    Document, InputDocumentFileLocation, InputPeerPhotoFileLocation,
    InputFileLocation, InputPhotoFileLocation
]

class DownloadSender:
    def __init__(self, client: TelegramClient, sender: MTProtoSender, file: TypeLocation,
                 offset: int, limit: int, stride: int, count: int,
                 loop: asyncio.AbstractEventLoop) -> None:
        self.client = client
        self.sender = sender
        self.file = file
        self.offset = offset
        self.limit = limit
        self.stride = stride
        self.remaining = count
        self.loop = loop
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=3)
        self.producer_task = self.loop.create_task(self._producer())

    async def _producer(self):
        """Continuously prefetch chunks into the queue so network pipeline never stalls."""
        try:
            curr_offset = self.offset
            for _ in range(self.remaining):
                req = GetFileRequest(self.file, offset=curr_offset, limit=self.limit)
                data = None
                for attempt in range(4):
                    try:
                        result = await self.client._call(self.sender, req)
                        data = result.bytes
                        break
                    except Exception as e:
                        if attempt == 3:
                            raise e
                        await asyncio.sleep(0.3 * (attempt + 1))
                curr_offset += self.stride
                await self.queue.put(data)
        except Exception as e:
            await self.queue.put(e)
        finally:
            await self.queue.put(None)

    async def next(self) -> Optional[bytes]:
        val = await self.queue.get()
        if isinstance(val, Exception):
            raise val
        return val

    async def disconnect(self):
        if not self.producer_task.done():
            self.producer_task.cancel()
        return await self.sender.disconnect()

class UploadSender:
    def __init__(self, client: TelegramClient, sender: MTProtoSender, file_id: int,
                 part_count: int, big: bool, index: int, stride: int,
                 loop: asyncio.AbstractEventLoop) -> None:
        self.client = client
        self.sender = sender
        self.part_count = part_count
        self.stride = stride
        self.loop = loop
        self.previous: Optional[asyncio.Task] = None
        if big:
            self.request = SaveBigFilePartRequest(file_id, index, part_count, b"")
        else:
            self.request = SaveFilePartRequest(file_id, index, b"")

    async def next(self, data: bytes) -> None:
        if self.previous:
            await self.previous
        self.previous = self.loop.create_task(self._next(data))

    async def _next(self, data: bytes) -> None:
        self.request.bytes = data
        for attempt in range(4):
            try:
                await self.client._call(self.sender, self.request)
                self.request.file_part += self.stride
                return
            except Exception as e:
                if attempt == 3:
                    raise e
                await asyncio.sleep(0.3 * (attempt + 1))

    async def disconnect(self) -> None:
        if self.previous:
            await self.previous
        return await self.sender.disconnect()

class ParallelTransferrer:
    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None) -> None:
        self.client = client
        self.loop = self.client.loop
        self.dc_id = dc_id or self.client.session.dc_id
        self.auth_key = (
            None if dc_id and self.client.session.dc_id != dc_id
            else self.client.session.auth_key
        )
        self.senders: Optional[List[Union[DownloadSender, UploadSender]]] = None
        self.upload_ticker = 0

    async def _cleanup(self) -> None:
        if self.senders:
            await asyncio.gather(*[sender.disconnect() for sender in self.senders], return_exceptions=True)
            self.senders = None

    @staticmethod
    def _get_connection_count(file_size: int, max_count: int = 16) -> int:
        """Calculate optimal parallel connection count (12 to 16 workers for high bandwidth saturation)."""
        if file_size > 50 * 1024 * 1024:
            return 16
        elif file_size > 15 * 1024 * 1024:
            return 12
        elif file_size > 5 * 1024 * 1024:
            return 8
        return 4

    async def _create_sender(self) -> MTProtoSender:
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        await sender.connect(self.client._connection(
            dc.ip_address, dc.port, dc.id,
            loggers=self.client._log,
            proxy=self.client._proxy
        ))
        if not self.auth_key:
            auth = await self.client(ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            await sender.send(req)
            self.auth_key = sender.auth_key
        return sender

    async def _init_download(self, connections: int, file: TypeLocation, part_count: int, part_size: int) -> None:
        minimum, remainder = divmod(part_count, connections)

        def get_part_count() -> int:
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        # First sender exports & imports auth across DCs if needed
        first_sender = await self._create_download_sender(
            file, 0, part_size, connections * part_size, get_part_count()
        )
        other_senders = await asyncio.gather(*[
            self._create_download_sender(file, i, part_size, connections * part_size, get_part_count())
            for i in range(1, connections)
        ])
        self.senders = [first_sender, *other_senders]

    async def _create_download_sender(self, file: TypeLocation, index: int, part_size: int,
                                      stride: int, part_count: int) -> DownloadSender:
        return DownloadSender(
            self.client, await self._create_sender(), file,
            index * part_size, part_size, stride, part_count, loop=self.loop
        )

    async def _init_upload(self, connections: int, file_id: int, part_count: int, big: bool) -> None:
        first_sender = await self._create_upload_sender(file_id, part_count, big, 0, connections)
        other_senders = await asyncio.gather(*[
            self._create_upload_sender(file_id, part_count, big, i, connections)
            for i in range(1, connections)
        ])
        self.senders = [first_sender, *other_senders]

    async def _create_upload_sender(self, file_id: int, part_count: int, big: bool, index: int, stride: int) -> UploadSender:
        return UploadSender(
            self.client, await self._create_sender(), file_id,
            part_count, big, index, stride, loop=self.loop
        )

    async def init_upload(self, file_id: int, file_size: int, part_size_kb: Optional[float] = None,
                          connection_count: Optional[int] = None) -> Tuple[int, int, bool]:
        connection_count = connection_count or self._get_connection_count(file_size)
        # Always use 512 KB chunks for maximum MTProto upload bandwidth
        part_size = int(part_size_kb * 1024) if part_size_kb else (512 * 1024)
        part_count = (file_size + part_size - 1) // part_size
        is_large = file_size > 10 * 1024 * 1024
        await self._init_upload(connection_count, file_id, part_count, is_large)
        return part_size, part_count, is_large

    async def upload_part(self, part: bytes) -> None:
        await self.senders[self.upload_ticker].next(part)
        self.upload_ticker = (self.upload_ticker + 1) % len(self.senders)

    async def finish_upload(self) -> None:
        await self._cleanup()

    async def download_generator(self, file: TypeLocation, file_size: int,
                                 part_size_kb: Optional[float] = None,
                                 connection_count: Optional[int] = None):
        connection_count = connection_count or self._get_connection_count(file_size)
        # Always use 512 KB chunks for maximum MTProto download bandwidth
        part_size = int(part_size_kb * 1024) if part_size_kb else (512 * 1024)
        part_count = math.ceil(file_size / part_size)
        
        await self._init_download(connection_count, file, part_count, part_size)

        part = 0
        try:
            while part < part_count:
                for s in self.senders:
                    if part >= part_count:
                        break
                    data = await s.next()
                    if not data:
                        break
                    yield data
                    part += 1
        finally:
            await self._cleanup()

class SpeedTracker:
    def __init__(self, total_bytes: int, label: str = "Transfer"):
        self.total_bytes = total_bytes
        self.label = label
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.last_bytes = 0

    def get_progress_info(self, current_bytes: int) -> Tuple[str, str, str]:
        now = time.time()
        elapsed = now - self.start_time
        if elapsed <= 0:
            elapsed = 0.001
        
        speed = current_bytes / elapsed # bytes per sec
        speed_mb = speed / (1024 * 1024)
        speed_str = f"{speed_mb:.1f} MB/s" if speed_mb >= 0.1 else f"{speed / 1024:.0f} KB/s"
        
        pct = (current_bytes / self.total_bytes * 100) if self.total_bytes > 0 else 0
        pct_str = f"{pct:.1f}%"

        rem_bytes = max(0, self.total_bytes - current_bytes)
        eta_seconds = int(rem_bytes / speed) if speed > 0 else 0
        
        if eta_seconds > 60:
            eta_str = f"{eta_seconds // 60}m {eta_seconds % 60}s"
        else:
            eta_str = f"{eta_seconds}s"
            
        return pct_str, speed_str, eta_str

async def fast_download_file(
    client: TelegramClient,
    location: Any,
    out_file_path: str,
    file_size: int,
    progress_callback: Optional[Callable[[str], None]] = None
) -> str:
    """Download large file using parallel MTProto connections with speed tracking."""
    dc_id, input_location = utils.get_input_location(location)
    downloader = ParallelTransferrer(client, dc_id)
    
    speed_tracker = SpeedTracker(file_size, label="Download")
    last_callback_time = 0
    downloaded_bytes = 0

    # Ensure output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(out_file_path)), exist_ok=True)

    with open(out_file_path, "wb") as f:
        async for chunk in downloader.download_generator(input_location, file_size):
            f.write(chunk)
            downloaded_bytes += len(chunk)
            
            now = time.time()
            if progress_callback and (now - last_callback_time >= 1.0 or downloaded_bytes >= file_size):
                pct, speed, eta = speed_tracker.get_progress_info(downloaded_bytes)
                size_mb = file_size / (1024 * 1024)
                done_mb = downloaded_bytes / (1024 * 1024)
                status_msg = f"Downloading: {done_mb:.1f}/{size_mb:.1f} MB ({pct}) • {speed} • ETA: {eta}"
                try:
                    if asyncio.iscoroutinefunction(progress_callback):
                        await progress_callback(status_msg)
                    else:
                        progress_callback(status_msg)
                except Exception:
                    pass
                last_callback_time = now

    return out_file_path

async def fast_upload_file(
    client: TelegramClient,
    file_path: str,
    progress_callback: Optional[Callable[[str], None]] = None
) -> TypeInputFile:
    """Upload large file using parallel MTProto connections with speed tracking."""
    file_size = os.path.getsize(file_path)
    file_id = helpers.generate_random_long()
    hash_md5 = hashlib.md5()

    uploader = ParallelTransferrer(client)
    part_size, part_count, is_large = await uploader.init_upload(file_id, file_size)

    speed_tracker = SpeedTracker(file_size, label="Upload")
    last_callback_time = 0
    uploaded_bytes = 0

    buffer = bytearray()
    chunk_read_size = 512 * 1024 # 512 KB chunks

    with open(file_path, "rb") as f:
        while True:
            data = f.read(chunk_read_size)
            if not data:
                break
            
            if not is_large:
                hash_md5.update(data)

            if len(buffer) == 0 and len(data) == part_size:
                await uploader.upload_part(data)
                uploaded_bytes += len(data)
            else:
                new_len = len(buffer) + len(data)
                if new_len >= part_size:
                    cutoff = part_size - len(buffer)
                    buffer.extend(data[:cutoff])
                    await uploader.upload_part(bytes(buffer))
                    uploaded_bytes += len(buffer)
                    buffer.clear()
                    buffer.extend(data[cutoff:])
                else:
                    buffer.extend(data)

            now = time.time()
            if progress_callback and (now - last_callback_time >= 1.0 or uploaded_bytes >= file_size):
                pct, speed, eta = speed_tracker.get_progress_info(uploaded_bytes)
                size_mb = file_size / (1024 * 1024)
                done_mb = uploaded_bytes / (1024 * 1024)
                status_msg = f"Uploading: {done_mb:.1f}/{size_mb:.1f} MB ({pct}) • {speed} • ETA: {eta}"
                try:
                    if asyncio.iscoroutinefunction(progress_callback):
                        await progress_callback(status_msg)
                    else:
                        progress_callback(status_msg)
                except Exception:
                    pass
                last_callback_time = now

        if len(buffer) > 0:
            await uploader.upload_part(bytes(buffer))
            uploaded_bytes += len(buffer)

    await uploader.finish_upload()

    name = os.path.basename(file_path)
    if is_large:
        return InputFileBig(file_id, part_count, name)
    else:
        return InputFile(file_id, part_count, name, hash_md5.hexdigest())
