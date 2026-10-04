import os
import json
import asyncio
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from database import init_db, get_db
from config import load_config, save_config
from telegram_service import telegram_service
from worker import worker

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    init_db()
    try:
        auth_res = await telegram_service.initialize_from_saved()
        if auth_res.get("authenticated"):
            print(f"[Telegram] Auto-logged in successfully as {auth_res.get('user', {}).get('first_name')}")
        else:
            print("[Telegram] Ready. Awaiting authentication via Web UI.")
    except Exception as e:
        print(f"[Telegram] Startup warning: {e}")
    yield
    # Shutdown: Stop any active tasks
    for task_id in list(worker.active_tasks.keys()):
        worker.stop_task(task_id)

app = FastAPI(title="Telegram Autonomous Forwarder", lifespan=lifespan)

# Models
class CredentialsModel(BaseModel):
    api_id: int
    api_hash: str

class PhoneModel(BaseModel):
    phone: str

class CodeModel(BaseModel):
    code: str

class PasswordModel(BaseModel):
    password: str

class AddChannelModel(BaseModel):
    identifier: str

class TaskCreateModel(BaseModel):
    name: Optional[str] = None
    source_id: str
    source_title: str
    destination_id: str
    destination_title: str
    mode: str = "history" # history, live, both
    reupload_mode: int = 0 # 0: smart clean copy, 1: true download & re-upload
    media_filter: str = "all" # all, media_only, text_only, photos, videos, documents
    delay_seconds: float = 1.5
    min_msg_id: int = 0
    max_msg_id: int = 0
    reverse_order: int = 1

# WebSocket Manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: Dict[str, Any]):
        dead = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                dead.append(connection)
        for d in dead:
            self.disconnect(d)

manager = ConnectionManager()

# Hook worker log broadcasts to WebSockets
async def on_worker_log(log_entry: Dict[str, Any]):
    await manager.broadcast(log_entry)

worker.subscribe_logs(on_worker_log)

# --- Auth Endpoints ---

@app.get("/api/auth/status")
async def get_auth_status():
    return await telegram_service.get_status()

@app.post("/api/auth/credentials")
async def set_credentials(data: CredentialsModel):
    try:
        await telegram_service.setup_client(data.api_id, data.api_hash)
        return {"status": "ok", "message": "Credentials saved and client connected"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/auth/send_code")
async def send_code(data: PhoneModel):
    try:
        res = await telegram_service.send_code(data.phone)
        return res
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/auth/verify_code")
async def verify_code(data: CodeModel):
    try:
        res = await telegram_service.sign_in_code(data.code)
        return res
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/auth/verify_2fa")
async def verify_2fa(data: PasswordModel):
    try:
        res = await telegram_service.sign_in_2fa(data.password)
        return res
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/auth/logout")
async def logout():
    try:
        return await telegram_service.log_out()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- Channel Endpoints ---

@app.get("/api/channels")
async def list_channels(refresh: bool = False):
    try:
        channels = await telegram_service.fetch_dialogs(force_refresh=refresh)
        return {"channels": channels}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/channels/add")
async def add_channel(data: AddChannelModel):
    try:
        ch = await telegram_service.add_or_resolve_channel(data.identifier)
        return {"status": "ok", "channel": ch}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- Task Endpoints ---

@app.get("/api/tasks")
async def list_tasks():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tasks ORDER BY id DESC")
    tasks = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {"tasks": tasks}

@app.post("/api/tasks")
async def create_task(data: TaskCreateModel):
    name = data.name or f"{data.source_title} ➔ {data.destination_title}"
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO tasks (
            name, source_id, source_title, destination_id, destination_title,
            mode, reupload_mode, media_filter, delay_seconds,
            min_msg_id, max_msg_id, reverse_order, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'idle')
    """, (
        name, data.source_id, data.source_title, data.destination_id, data.destination_title,
        data.mode, data.reupload_mode, data.media_filter, data.delay_seconds,
        data.min_msg_id, data.max_msg_id, data.reverse_order
    ))
    task_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return {"status": "ok", "task_id": task_id}

@app.post("/api/tasks/{task_id}/start")
async def start_task(task_id: int):
    try:
        await worker.start_task(task_id)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/tasks/{task_id}/pause")
async def pause_task(task_id: int):
    try:
        worker.pause_task(task_id)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/tasks/{task_id}/stop")
async def stop_task(task_id: int):
    try:
        worker.stop_task(task_id)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/tasks/{task_id}/reset")
async def reset_task(task_id: int):
    worker.stop_task(task_id)
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE tasks 
        SET status = 'idle', processed_messages = 0, forwarded_messages = 0, 
            failed_messages = 0, last_source_msg_id = 0, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (task_id,))
    cursor.execute("DELETE FROM forwarded_records WHERE task_id = ?", (task_id,))
    conn.commit()
    conn.close()
    await worker.broadcast_task_update(task_id)
    return {"status": "ok"}

@app.delete("/api/tasks/{task_id}")
async def delete_task(task_id: int):
    worker.stop_task(task_id)
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
    cursor.execute("DELETE FROM forwarded_records WHERE task_id = ?", (task_id,))
    conn.commit()
    conn.close()
    return {"status": "ok"}

# --- WebSocket ---

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keepalive ping
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)

# --- Frontend Serving ---

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if not os.path.exists(STATIC_DIR):
    os.makedirs(STATIC_DIR)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/")
async def root():
    index_file = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return JSONResponse({"status": "API online", "docs": "/docs"})
