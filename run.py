import sys
import os
import webbrowser
import threading
import time
import asyncio
import uvicorn
from config import load_config

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

def open_browser(host, port):
    time.sleep(1.5)
    url = f"http://{host}:{port}"
    print(f"\n[TeleForward] Launching dashboard at {url} ...\n")
    webbrowser.open(url)

if __name__ == "__main__":
    cfg = load_config()
    host = os.environ.get("HOST", cfg.get("host", "0.0.0.0"))
    port = int(os.environ.get("PORT", cfg.get("port", 5000)))

    if cfg.get("auto_open_browser", True) and os.environ.get("NO_BROWSER", "0") != "1":
        # Only attempt browser launch if display exists or on Windows/macOS
        if sys.platform in ("win32", "darwin") or os.environ.get("DISPLAY"):
            threading.Thread(target=open_browser, args=(host if host != "0.0.0.0" else "127.0.0.1", port), daemon=True).start()

    print("=" * 60)
    print("  >> TeleForward - Autonomous Channel Forwarding System")
    print(f"  >> Dashboard: http://{host}:{port}")
    print("=" * 60)

    uvicorn.run("app:app", host=host, port=port, log_level="info", reload=False)
