import os
import json
from pathlib import Path
from typing import Optional, Dict, Any

BASE_DIR = Path(__file__).resolve().parent
TEMP_DOWNLOAD_DIR = BASE_DIR / "temp_downloads"
TEMP_DOWNLOAD_DIR.mkdir(exist_ok=True)

SESSION_NAME = str(BASE_DIR / "telegram_forwarder")
CONFIG_FILE = BASE_DIR / "config.json"

DEFAULT_CONFIG = {
    "api_id": None,
    "api_hash": None,
    "phone": None,
    "host": "127.0.0.1",
    "port": 5000,
    "auto_open_browser": True
}

def load_config() -> Dict[str, Any]:
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return {**DEFAULT_CONFIG, **data}
        except Exception:
            return DEFAULT_CONFIG.copy()
    return DEFAULT_CONFIG.copy()

def save_config(updates: Dict[str, Any]):
    current = load_config()
    current.update(updates)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(current, f, indent=4)
    return current
