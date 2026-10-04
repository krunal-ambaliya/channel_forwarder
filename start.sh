#!/usr/bin/env bash
# ==============================================================================
# TeleForward - Autonomous Telegram Channel Forwarder
# Setup & Launch Script for Linux / Google Cloud Shell / VPS
# ==============================================================================

set -e

# Detect script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "============================================================"
echo "  TeleForward Setup & Runner"
echo "  Location: $SCRIPT_DIR"
echo "============================================================"

# 1. Check Python installation
if ! command -v python3 &>/dev/null; then
    echo "[-] Error: python3 is not installed or not in PATH."
    exit 1
fi

PYTHON_VER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "[+] Detected Python $PYTHON_VER"

# 2. Setup Virtual Environment (venv)
if [ ! -d "venv" ]; then
    echo "[*] Creating Python virtual environment (venv)..."
    python3 -m venv venv || {
        echo "[-] Failed to create venv. If on Debian/Ubuntu, run: sudo apt-get update && sudo apt-get install -y python3-venv"
        echo "[*] Falling back to system python3 environment..."
    }
fi

if [ -f "venv/bin/activate" ]; then
    echo "[*] Activating virtual environment..."
    source venv/bin/activate
fi

# 3. Upgrade pip and install requirements
echo "[*] Verifying & installing requirements..."
pip install --upgrade pip --quiet
pip install -r requirements.txt

# 4. Set Host & Port (0.0.0.0 allows GCP Cloud Shell Web Preview & external access)
export HOST="${HOST:-0.0.0.0}"
export PORT="${PORT:-5000}"
export NO_BROWSER="${NO_BROWSER:-1}"

echo "============================================================"
echo "  [+] Starting Web Dashboard on http://${HOST}:${PORT}"
echo "  [+] If using Google Cloud Shell:"
echo "      Click 'Web Preview' (top right icon) -> 'Change port' -> 5000"
echo "============================================================"

# 5. Run the application
exec python3 run.py
