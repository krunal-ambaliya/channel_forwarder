# TeleForward — Autonomous Telegram Channel Forwarding & Cloning System

An autonomous Telegram forwarding system with multi-threading, persistent login, and an interactive modern web dashboard.

---

## Key Features

1. **Clean Copy (Zero-Source Header Guarantee)**
   - **No "Forwarded from" tags**: Messages, media, videos, images, and files are cloned cleanly as fresh posts sent by your account.
   - **Immune to Source Bans & Deletions**: Because messages are posted independently (or re-uploaded), if the source channel is banned, deleted, or removes media, **your destination channel's posts remain 100% intact**.
   - **Restricted Content Bypass (Deep Clone & Re-upload)**: Automatically handles or downloads & re-uploads media if saving/forwarding is restricted on the source channel (`chat.noforwards`).
   - **wzgram Ultra-Speed Transfer Engine (`wzgram` + `WarpCrypto` Rust)**:
     - Rust-powered MTProto acceleration via **WarpCrypto**.
     - 24 parallel concurrent chunk transmissions (`max_concurrent_transmissions=24`) with 16 async workers.
     - Maximum throughput for large files (500 MB – 2 GB+) saturating your internet line.
     - Real-time speed (`MB/s`), progress (`%`), and `ETA` streaming directly in the dashboard console.
     - Smart zero-disk I/O fallback: Server-side clean clone by default without downloading a single byte when not restricted.

2. **Persistent Session (Login Once, Remember Forever)**
   - Login once using your phone number and Telegram verification code (supports 2FA).
   - Session is saved to `telegram_forwarder.session` SQLite database.
   - **You never have to log in again** when restarting the application or rebooting your computer.

3. **Interactive Modern Web Dashboard**
   - **Dual Channel Selectors**: Pick Source and Destination channels easily from your account's joined channels and groups.
   - **Dedicated Refresh & Add Buttons**: Both the **Source** and **Destination** dropdowns include their own:
     - 🔄 **Refresh button**: Quickly reload and fetch new channels from Telegram.
     - ➕ **Add button**: Join or resolve any channel by `@username`, invite link (`t.me/+xyz`), or ID with auto-selection.
   - **Multi-Threading / Multi-Tasking**: Run multiple forwarding tasks concurrently (e.g., Channel A ➔ Channel B, and Channel C ➔ Channel D).
   - **Modes**:
     - *History*: Clones all past messages chronologically (oldest to newest).
     - *Live*: Autonomous real-time listener forwarding new incoming posts immediately.
     - *Both*: Clones all past history first, then transitions automatically into live listening!
   - **Filters**: All messages, Media Only, Videos Only, Photos Only, Documents/Files Only, or Plain Text Only.
   - **Rate-limit / Flood-Wait Defense**: Automatic intelligent wait and retry on Telegram's rate limits.
   - **Live Activity Console**: Real-time streaming log terminal with color-coded events.

---

## Quick Start

### 1. Start the Application

**On Windows:**
```powershell
python run.py
```

**On Linux / Google Cloud Shell:**
```bash
chmod +x start.sh
./start.sh
```
This automatically sets up Python `venv`, installs `requirements.txt`, and launches the web dashboard.
- In Google Cloud Shell: Click the **Web Preview** icon (top right) ➔ **Change port to 5000** ➔ **Preview and Run**.

### 2. Connect Your Account
1. Click **"Connect Telegram"** on the dashboard.
2. Enter your `API ID` and `API Hash` (obtainable for free from [my.telegram.org](https://my.telegram.org)).
3. Enter your phone number (e.g. `+1234567890`) and the verification code sent to your Telegram app.
4. If you have Two-Step Verification (2FA) enabled, enter your password.
5. Your session will be saved persistently!

### 3. Create a Forwarding Task
1. Select your **Source Channel** from the dropdown.
2. Select your **Destination Channel** where you have posting permissions.
3. Choose your **Mode** (History, Live, or Both).
4. Click **"Create Autonomous Task"**.
5. Monitor progress, speed, copied message counts, and live logs in real time!
