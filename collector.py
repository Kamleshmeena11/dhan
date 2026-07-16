import json, os, sys, time, datetime, threading, queue, requests
from dhanhq import dhanhq
from dhanhq import DhanFeed
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io
import asyncio

# =====================================================================
# CONFIGURATION — loaded from environment variables (GitHub Secrets).
# Never hardcode credentials in this file.
# =====================================================================
def _require_env(name):
    val = os.environ.get(name)
    if not val:
        print(f"❌ Missing required environment variable: {name}")
        sys.exit(1)
    return val

# Dhan-specific API parameters
DHAN_CLIENT_ID    = _require_env("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN  = _require_env("DHAN_ACCESS_TOKEN")

# Instrument Mapping for Nifty 50 Index (Exchange segment 0, Security ID "13")
# You can verify this ID in the Dhan security master database if needed.
DHAN_EXCHANGE_SEG  = 0     # 0 = NSE Index
DHAN_SECURITY_ID   = "13"    # "13" is usually NIFTY 50 Index on Dhan
INSTRUMENTS        = [(DHAN_EXCHANGE_SEG, DHAN_SECURITY_ID)]

GOOGLE_CLIENT_ID      = _require_env("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET  = _require_env("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN  = _require_env("GOOGLE_REFRESH_TOKEN")

DRIVE_SYNC_INTERVAL_SECONDS = 5
GOOGLE_SCOPES   = ["https://www.googleapis.com/auth/drive.file"]
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID") or None

MARKET_OPEN_IST_HOUR    = 9
MARKET_OPEN_IST_MINUTE  = 15
MARKET_CLOSE_IST_HOUR   = 15
MARKET_CLOSE_IST_MINUTE = 35
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

# =====================================================================
# FILE LAYOUT
# =====================================================================
DAILY_FOLDER_NAME = "daily_bars"
DRIVE_DAILY_SUBFOLDER_NAME = "daily_bars"

TODAY_STR   = datetime.datetime.now(IST).strftime("%Y-%m-%d")
DAILY_FILE  = os.path.join(DAILY_FOLDER_NAME, f"candles_1s_{TODAY_STR}.csv")
COMBINED_FILE = "candles_1s_all.csv"

CSV_HEADER = "Timestamp,Open,High,Low,Close,Volume\n"
GAP_LOG_FILE = os.path.join(DAILY_FOLDER_NAME, f"gaps_{TODAY_STR}.log")

os.makedirs(DAILY_FOLDER_NAME, exist_ok=True)

SUCCESS, ERROR = 1, -1
start_time = time.time()

# =====================================================================
# RUNTIME & MARKET CHECKS
# =====================================================================
def compute_max_runtime_seconds():
    now_ist   = datetime.datetime.now(IST)
    close_ist = now_ist.replace(
        hour=MARKET_CLOSE_IST_HOUR, minute=MARKET_CLOSE_IST_MINUTE,
        second=0, microsecond=0
    )
    remaining = (close_ist - now_ist).total_seconds()
    return int(remaining) if remaining > 0 else 60

def wait_until_market_open():
    now_ist   = datetime.datetime.now(IST)
    open_ist  = now_ist.replace(
        hour=MARKET_OPEN_IST_HOUR, minute=MARKET_OPEN_IST_MINUTE,
        second=0, microsecond=0
    )
    close_ist = now_ist.replace(
        hour=MARKET_CLOSE_IST_HOUR, minute=MARKET_CLOSE_IST_MINUTE,
        second=0, microsecond=0
    )
    if now_ist >= close_ist:
        print(f"⛔ Market already closed for today. Exiting.")
        sys.exit(0)
    if now_ist < open_ist:
        wait_seconds = (open_ist - now_ist).total_seconds()
        print(f"⏳ Waiting {int(wait_seconds)}s for market open …")
        time.sleep(wait_seconds)
    else:
        print(f"▶️  Market already open — starting immediately.")

# =====================================================================
# GAP / DIAGNOSTIC LOGGING
# =====================================================================
def log_gap(text):
    line = f"[{datetime.datetime.now(IST).strftime('%H:%M:%S')}] {text}"
    print(line)
    try:
        with open(GAP_LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass

# =====================================================================
# TICK QUEUE
# =====================================================================
tick_queue = queue.Queue()

# =====================================================================
# 1-SECOND CANDLE STATE (Consumer Thread)
# =====================================================================
current_bar_second = None
o = h = l = c = None
bar_start_vol = None
last_vol      = None

def _write_bar(second, o_, h_, l_, c_, vol_):
    utc_dt = datetime.datetime.fromtimestamp(second, tz=datetime.timezone.utc)
    ist_dt = utc_dt.astimezone(IST)
    ts  = ist_dt.strftime("%Y-%m-%d %H:%M:%S")

    row = f"{ts},{o_},{h_},{l_},{c_},{vol_}\n"

    with open(DAILY_FILE, "a") as f:
        f.write(row)
    with open(COMBINED_FILE, "a") as f:
        f.write(row)

    print(f"🕐 1s Candle: {row.strip()}")

def _start_new_bar(second, price, vol):
    global current_bar_second, o, h, l, c, bar_start_vol, last_vol
    current_bar_second = second
    o = h = l = c = price
    bar_start_vol = vol
    last_vol      = vol

def _flush_current_bar():
    global current_bar_second, o, h, l, c, bar_start_vol, last_vol
    if current_bar_second is None:
        return
    bar_volume = (
        (last_vol - bar_start_vol)
        if (last_vol is not None and bar_start_vol is not None)
        else 0
    )
    if bar_volume < 0:
        bar_volume = 0
    _write_bar(current_bar_second, o, h, l, c, bar_volume)
    current_bar_second = None

def _process_tick(price, vol, tick_second):
    global o, h, l, c, last_vol, bar_start_vol, current_bar_second

    if current_bar_second is None:
        _start_new_bar(tick_second, price, vol)
        return

    if tick_second == current_bar_second:
        h        = max(h, price)
        l        = min(l, price)
        c        = price
        last_vol = vol
    elif tick_second > current_bar_second:
        bar_volume = (
            (last_vol - bar_start_vol)
            if (last_vol is not None and bar_start_vol is not None)
            else 0
        )
        if bar_volume < 0:
            bar_volume = 0
        _write_bar(current_bar_second, o, h, l, c, bar_volume)

        missing_seconds = tick_second - current_bar_second - 1
        if missing_seconds > 0:
            log_gap(f"⚠️  Gap detected: {missing_seconds} second(s) with no ticks "
                     f"between {current_bar_second} and {tick_second}.")

        _start_new_bar(tick_second, price, vol)
    else:
        log_gap(f"⚠️  Late/out-of-order tick for second {tick_second} "
                 f"arrived after bar {current_bar_second} was already open — dropped.")

def bar_builder_loop():
    while True:
        message = tick_queue.get()
        if message is None:
            break
        price = message.get("LTP")
        vol = message.get("volume", 0)
        exch_ts = message.get("exchange_time")  # Dhan uses 'exchange_time'
        if price is None or exch_ts is None:
            continue
        _process_tick(price, vol, int(exch_ts))

# =====================================================================
# DHAN WEBSOCKET CALLBACKS (Asynchronous)
# =====================================================================
async def on_connect(instance):
    print("✅ Dhan Live feed connected.")
    # Subscribe using Dhan's feed style
    # Subscribing to "Ticker" style (for LTP, exchange_time, and volume)
    await instance.subscribe_symbols(marketfeed.Ticker, INSTRUMENTS)
    print(f"📡 Subscribed to instruments: {INSTRUMENTS}")

async def on_message(instance, message):
    # Dhan passes updates as structured dictionaries
    if "LTP" in message:
        tick_queue.put(message)

# Dhan Websocket Core Runner Thread
def run_dhan_websocket():
    # subscription_code 2 corresponds to marketfeed.Ticker (LTP + volume)
    feed = DhanFeed(
        client_id=DHAN_CLIENT_ID,
        access_token=DHAN_ACCESS_TOKEN,
        instruments=INSTRUMENTS,
        subscription_code=marketfeed.Ticker,
        on_connect=on_connect,
        on_message=on_message
    )
    # run_forever starts the internal event loop and handles reconnects
    feed.run_forever()

# =====================================================================
# WATCHDOG — kills process at market close
# =====================================================================
def runtime_watchdog():
    time.sleep(MAX_RUNTIME_SECONDS)
    print("⏱️  Session end reached — flushing last candle + final Drive sync …")
    time.sleep(1)
    _flush_current_bar()
    try:
        sync_all_to_drive()
    except Exception as e:
        print(f"⚠️  Final Drive sync failed: {e}")
    os._exit(0)

# =====================================================================
# GOOGLE DRIVE SYNC
# =====================================================================
def get_drive_service():
    creds = Credentials(
        token=None, refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID, client_secret=GOOGLE_CLIENT_SECRET,
        scopes=GOOGLE_SCOPES,
    )
    creds.refresh(Request())
    return build("drive", "v3", credentials=creds)

_drive_service_cache = {}
_drive_daily_folder_id_cache = {}
_daily_safe_to_sync = False
_combined_safe_to_sync = False

def _get_service():
    if "service" not in _drive_service_cache:
        _drive_service_cache["service"] = get_drive_service()
    return _drive_service_cache["service"]

def _reset_drive_service():
    _drive_service_cache.clear()

def _execute_with_retry(build_request_fn, max_retries=3, what=""):
    last_err = None
    for attempt in range(1, max_retries + 1):
        service = _get_service()
        try:
            return build_request_fn(service).execute()
        except Exception as e:
            last_err = e
            label = f" ({what})" if what else ""
            print(f"⚠️  Drive API call failed{label}, attempt {attempt}/{max_retries}: {e}")
            _reset_drive_service()
            time.sleep(1.5 * attempt)
    raise last_err

def _find_drive_file_id(filename, parent_id):
    query_parts = [f"name = '{filename}'", "trashed = false"]
    if parent_id and str(parent_id).strip() and str(parent_id).strip() != "None":
        query_parts.append(f"'{str(parent_id).strip()}' in parents")
    query = " and ".join(query_parts)

    def _req(service):
        return service.files().list(q=query, spaces="drive", fields="files(id, name)")

    results = _execute_with_retry(_req, what=f"find '{filename}'")
    files = results.get("files", [])
    return files[0]["id"] if files else None

def _get_or_create_daily_subfolder():
    if "id" in _drive_daily_folder_id_cache:
        return _drive_daily_folder_id_cache["id"]

    query_parts = [
        f"name = '{DRIVE_DAILY_SUBFOLDER_NAME}'",
        "trashed = false",
        "mimeType = 'application/vnd.google-apps.folder'"
    ]
    if DRIVE_FOLDER_ID and str(DRIVE_FOLDER_ID).strip() and str(DRIVE_FOLDER_ID).strip() != "None":
        query_parts.append(f"'{str(DRIVE_FOLDER_ID).strip()}' in parents")

    query = " and ".join(query_parts)

    def _list_req(service):
        return service.files().list(q=query, spaces="drive", fields="files(id, name)")

    results = _execute_with_retry(_list_req, what="find daily subfolder")
    files = results.get("files", [])

    if files:
        folder_id = files[0]["id"]
    else:
        metadata = {
            "name": DRIVE_DAILY_SUBFOLDER_NAME,
            "mimeType": "application/vnd.google-apps.folder",
        }
        if DRIVE_FOLDER_ID and str(DRIVE_FOLDER_ID).strip() and str(DRIVE_FOLDER_ID).strip() != "None":
            metadata["parents"] = [str(DRIVE_FOLDER_ID).strip()]

        def _create_req(service):
            return service.files().create(body=metadata, fields="id")

        folder = _execute_with_retry(_create_req, what="create daily subfolder")
        folder_id = folder["id"]
        print(f"📁 Created Drive folder '{DRIVE_DAILY_SUBFOLDER_NAME}'.")

    _drive_daily_folder_id_cache["id"] = folder_id
    return folder_id

def upload_or_update_drive(local_path, parent_id):
    filename = os.path.basename(local_path)
    existing_id = _find_drive_file_id(filename, parent_id)

    def _req(service):
        media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
        if existing_id:
            return service.files().update(fileId=existing_id, media_body=media)
        metadata = {"name": filename}
        if parent_id and str(parent_id).strip() and str(parent_id).strip() != "None":
            metadata["parents"] = [str(parent_id).strip()]
        return service.files().create(body=metadata, media_body=media, fields="id")

    try:
        _execute_with_retry(_req, what=f"upload '{filename}'")
        print(f"☁️  Synced '{filename}' → Google Drive.")
    finally:
        _reset_drive_service()

def sync_all_to_drive():
    global _daily_safe_to_sync, _combined_safe_to_sync
    daily_folder_id = _get_or_create_daily_subfolder()

    if _daily_safe_to_sync:
        upload_or_update_drive(DAILY_FILE, daily_folder_id)
    else:
        print("⏸️  Skipping daily-file sync — not yet confirmed safe.")
        try:
            result = _download_from_drive(
                os.path.basename(DAILY_FILE), daily_folder_id, DAILY_FILE
            )
            _daily_safe_to_sync = True
            print(f"⬇️  Daily file now confirmed ({result}).")
        except Exception as e:
            print(f"⚠️  Still can't confirm daily file: {e}")

    if _combined_safe_to_sync:
        upload_or_update_drive(COMBINED_FILE, DRIVE_FOLDER_ID)
    else:
        print("⏸️  Skipping combined-file sync — not yet confirmed safe.")
        try:
            result = _download_from_drive(
                os.path.basename(COMBINED_FILE), DRIVE_FOLDER_ID, COMBINED_FILE
            )
            _combined_safe_to_sync = True
            print(f"⬇️  Combined file now confirmed ({result}).")
        except Exception as e:
            print(f"⚠️  Still can't confirm combined file: {e}")

def _download_from_drive(filename, parent_id, local_path):
    file_id = _find_drive_file_id(filename, parent_id)
    if not file_id:
        return "none_found"

    def _do_download():
        service = _get_service()
        request = service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()

    last_err = None
    for attempt in range(1, 4):
        try:
            data = _do_download()
            with open(local_path, "wb") as f:
                f.write(data)
            return "downloaded"
        except Exception as e:
            last_err = e
            print(f"⚠️  Drive download of '{filename}' failed, attempt {attempt}/3: {e}")
            _reset_drive_service()
            time.sleep(1.5 * attempt)
        finally:
            _reset_drive_service()
    raise last_err

def bootstrap_local_files():
    global _daily_safe_to_sync, _combined_safe_to_sync

    try:
        daily_folder_id = _get_or_create_daily_subfolder()
        result = _download_from_drive(
            os.path.basename(DAILY_FILE), daily_folder_id, DAILY_FILE
        )
        _daily_safe_to_sync = True
        print(f"⬇️  Daily file: {result} (safe to sync).")
    except Exception as e:
        print(f"⚠️  Could not verify daily file: {e}")

    try:
        result = _download_from_drive(
            os.path.basename(COMBINED_FILE), DRIVE_FOLDER_ID, COMBINED_FILE
        )
        _combined_safe_to_sync = True
        print(f"⬇️  Combined file: {result} (safe to sync).")
    except Exception as e:
        print(f"⚠️  Could not verify combined file: {e}")

    for _f in (DAILY_FILE, COMBINED_FILE):
        if not os.path.exists(_f):
            with open(_f, "w") as fh:
                fh.write(CSV_HEADER)

def drive_sync_loop():
    while not (os.path.exists(DAILY_FILE) and os.path.exists(COMBINED_FILE)):
        time.sleep(2)
    while True:
        try:
            sync_all_to_drive()
        except Exception as e:
            print(f"⚠️  Drive sync error: {e}")
        time.sleep(DRIVE_SYNC_INTERVAL_SECONDS)

# =====================================================================
# MAIN
# =====================================================================
if __name__ == "__main__":
    wait_until_market_open()
    MAX_RUNTIME_SECONDS = compute_max_runtime_seconds()
    start_time = time.time()
    print(f"⏱️  Auto-stop in {MAX_RUNTIME_SECONDS}s.")
    print(f"📄 Today's file: {DAILY_FILE}")
    print(f"📄 Combined file: {COMBINED_FILE}")
    print(f"📄 Gap log: {GAP_LOG_FILE}")

    # No complex manual login endpoints anymore!
    print("✅ Using Dhan direct client/token configuration setup.")

    bootstrap_local_files()

    # Start independent processing / syncing loops
    threading.Thread(target=drive_sync_loop,  daemon=True).start()
    threading.Thread(target=runtime_watchdog, daemon=True).start()
    threading.Thread(target=bar_builder_loop, daemon=True).start()

    # Start Dhan WebSocket client in its own daemon thread
    ws_thread = threading.Thread(target=run_dhan_websocket, daemon=True)
    ws_thread.start()

    # Keep main thread alive as long as watchdog hasn't triggered shutdown
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Script interrupted by user. Exiting.")
        sys.exit(0)
