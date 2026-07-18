import os
import sys
import time
import logging
import asyncio
from datetime import datetime
import pandas as pd

# Upstox SDK & Streaming Tools
import upstox_client
from upstox_client.feeder import MarketDataStreamerV3

# Google Drive Modules
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- Configuration & Credentials ---
UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")
INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
CSV_FILENAME = "candles_1s_upstox.csv"

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")

# Global memory array to store streaming raw ticks inside the 1-second interval window
tick_store = []


def process_ticks_to_1s():
    """Compiles all raw stream ticks collected over the past second into an OHLCV bar."""
    global tick_store
    if not tick_store:
        return

    now = datetime.now()
    timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

    prices = [t["price"] for t in tick_store]
    volumes = [t.get("volume", 0) for t in tick_store]

    new_row = {
        "timestamp": timestamp_str,
        "instrument_key": INSTRUMENT_KEY,
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "volume": sum(volumes)
    }

    # Wipe queue clean for the next second's window immediately
    tick_store = []

    # Write data row straight to the destination CSV
    df = pd.DataFrame([new_row])
    file_exists = os.path.isfile(CSV_FILENAME)
    df.to_csv(CSV_FILENAME, mode="a", index=False, header=not file_exists)
    logger.info(f"Saved 1s Bar -> {timestamp_str} | Close: {new_row['close']}")


def upload_to_drive():
    if not os.path.exists(CSV_FILENAME):
        return
    if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
        return
    try:
        creds = Credentials(
            token=None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET
        )
        service = build("drive", "v3", credentials=creds)
        query = f"name = '{CSV_FILENAME}' and trashed = false"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        media = MediaFileUpload(CSV_FILENAME, mimetype="text/csv", resumable=True)
        if files:
            file_id = files[0]["id"]
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            file_metadata = {"name": CSV_FILENAME}
            service.files().create(body=file_metadata, media_body=media).execute()
    except Exception as e:
        logger.error(f"Google Drive Sync Failure: {e}")


async def seconds_timer_loop():
    """Triggers the OHLC computation loop precisely at the turn of every clock second."""
    while True:
        now = time.time()
        sleep_time = 1.0 - (now % 1.0)
        await asyncio.sleep(sleep_time)
        process_ticks_to_1s()


async def google_drive_sync_loop():
    while True:
        await asyncio.sleep(10)
        upload_to_drive()


def on_open():
    logger.info("Successfully established connection to Upstox Market Stream Feed.")


def on_message(feed_dict):
    """
    Callback invoked by MarketDataStreamerV3 for every decoded protobuf feed message.
    feed_dict is already a plain dict (via protobuf json_format.MessageToDict).
    """
    global tick_store
    try:
        feeds = feed_dict.get("feeds", {})
        feed = feeds.get(INSTRUMENT_KEY)
        if not feed:
            return

        full_feed = feed.get("fullFeed", {})

        # NSE_INDEX instruments populate indexFF, not marketFF (indices have no
        # traded-quantity/market-depth fields the way equities/derivatives do).
        ltpc = full_feed.get("indexFF", {}).get("ltpc") \
            or full_feed.get("marketFF", {}).get("ltpc") \
            or feed.get("ltpc")

        if not ltpc:
            return

        ltp = ltpc.get("ltp")
        ltq = ltpc.get("ltq", 0)

        if ltp is not None:
            tick_store.append({
                "price": float(ltp),
                "volume": float(ltq)
            })
    except Exception as e:
        logger.error(f"Error reading live feed update: {e}")


def on_error(error):
    logger.error(f"Upstox WebSocket Feed Error: {error}")


def on_close(close_status_code, close_msg):
    logger.info(f"WebSocket connection closed. Code: {close_status_code}, Msg: {close_msg}")


def start_streamer(token):
    """Configures and connects the MarketDataStreamerV3 (non-blocking; runs on its own thread)."""
    configuration = upstox_client.Configuration()
    configuration.access_token = token
    api_client = upstox_client.ApiClient(configuration)

    streamer = MarketDataStreamerV3(api_client, [INSTRUMENT_KEY], "full")
    streamer.on("open", on_open)
    streamer.on("message", on_message)
    streamer.on("error", on_error)
    streamer.on("close", on_close)

    streamer.connect()  # spawns its own background thread; returns immediately
    return streamer


async def main():
    if not UPSTOX_ACCESS_TOKEN:
        logger.error("UPSTOX_ACCESS_TOKEN configuration secret is missing!")
        sys.exit(1)

    start_streamer(UPSTOX_ACCESS_TOKEN)

    logger.info("Starting up active 1s real-time loops...")
    await asyncio.gather(
        seconds_timer_loop(),
        google_drive_sync_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script stopped manually by user.")
