import os
import sys
import time
import json
import logging
import asyncio
from datetime import datetime
import pandas as pd

# Dhan v2.2.0 imports
from dhanhq import DhanContext, MarketFeed
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
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")

# Google Drive Credentials
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")

# Target Instruments: Nifty 50 Index Spot Only (No Futures)
# Exchange Segment 2 is for NSE Indices. Security ID "13" is Nifty 50 Index.
INSTRUMENTS = [
    (2, "13", MarketFeed.Ticker)  # (Segment 2 = NSE Indices, ID "13", Subscription Type)
]

# State variables to hold tick data
tick_store = {}
CSV_FILENAME = "candles_1s_all.csv"


# --- Google Drive Sync Helper ---
def upload_to_drive():
    """Uploads/Updates the CSV file on Google Drive."""
    if not os.path.exists(CSV_FILENAME):
        logger.info(f"Local file {CSV_FILENAME} does not exist yet. Skipping sync...")
        return

    if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
        logger.warning("Google Drive credentials missing. Skipping cloud backup.")
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

        # Look if file already exists on Drive
        query = f"name = '{CSV_FILENAME}' and trashed = false"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])

        media = MediaFileUpload(CSV_FILENAME, mimetype="text/csv", resumable=True)

        if files:
            file_id = files[0]["id"]
            logger.info(f"Updating existing Google Drive file: {file_id}")
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            logger.info("Creating new file on Google Drive...")
            file_metadata = {"name": CSV_FILENAME}
            new_file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
            logger.info(f"File created successfully with ID: {new_file.get('id')}")

    except Exception as e:
        logger.error(f"Error syncing to Google Drive: {e}")


# --- 1-Second Bar Processing ---
def process_ticks_to_1s():
    """Aggregates collected ticks into 1-second candles and appends to CSV."""
    now = datetime.now()
    timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

    new_rows = []
    for inst_id, ticks in list(tick_store.items()):
        if not ticks:
            continue
        
        prices = [t["price"] for t in ticks]
        volumes = [t.get("volume", 0) for t in ticks]

        open_p = prices[0]
        high_p = max(prices)
        low_p = min(prices)
        close_p = prices[-1]
        total_vol = sum(volumes)

        new_rows.append({
            "timestamp": timestamp_str,
            "instrument_id": inst_id,
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "close": close_p,
            "volume": total_vol
        })
        tick_store[inst_id] = []  # Reset for the next second

    if new_rows:
        df = pd.DataFrame(new_rows)
        file_exists = os.path.isfile(CSV_FILENAME)
        df.to_csv(CSV_FILENAME, mode="a", index=False, header=not file_exists)
        logger.info(f"Saved {len(new_rows)} rows for timestamp {timestamp_str}")


# --- Dhan Feed Callback Handlers (Synchronous for v2.2.0) ---
def on_connect(instance):
    logger.info("Successfully connected to Dhan Market Feed WebSockets.")


def on_message(instance, message):
    """Processes incoming ticker stream messages."""
    try:
        # Check if the message contains valid ticker/LTP fields
        inst_id = message.get("security_id") or message.get("instrument_id")
        price = message.get("last_traded_price") or message.get("price")
        
        if inst_id and price is not None:
            inst_id = str(inst_id)
            if inst_id not in tick_store:
                tick_store[inst_id] = []

            tick_store[inst_id].append({
                "price": float(price),
                "volume": float(message.get("volume", 0))
            })
    except Exception as e:
        logger.error(f"Error handling live feed message: {e}")


# --- Scheduler Loops ---
async def seconds_timer_loop():
    """Runs exactly every second to process bars."""
    while True:
        # Align run with the boundary of the next system second
        now = time.time()
        sleep_time = 1.0 - (now % 1.0)
        await asyncio.sleep(sleep_time)
        process_ticks_to_1s()


async def google_drive_sync_loop():
    """Backup data to Google Drive every 10 seconds."""
    while True:
        await asyncio.sleep(10)
        upload_to_drive()


# --- Main Web Socket Core ---
async def main():
    if not CLIENT_ID or not ACCESS_TOKEN:
        logger.error("Dhan credentials (DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN) are missing!")
        sys.exit(1)

    # 1. Initialize Context & Feed
    dhan_context = DhanContext(client_id=CLIENT_ID, access_token=ACCESS_TOKEN)
    
    feed = MarketFeed(
        dhan_context=dhan_context,
        instruments=INSTRUMENTS,
        version="v2"
    )

    # 2. Assign synchronous callbacks
    feed.on_connect = on_connect
    feed.on_message = on_message

    # 3. Start our concurrent execution tasks
    tasks = [
        asyncio.create_task(feed.connect()),
        asyncio.create_task(seconds_timer_loop()),
        asyncio.create_task(google_drive_sync_loop())
    ]

    logger.info("Starting loops and WebSocket client connection...")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script stopped manually by user.")
