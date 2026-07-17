import os
import sys
import time
import logging
import asyncio
from datetime import datetime, timedelta
import pandas as pd

# Upstox SDK Imports
import upstox_client
from upstox_client.rest import ApiException

# Google Drive Imports
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

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")

# Instrument Key for Nifty 50 Index (Upstox format)
INSTRUMENT_KEY = "NSE_INDEX|Nifty 50" 
INTERVAL = "1minute" # Upstox API native options: 1minute, 30minute, day, etc.
CSV_FILENAME = "candles_upstox.csv"


def upload_to_drive():
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


async def fetch_upstox_candles_loop(api_instance):
    """
    Polls the Upstox Historical/Intraday API every second to fetch completed 
    or live candle data, directly bypassing manual tick aggregation.
    """
    logger.info("Starting Upstox data collection loop...")
    last_processed_timestamp = None

    while True:
        try:
            # Align loop to fire exactly on the 1-second boundary
            now = time.time()
            sleep_time = 1.0 - (now % 1.0)
            await asyncio.sleep(sleep_time)

            # Define historical data window (fetching today's candles)
            to_date = datetime.now().strftime("%Y-%m-%d")
            
            # API Call to fetch intraday candles
            api_response = api_instance.get_intra_day_candle_data(
                instrument_key=INSTRUMENT_KEY,
                interval=INTERVAL,
                to_date=to_date,
                api_version="2.0"
            )

            if api_response and api_response.data and api_response.data.candles:
                # Upstox returns candles in descending order (latest first)
                # Structure: [timestamp, open, high, low, close, volume, open_interest]
                latest_candle = api_response.data.candles[0]
                candle_timestamp = latest_candle[0]

                # Prevent writing duplicate rows for the same timestamp block
                if candle_timestamp != last_processed_timestamp:
                    new_row = {
                        "timestamp": candle_timestamp,
                        "instrument_key": INSTRUMENT_KEY,
                        "open": latest_candle[1],
                        "high": latest_candle[2],
                        "low": latest_candle[3],
                        "close": latest_candle[4],
                        "volume": latest_candle[5]
                    }

                    df = pd.DataFrame([new_row])
                    file_exists = os.path.isfile(CSV_FILENAME)
                    df.to_csv(CSV_FILENAME, mode="a", index=False, header=not file_exists)
                    
                    logger.info(f"Saved candle for timestamp: {candle_timestamp}")
                    last_processed_timestamp = candle_timestamp

        except ApiException as e:
            logger.error(f"Upstox API Exception: {e}")
            await asyncio.sleep(2) # Backoff briefly on API failure
        except Exception as e:
            logger.error(f"Unexpected error in feed loop: {e}")
            await asyncio.sleep(2)


async def google_drive_sync_loop():
    while True:
        await asyncio.sleep(10)
        upload_to_drive()


async def main():
    if not UPSTOX_ACCESS_TOKEN:
        logger.error("Upstox access token (UPSTOX_ACCESS_TOKEN) is missing!")
        sys.exit(1)

    # Configure Upstox API client
    configuration = upstox_client.Configuration()
    configuration.access_token = UPSTOX_ACCESS_TOKEN
    api_instance = upstox_client.HistoryApi(upstox_client.ApiClient(configuration))

    tasks = [
        asyncio.create_task(fetch_upstox_candles_loop(api_instance)),
        asyncio.create_task(google_drive_sync_loop())
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script stopped manually by user.")
