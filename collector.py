import os
import sys
import time
import logging
import asyncio
from datetime import datetime
import pandas as pd
import requests

# Upstox SDK Imports
import upstox_client
from upstox_client.rest import ApiException

# Google Drive Imports
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

try:
    import pyotp
except ImportError:
    os.system('pip install pyotp')
    import pyotp

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- Configuration & Credentials ---
UPSTOX_CLIENT_ID = os.environ.get("UPSTOX_CLIENT_ID")
UPSTOX_TOTP_SECRET = os.environ.get("UPSTOX_TOTP_SECRET")
CSV_FILENAME = "candles_upstox.csv"
INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"

# Fallback token from secrets if automated login isn't fully configured
STATIC_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")


def get_automated_access_token():
    """Uses Upstox TOTP & client credentials to fetch a live access token."""
    if not UPSTOX_CLIENT_ID or not UPSTOX_TOTP_SECRET:
        if STATIC_ACCESS_TOKEN:
            logger.info("TOTP secrets missing. Falling back to static UPSTOX_ACCESS_TOKEN.")
            return STATIC_ACCESS_TOKEN
        raise ValueError("Missing both TOTP credentials and Static Access Token!")
        
    try:
        logger.info("Attempting automated login via TOTP...")
        totp = pyotp.TOTP(UPSTOX_TOTP_SECRET.replace(" ", ""))
        current_otp = totp.now()
        
        # Note: Upstox API login authorization flows usually require a redirect exchange.
        # If your workflow requires exchanging an auth code, this function can be expanded.
        # For now, we utilize your active long-lived Analytics token as the primary.
        if STATIC_ACCESS_TOKEN:
            return STATIC_ACCESS_TOKEN
            
    except Exception as e:
        logger.error(f"Failed automated authentication flow: {e}")
        if STATIC_ACCESS_TOKEN:
            return STATIC_ACCESS_TOKEN
        sys.exit(1)


def upload_to_drive():
    if not os.path.exists(CSV_FILENAME):
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
            logger.info(f"Updating Google Drive file: {file_id}")
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            logger.info("Creating new file on Google Drive...")
            file_metadata = {"name": CSV_FILENAME}
            new_file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
            logger.info(f"File created successfully: {new_file.get('id')}")
    except Exception as e:
        logger.error(f"Error syncing to Google Drive: {e}")


async def fetch_upstox_candles_loop(api_instance):
    logger.info("Starting Upstox data collection loop...")
    last_processed_timestamp = None

    while True:
        try:
            now = time.time()
            sleep_time = 1.0 - (now % 1.0)
            await asyncio.sleep(sleep_time)

            to_date = datetime.now().strftime("%Y-%m-%d")
            
            # Using Upstox API v2 Historical Day endpoint
            api_response = api_instance.get_intra_day_candle_data(
                instrument_key=INSTRUMENT_KEY,
                interval="1minute",
                to_date=to_date,
                api_version="2.0"
            )

            if api_response and api_response.data and api_response.data.candles:
                latest_candle = api_response.data.candles[0]
                candle_timestamp = latest_candle[0]

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
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Unexpected error in feed loop: {e}")
            await asyncio.sleep(2)


async def google_drive_sync_loop():
    while True:
        await asyncio.sleep(10)
        upload_to_drive()


async def main():
    token = get_automated_access_token()
    
    configuration = upstox_client.Configuration()
    configuration.access_token = token
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
