import requests
import json
import logging
from datetime import datetime
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET_NAME = "cdc-health-ingestion-bucket"
RAW_PREFIX = "raw/"
CDC_URL = "https://data.cdc.gov/resource/akn2-qxic.json"
REQUEST_TIMEOUT = 30

def run():
    logger.info("Starting CDC ingestion job...")

    # Step 1: Fetch CDC API
    try:
        response = requests.get(CDC_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        json_data = response.json()
        logger.info(f"Fetched {len(json_data)} records")
    except Exception as e:
        logger.error(f"API error: {e}")
        raise

    # Step 2: Store to GCS
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)

        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{RAW_PREFIX}cdc_data_{ts}.json"

        blob = bucket.blob(filename)
        blob.upload_from_string(json.dumps(json_data, indent=2), content_type="application/json")

        logger.info(f"Uploaded file: gs://{BUCKET_NAME}/{filename}")

    except Exception as e:
        logger.error(f"GCS upload error: {e}")
        raise

    logger.info("CDC ingestion completed successfully.")
