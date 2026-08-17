import requests
import os
from pathlib import Path
from prefect import task, get_run_logger

@task(name="Zone 1: Download Raw Database File", retries=2, retry_delay_seconds=5)
def download_file(url: str, filepath: str) -> str:
    """
    Downloads a file from a URL to the raw zone.
    Skips downloading if the file already exists.
    """
    logger = get_run_logger()
    
    # ตรวจสอบว่ามีไฟล์อยู่แล้วหรือไม่
    if os.path.exists(filepath):
        logger.info(f"File '{filepath}' already exists. Skipping download.")
        return filepath

    logger.info(f"Downloading {filepath} from {url}...")
    try:
        # ใช้ stream=True เพื่อจัดการไฟล์ขนาดใหญ่
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        logger.info(f"✅ Successfully downloaded and saved '{filepath}'.")
        return filepath
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error downloading '{filepath}': {e}")
        raise