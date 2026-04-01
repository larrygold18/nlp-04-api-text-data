"""
config_sandra.py

Purpose

  Store configuration values for the EVTL pipeline.
"""

from pathlib import Path

# ============================================================
# API CONFIGURATION
# ============================================================

API_URL: str = "https://jsonplaceholder.typicode.com/posts"

HTTP_REQUEST_HEADERS: dict[str, str] = {
    "User-Agent": "sandra-api-text-project/1.0",
    "Accept": "application/json",
}

# ============================================================
# PATH CONFIGURATION
# ============================================================

ROOT_PATH: Path = Path.cwd()
DATA_PATH: Path = ROOT_PATH / "data"
RAW_PATH: Path = DATA_PATH / "raw"
PROCESSED_PATH: Path = DATA_PATH / "processed"

# Custom output files
RAW_JSON_PATH: Path = RAW_PATH / "sandra_raw.json"
PROCESSED_CSV_PATH: Path = PROCESSED_PATH / "sandra_processed.csv"
