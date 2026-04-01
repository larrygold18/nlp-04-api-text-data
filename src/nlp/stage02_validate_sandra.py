"""
stage02_validate_sandra.py

Source: raw JSON object
Sink: validated JSON object

Purpose

  Inspect JSON structure and validate that the data is usable.

Analytical Questions

- What is the top-level structure of the JSON data?
- What keys are present in each record?
- What data types are associated with each field?
- Does the data meet expectations for transformation?
"""

# ============================================================
# Section 1. Setup and Imports
# ============================================================

import logging
from typing import Any

# ============================================================
# Section 2. Define Run Validate Function
# ============================================================


def run_validate(
    json_data: Any,
    LOG: logging.Logger,
) -> list[dict]:
    """Inspect and validate JSON structure."""

    LOG.info("========================")
    LOG.info("STAGE 02: VALIDATE starting...")
    LOG.info("========================")

    # ============================================================
    # INSPECT JSON STRUCTURE
    # ============================================================

    LOG.info("JSON STRUCTURE INSPECTION:")

    # Log top-level type of JSON (list, dict, etc.)
    LOG.info(f"Top-level type: {type(json_data).__name__}")

    # Inspect first record if JSON is a non-empty list
    if isinstance(json_data, list) and len(json_data) > 0:
        first_record = json_data[0]

        # Show available keys
        LOG.info(f"Keys in first record: {list(first_record.keys())}")

        # Show data types of each field
        LOG.info("Field types:")
        for key, value in first_record.items():
            LOG.info(f"{key}: {type(value).__name__}")

    # ============================================================
    # VALIDATE EXPECTATIONS
    # ============================================================

    # Ensure JSON is a list
    if not isinstance(json_data, list):
        raise ValueError("Expected JSON data to be a list of records.")

    # Ensure list is not empty
    if len(json_data) == 0:
        raise ValueError("Expected at least one record.")

    # Ensure each item is a dictionary
    if not all(isinstance(record, dict) for record in json_data):
        raise ValueError("Expected each record to be a dictionary.")

    # Ensure required keys exist
    required_keys = {"userId", "id", "title", "body"}

    for i, record in enumerate(json_data):
        missing = required_keys - set(record.keys())
        if missing:
            raise ValueError(f"Record {i} missing keys: {missing}")

    LOG.info(f"Validation passed for {len(json_data)} records.")
    LOG.info("Sink: validated JSON object")

    return json_data
