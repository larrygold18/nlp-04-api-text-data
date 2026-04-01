"""
stage03_transform_sandra.py

Source: validated JSON object
Sink: Polars DataFrame

Purpose

  Transform validated JSON data into a structured format
  and create derived features for analysis.
"""

# ============================================================
# Section 1. Setup and Imports
# ============================================================

import logging
from typing import Any

import polars as pl

# ============================================================
# Section 2. Define Run Transform Function
# ============================================================


def run_transform(
    json_data: list[dict[str, Any]],
    LOG: logging.Logger,
) -> pl.DataFrame:
    """Transform JSON into structured DataFrame."""

    LOG.info("========================")
    LOG.info("STAGE 03: TRANSFORM starting...")
    LOG.info("========================")

    records: list[dict[str, Any]] = []

    # ============================================================
    # EXTRACT REQUIRED FIELDS
    # ============================================================

    for record in json_data:
        records.append(
            {
                "user_id": record["userId"],
                "post_id": record["id"],
                "title": record["title"],
                "body": record["body"],
            }
        )

    # Convert to DataFrame
    df: pl.DataFrame = pl.DataFrame(records)

    # ============================================================
    # CREATE DERIVED FEATURES
    # ============================================================

    df = df.with_columns(
        [
            # Length features
            pl.col("title").str.len_chars().alias("title_length"),
            pl.col("body").str.len_chars().alias("body_length"),
            # Combined length
            (pl.col("title").str.len_chars() + pl.col("body").str.len_chars()).alias(
                "total_length"
            ),
            # Word count features
            pl.col("title").str.split(" ").list.len().alias("title_word_count"),
            pl.col("body").str.split(" ").list.len().alias("body_word_count"),
        ]
    )

    # ============================================================
    # CLASSIFY POSTS BY SIZE
    # ============================================================

    df = df.with_columns(
        [
            pl.when(pl.col("body_length") < 140)
            .then(pl.lit("short"))
            .when(pl.col("body_length") < 180)
            .then(pl.lit("medium"))
            .otherwise(pl.lit("long"))
            .alias("body_size_group")
        ]
    )

    # ============================================================
    # FILTER DATA
    # ============================================================

    df = df.filter(pl.col("body_length") > 150)

    # ============================================================
    # LOG RESULTS
    # ============================================================

    LOG.info("Transformation complete.")
    LOG.info(f"Rows after filtering: {df.shape[0]}")
    LOG.info(f"Columns: {df.columns}")
    LOG.info(f"DataFrame preview:\n{df.head()}")
    LOG.info("Sink: Polars DataFrame created")

    return df
