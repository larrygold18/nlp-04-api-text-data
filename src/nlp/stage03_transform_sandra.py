"""
stage03_transform_sandra.py

Source: validated JSON object
Sink: Polars DataFrame

Purpose

  Transform validated JSON data into a structured format.

Analytical Questions

- Which fields are needed from the JSON data?
- How can records be normalized into tabular form?
- What derived fields would support analysis?

Notes

- This version adds feature engineering.
- Includes filtering logic to improve dataset quality.
- Adds logging to track transformation results.
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

    df: pl.DataFrame = pl.DataFrame(records)

    # ============================================================
    # CREATE DERIVED FEATURES
    # ============================================================

    df = df.with_columns(
        [
            pl.col("title").str.len_chars().alias("title_length"),
            pl.col("body").str.len_chars().alias("body_length"),
            (pl.col("title").str.len_chars() + pl.col("body").str.len_chars()).alias(
                "total_length"
            ),
            pl.col("title").str.split(" ").list.len().alias("title_word_count"),
            pl.col("body").str.split(" ").list.len().alias("body_word_count"),
        ]
    )

    # ============================================================
    # CLASSIFY POSTS BY BODY LENGTH
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
    # CLASSIFY POSTS BY TOTAL LENGTH
    # ============================================================

    df = df.with_columns(
        [
            pl.when(pl.col("total_length") < 200)
            .then(pl.lit("brief"))
            .when(pl.col("total_length") < 260)
            .then(pl.lit("balanced"))
            .otherwise(pl.lit("detailed"))
            .alias("content_detail_group")
        ]
    )

    # ============================================================
    # FILTER DATA
    # ============================================================

    df = df.filter(pl.col("body_length") > 150)

    # ============================================================
    # SUMMARY ANALYSIS
    # ============================================================

    summary_df = df.group_by("content_detail_group").len().sort("content_detail_group")

    # ============================================================
    # LOG RESULTS
    # ============================================================

    LOG.info("Transformation complete.")
    LOG.info(f"Rows after filtering: {df.shape[0]}")
    LOG.info(f"Columns: {df.columns}")
    LOG.info(f"DataFrame preview:\n{df.head()}")
    LOG.info(f"Content detail summary:\n{summary_df}")
    LOG.info("Sink: Polars DataFrame created")

    # Return the transformed DataFrame for use in the next stage.
    return df
