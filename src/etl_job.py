"""
etl_job.py — Main ETL orchestrator.

Usage:
    python -m src.etl_job
    python -m src.etl_job --since 2025-08-21T00:00:00Z
    python -m src.etl_job --last-processed 2025-08-20T23:59:59Z

Idempotence strategy:
    Before writing curated Parquet partitions, the job deletes any existing
    partition file for the same date (overwrite-by-partition). Running the
    job twice produces the same files with the same content.
"""
import argparse
import json
import logging
import shutil
import sys
from pathlib import Path

import pandas as pd

# Ensure src/ is importable when running as `python -m src.etl_job`
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.api_client import fetch_orders
from src.db import load_users, load_products
from src.transforms import (
    validate_orders,
    deduplicate_orders,
    build_fact_orders,
    build_dim_user,
    build_dim_product,
    filter_since,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SAMPLE_DATA = ROOT / "sample_data"
OUTPUT_ROOT  = ROOT / "output"
OUTPUT_RAW   = OUTPUT_ROOT / "raw"
OUTPUT_CURATED = OUTPUT_ROOT / "curated"


def _setup_logging() -> logging.Logger:
    """
    Create output/ directories first, then attach the FileHandler.
    This avoids FileNotFoundError on a fresh clone where output/ doesn't exist.
    """
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    OUTPUT_RAW.mkdir(parents=True, exist_ok=True)
    OUTPUT_CURATED.mkdir(parents=True, exist_ok=True)

    fmt = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    root_logger = logging.getLogger()
    if not root_logger.handlers:          # avoid duplicate handlers on re-runs
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(logging.StreamHandler(sys.stdout))
        root_logger.addHandler(
            logging.FileHandler(OUTPUT_ROOT / "etl.log", mode="a", encoding="utf-8")
        )
        for h in root_logger.handlers:
            h.setFormatter(logging.Formatter(fmt))

    return logging.getLogger("etl_job")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def write_raw(orders: list, since: str | None, logger) -> Path:
    """Persist the raw JSON response as-is (landing zone)."""
    tag = since.replace(":", "-").replace("Z", "") if since else "full"
    path = OUTPUT_RAW / f"orders_{tag}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(orders, f, ensure_ascii=False, indent=2)
    logger.info("Raw JSON written → %s (%d records)", path, len(orders))
    return path


def write_curated_parquet(df: pd.DataFrame, table: str, logger, partition_col: str | None = None) -> None:
    """
    Write DataFrame as Parquet, optionally partitioned by date column.

    Idempotence: each partition directory is replaced (delete + write).
    This allows Redshift COPY to load without duplicates.
    """
    if df.empty:
        logger.warning("Nothing to write for table=%s", table)
        return

    base = OUTPUT_CURATED / table

    if partition_col and partition_col in df.columns:
        for part_value, group in df.groupby(partition_col):
            part_dir = base / f"{partition_col}={part_value}"
            if part_dir.exists():
                shutil.rmtree(part_dir)
            part_dir.mkdir(parents=True, exist_ok=True)
            out_file = part_dir / "data.parquet"
            group.drop(columns=[partition_col]).to_parquet(out_file, index=False)
            logger.info("Curated Parquet → %s (%d rows)", out_file, len(group))
    else:
        base.mkdir(parents=True, exist_ok=True)
        out_file = base / "data.parquet"
        df.to_parquet(out_file, index=False)
        logger.info("Curated Parquet → %s (%d rows)", out_file, len(df))


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run(since: str | None = None) -> None:
    logger = _setup_logging()

    logger.info("=" * 60)
    logger.info("ETL job started | since=%s", since)
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # EXTRACT
    # ------------------------------------------------------------------
    raw_orders = fetch_orders(
        since=since,
        local_fallback=SAMPLE_DATA / "api_orders.json",
    )
    write_raw(raw_orders, since, logger)

    users_df    = load_users(SAMPLE_DATA / "users.csv")
    products_df = load_products(SAMPLE_DATA / "products.csv")

    # ------------------------------------------------------------------
    # TRANSFORM
    # ------------------------------------------------------------------
    valid_df, rejected_df = validate_orders(raw_orders)

    if not rejected_df.empty:
        rej_path = OUTPUT_RAW / "orders_rejected.json"
        rejected_df.to_json(rej_path, orient="records", force_ascii=False, indent=2)
        logger.warning("Rejected records written → %s", rej_path)

    if valid_df.empty:
        logger.warning("No valid orders to process. Exiting.")
        return

    deduped_df     = deduplicate_orders(valid_df)
    incremental_df = filter_since(deduped_df, since)

    fact_df        = build_fact_orders(incremental_df)
    dim_user_df    = build_dim_user(users_df)
    dim_product_df = build_dim_product(products_df)

    # ------------------------------------------------------------------
    # LOAD (curated zone — simulates S3 / Redshift staging)
    # ------------------------------------------------------------------
    write_curated_parquet(fact_df,        "fact_order",  logger, partition_col="order_date")
    write_curated_parquet(dim_user_df,    "dim_user",    logger)
    write_curated_parquet(dim_product_df, "dim_product", logger)

    # ------------------------------------------------------------------
    # Metrics summary
    # ------------------------------------------------------------------
    logger.info("-" * 60)
    logger.info("ETL completed successfully")
    logger.info("  raw orders received   : %d", len(raw_orders))
    logger.info("  valid after validation: %d", len(valid_df))
    logger.info("  after dedup           : %d", len(deduped_df))
    logger.info("  after incremental     : %d", len(incremental_df))
    logger.info("  fact_order rows       : %d", len(fact_df))
    logger.info("  dim_user rows         : %d", len(dim_user_df))
    logger.info("  dim_product rows      : %d", len(dim_product_df))
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETL job — orders pipeline")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--since",
        metavar="ISO8601",
        help="Process only orders created after this timestamp (e.g. 2025-08-21T00:00:00Z)",
    )
    group.add_argument(
        "--last-processed",
        metavar="ISO8601",
        dest="last_processed",
        help="Alias for --since (last successfully processed timestamp)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    since = args.since or args.last_processed
    run(since=since)
