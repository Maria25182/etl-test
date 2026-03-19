"""
transforms.py — Pure transformation functions (no I/O).
All functions receive and return DataFrames / plain Python objects,
making them trivially unit-testable.
"""
import json
import logging
from typing import List, Dict, Any

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Validation & cleansing
# ---------------------------------------------------------------------------

def validate_orders(raw_orders: List[Dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split raw order list into valid and rejected records.

    Rules:
    - order_id must be present and non-null.
    - created_at must be present and parseable (null → rejected).
    - items must be a non-empty list (empty list → rejected).
    - items[*].price null → replaced with 0.0 and flagged in a warning.

    Returns:
        (valid_df, rejected_df)
    """
    valid, rejected = [], []

    for record in raw_orders:
        order_id = record.get("order_id")
        created_at = record.get("created_at")
        items = record.get("items") or []

        if not order_id:
            logger.warning("Rejecting record without order_id: %s", record)
            record["_reject_reason"] = "missing_order_id"
            rejected.append(record)
            continue

        if not created_at:
            logger.warning("Rejecting order %s: null created_at", order_id)
            record["_reject_reason"] = "null_created_at"
            rejected.append(record)
            continue

        if not items:
            logger.warning("Rejecting order %s: empty items list", order_id)
            record["_reject_reason"] = "empty_items"
            rejected.append(record)
            continue

        # Normalise null prices inside items
        for item in items:
            if item.get("price") is None:
                logger.warning("order %s sku %s: null price → 0.0", order_id, item.get("sku"))
                item["price"] = 0.0

        valid.append(record)

    valid_df = pd.DataFrame(valid) if valid else pd.DataFrame()
    rejected_df = pd.DataFrame(rejected) if rejected else pd.DataFrame()
    logger.info("Validation: %d valid, %d rejected", len(valid_df), len(rejected_df))
    return valid_df, rejected_df


# ---------------------------------------------------------------------------
# 2. Deduplication
# ---------------------------------------------------------------------------

def deduplicate_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate order_ids, keeping the last occurrence.
    This is idempotent: running twice on the same data produces the same result.
    """
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"], keep="last").reset_index(drop=True)
    logger.info("Dedup: %d → %d rows (removed %d duplicates)", before, len(df), before - len(df))
    return df


# ---------------------------------------------------------------------------
# 3. Fact table builder
# ---------------------------------------------------------------------------

def build_fact_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build fact_order rows from the validated+deduped orders DataFrame.

    Columns produced:
        order_id, user_id, amount, currency, created_at,
        order_date (DATE partition key),
        total_items_qty, total_items_value,
        metadata_source, metadata_promo,
        _ingested_at
    """
    if df.empty:
        return pd.DataFrame()

    out = df[["order_id", "user_id", "amount", "currency", "created_at"]].copy()
    out["created_at"] = pd.to_datetime(out["created_at"], utc=True)
    out["order_date"] = out["created_at"].dt.date.astype(str)

    # Aggregate items
    def _items_qty(items):
        return sum(i.get("qty", 0) for i in (items or []))

    def _items_value(items):
        return sum((i.get("qty", 0) * (i.get("price") or 0.0)) for i in (items or []))

    out["total_items_qty"] = df["items"].apply(_items_qty)
    out["total_items_value"] = df["items"].apply(_items_value).round(2)

    # Flatten metadata
    def _meta(field):
        def _get(m):
            if isinstance(m, dict):
                return m.get(field)
            return None
        return _get

    out["metadata_source"] = df["metadata"].apply(_meta("source"))
    out["metadata_promo"] = df["metadata"].apply(_meta("promo"))

    out["_ingested_at"] = pd.Timestamp.utcnow().isoformat()

    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 4. Dimension builders
# ---------------------------------------------------------------------------

def build_dim_user(users_df: pd.DataFrame) -> pd.DataFrame:
    """Normalise and return dim_user."""
    df = users_df.copy()
    df.columns = [c.lower() for c in df.columns]
    df["created_at"] = pd.to_datetime(df["created_at"])
    df = df.drop_duplicates(subset=["user_id"], keep="last")
    logger.info("dim_user: %d rows", len(df))
    return df.reset_index(drop=True)


def build_dim_product(products_df: pd.DataFrame) -> pd.DataFrame:
    """Normalise and return dim_product."""
    df = products_df.copy()
    df.columns = [c.lower() for c in df.columns]
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
    df = df.drop_duplicates(subset=["sku"], keep="last")
    logger.info("dim_product: %d rows", len(df))
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# 5. Incremental filter
# ---------------------------------------------------------------------------

def filter_since(df: pd.DataFrame, since: str | None) -> pd.DataFrame:
    """Keep only rows where created_at > since (ISO8601 string)."""
    if not since or df.empty:
        return df
    cutoff = pd.Timestamp(since, tz="UTC")
    mask = pd.to_datetime(df["created_at"], utc=True) > cutoff
    filtered = df[mask].reset_index(drop=True)
    logger.info("Incremental filter (since=%s): %d → %d rows", since, len(df), len(filtered))
    return filtered
