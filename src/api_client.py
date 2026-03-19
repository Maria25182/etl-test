"""
api_client.py — Fetches orders from the mock API (or local file fallback).
Implements retry with exponential back-off using tenacity.
"""
import json
import logging
import os
from pathlib import Path
from typing import List, Dict, Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "10"))


@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _get_with_retry(url: str, params: dict | None = None) -> requests.Response:
    """HTTP GET with automatic retries on transient errors."""
    logger.debug("GET %s params=%s", url, params)
    response = requests.get(url, params=params, timeout=API_TIMEOUT)
    response.raise_for_status()
    return response


def fetch_orders(since: str | None = None, local_fallback: Path | None = None) -> List[Dict[str, Any]]:
    """
    Fetch orders from the API.

    Args:
        since: ISO8601 timestamp — only return orders created after this date.
        local_fallback: Path to a JSON file used when the API is unavailable
                        (useful for local dev / testing).

    Returns:
        List of raw order dicts.
    """
    if local_fallback and local_fallback.exists():
        logger.info("Using local fallback file: %s", local_fallback)
        with open(local_fallback, "r", encoding="utf-8") as f:
            orders = json.load(f)
        if since:
            orders = [o for o in orders if (o.get("created_at") or "") >= since]
        logger.info("Loaded %d orders from local file (since=%s)", len(orders), since)
        return orders

    params = {}
    if since:
        params["since"] = since

    try:
        url = f"{API_BASE_URL}/api/v1/orders"
        response = _get_with_retry(url, params=params)
        orders = response.json()
        logger.info("Fetched %d orders from API (since=%s)", len(orders), since)
        return orders
    except Exception as exc:
        logger.error("Failed to fetch orders from API: %s", exc)
        raise
