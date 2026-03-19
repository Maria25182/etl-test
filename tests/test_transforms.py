"""
test_transforms.py — Unit tests for transform functions and API client mock.
Run with: pytest tests/ -v
"""
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from src.transforms import (
    validate_orders,
    deduplicate_orders,
    build_fact_orders,
    build_dim_user,
    build_dim_product,
    filter_since,
)
from src.api_client import fetch_orders


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

VALID_ORDER = {
    "order_id": "o_1001",
    "user_id": "u_1",
    "amount": 125.50,
    "currency": "USD",
    "created_at": "2025-08-20T15:23:10Z",
    "items": [{"sku": "p_1", "qty": 2, "price": 60.0}],
    "metadata": {"source": "api", "promo": None},
}

ORDER_NULL_CREATED_AT = {
    "order_id": "o_1005",
    "user_id": "u_4",
    "amount": 310.00,
    "currency": "USD",
    "created_at": None,
    "items": [{"sku": "p_1", "qty": 1, "price": 10.0}],
    "metadata": None,
}

ORDER_EMPTY_ITEMS = {
    "order_id": "o_1006",
    "user_id": "u_2",
    "amount": 50.00,
    "currency": "USD",
    "created_at": "2025-08-21T09:00:00Z",
    "items": [],
    "metadata": None,
}

ORDER_NULL_PRICE = {
    "order_id": "o_1007",
    "user_id": "u_3",
    "amount": 80.00,
    "currency": "USD",
    "created_at": "2025-08-21T11:30:00Z",
    "items": [{"sku": "p_2", "qty": 2, "price": None}],
    "metadata": {"source": "mobile", "promo": None},
}

DUPLICATE_ORDER = {**VALID_ORDER}  # same order_id as VALID_ORDER


# ---------------------------------------------------------------------------
# validate_orders tests
# ---------------------------------------------------------------------------

class TestValidateOrders:
    def test_valid_order_passes(self):
        valid_df, rejected_df = validate_orders([VALID_ORDER])
        assert len(valid_df) == 1
        assert len(rejected_df) == 0

    def test_null_created_at_is_rejected(self):
        valid_df, rejected_df = validate_orders([ORDER_NULL_CREATED_AT])
        assert len(valid_df) == 0
        assert len(rejected_df) == 1
        assert rejected_df.iloc[0]["_reject_reason"] == "null_created_at"

    def test_empty_items_is_rejected(self):
        valid_df, rejected_df = validate_orders([ORDER_EMPTY_ITEMS])
        assert len(valid_df) == 0
        assert len(rejected_df) == 1
        assert rejected_df.iloc[0]["_reject_reason"] == "empty_items"

    def test_null_price_is_normalised_to_zero(self):
        valid_df, rejected_df = validate_orders([ORDER_NULL_PRICE])
        assert len(valid_df) == 1
        assert len(rejected_df) == 0
        items = valid_df.iloc[0]["items"]
        assert items[0]["price"] == 0.0

    def test_mixed_batch(self):
        orders = [VALID_ORDER, ORDER_NULL_CREATED_AT, ORDER_EMPTY_ITEMS]
        valid_df, rejected_df = validate_orders(orders)
        assert len(valid_df) == 1
        assert len(rejected_df) == 2

    def test_empty_input(self):
        valid_df, rejected_df = validate_orders([])
        assert valid_df.empty
        assert rejected_df.empty


# ---------------------------------------------------------------------------
# deduplicate_orders tests
# ---------------------------------------------------------------------------

class TestDeduplicateOrders:
    def test_removes_duplicates(self):
        valid_df, _ = validate_orders([VALID_ORDER, DUPLICATE_ORDER])
        deduped = deduplicate_orders(valid_df)
        assert len(deduped) == 1

    def test_no_duplicates_unchanged(self):
        order2 = {**VALID_ORDER, "order_id": "o_9999"}
        valid_df, _ = validate_orders([VALID_ORDER, order2])
        deduped = deduplicate_orders(valid_df)
        assert len(deduped) == 2

    def test_idempotent(self):
        valid_df, _ = validate_orders([VALID_ORDER, DUPLICATE_ORDER])
        deduped_once = deduplicate_orders(valid_df)
        deduped_twice = deduplicate_orders(deduped_once)
        assert len(deduped_once) == len(deduped_twice)


# ---------------------------------------------------------------------------
# build_fact_orders tests
# ---------------------------------------------------------------------------

class TestBuildFactOrders:
    def _get_fact(self, orders=None):
        if orders is None:
            orders = [VALID_ORDER]
        valid_df, _ = validate_orders(orders)
        deduped = deduplicate_orders(valid_df)
        return build_fact_orders(deduped)

    def test_columns_present(self):
        fact = self._get_fact()
        expected = {
            "order_id", "user_id", "amount", "currency", "created_at",
            "order_date", "total_items_qty", "total_items_value",
            "metadata_source", "metadata_promo", "_ingested_at",
        }
        assert expected.issubset(set(fact.columns))

    def test_order_date_is_string(self):
        fact = self._get_fact()
        assert fact["order_date"].dtype == object  # string

    def test_items_aggregated_correctly(self):
        fact = self._get_fact()
        assert fact.iloc[0]["total_items_qty"] == 2
        assert fact.iloc[0]["total_items_value"] == pytest.approx(120.0)

    def test_null_metadata_handled(self):
        order = {**VALID_ORDER, "metadata": None}
        fact = self._get_fact([order])
        assert pd.isna(fact.iloc[0]["metadata_source"])
        assert pd.isna(fact.iloc[0]["metadata_promo"])

    def test_empty_df_returns_empty(self):
        fact = build_fact_orders(pd.DataFrame())
        assert fact.empty


# ---------------------------------------------------------------------------
# build_dim_user / build_dim_product tests
# ---------------------------------------------------------------------------

class TestDimensions:
    def test_dim_user_deduplicates(self):
        users = pd.DataFrame([
            {"user_id": "u_1", "email": "a@b.com", "created_at": "2024-01-01", "country": "US"},
            {"user_id": "u_1", "email": "a@b.com", "created_at": "2024-01-01", "country": "US"},
        ])
        dim = build_dim_user(users)
        assert len(dim) == 1

    def test_dim_product_null_price_to_zero(self):
        products = pd.DataFrame([
            {"sku": "p_x", "name": "X", "category": "Cat", "price": None},
        ])
        dim = build_dim_product(products)
        assert dim.iloc[0]["price"] == 0.0


# ---------------------------------------------------------------------------
# filter_since tests
# ---------------------------------------------------------------------------

class TestFilterSince:
    def _make_df(self):
        valid_df, _ = validate_orders([VALID_ORDER])
        return build_fact_orders(deduplicate_orders(valid_df))

    def test_since_filters_older_records(self):
        df = self._make_df()
        filtered = filter_since(df, "2025-08-21T00:00:00Z")
        assert filtered.empty  # VALID_ORDER is from 2025-08-20

    def test_since_none_returns_all(self):
        df = self._make_df()
        filtered = filter_since(df, None)
        assert len(filtered) == len(df)

    def test_since_keeps_newer_records(self):
        df = self._make_df()
        filtered = filter_since(df, "2025-08-19T00:00:00Z")
        assert len(filtered) == 1


# ---------------------------------------------------------------------------
# API client mock tests
# ---------------------------------------------------------------------------

class TestApiClient:
    def test_fetch_orders_uses_local_fallback(self, tmp_path):
        data = [VALID_ORDER]
        fallback = tmp_path / "orders.json"
        fallback.write_text(json.dumps(data))

        orders = fetch_orders(local_fallback=fallback)
        assert len(orders) == 1
        assert orders[0]["order_id"] == "o_1001"

    def test_fetch_orders_filters_by_since(self, tmp_path):
        data = [
            VALID_ORDER,  # 2025-08-20
            {**VALID_ORDER, "order_id": "o_9998", "created_at": "2025-08-22T00:00:00Z"},
        ]
        fallback = tmp_path / "orders.json"
        fallback.write_text(json.dumps(data))

        orders = fetch_orders(since="2025-08-21T00:00:00Z", local_fallback=fallback)
        assert len(orders) == 1
        assert orders[0]["order_id"] == "o_9998"

    @patch("src.api_client.requests.get")
    def test_fetch_orders_from_api(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = [VALID_ORDER]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        orders = fetch_orders()  # no local_fallback → hits "API"
        assert len(orders) == 1
        mock_get.assert_called_once()

    @patch("src.api_client.requests.get", side_effect=Exception("API down"))
    def test_fetch_orders_api_error_raises(self, mock_get):
        with pytest.raises(Exception, match="API down"):
            fetch_orders()  # no fallback, API errors → should raise
