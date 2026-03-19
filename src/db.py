"""
db.py — Loads dimension sources (users, products) from CSV files.
If MSSQL credentials are provided via env vars, reads from the database instead.
"""
import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Optional MSSQL connection (not required)
MSSQL_SERVER = os.getenv("MSSQL_SERVER")
MSSQL_DB = os.getenv("MSSQL_DB")
MSSQL_USER = os.getenv("MSSQL_USER")
MSSQL_PASS = os.getenv("MSSQL_PASS")


def _mssql_available() -> bool:
    return all([MSSQL_SERVER, MSSQL_DB, MSSQL_USER, MSSQL_PASS])


def load_users(csv_path: Path) -> pd.DataFrame:
    """Load users from CSV (or MSSQL if credentials are set)."""
    if _mssql_available():
        try:
            import pyodbc  # noqa: PLC0415
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={MSSQL_SERVER};DATABASE={MSSQL_DB};"
                f"UID={MSSQL_USER};PWD={MSSQL_PASS}"
            )
            conn = pyodbc.connect(conn_str)
            df = pd.read_sql("SELECT user_id, email, created_at, country FROM dbo.users", conn)
            conn.close()
            logger.info("Loaded %d users from MSSQL", len(df))
            return df
        except Exception as exc:
            logger.warning("MSSQL unavailable (%s), falling back to CSV", exc)

    logger.info("Loading users from CSV: %s", csv_path)
    df = pd.read_csv(csv_path, parse_dates=["created_at"])
    logger.info("Loaded %d users", len(df))
    return df


def load_products(csv_path: Path) -> pd.DataFrame:
    """Load products from CSV (or MSSQL if credentials are set)."""
    if _mssql_available():
        try:
            import pyodbc  # noqa: PLC0415
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={MSSQL_SERVER};DATABASE={MSSQL_DB};"
                f"UID={MSSQL_USER};PWD={MSSQL_PASS}"
            )
            conn = pyodbc.connect(conn_str)
            df = pd.read_sql("SELECT sku, name, category, price FROM dbo.products", conn)
            conn.close()
            logger.info("Loaded %d products from MSSQL", len(df))
            return df
        except Exception as exc:
            logger.warning("MSSQL unavailable (%s), falling back to CSV", exc)

    logger.info("Loading products from CSV: %s", csv_path)
    df = pd.read_csv(csv_path)
    logger.info("Loaded %d products", len(df))
    return df
