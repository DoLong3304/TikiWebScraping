"""Simple smoke tests to verify configuration and basic pipeline wiring.

Run with:

    pytest -q

These tests assume SUPABASE_URL and SUPABASE_SERVICE_KEY are set.
"""

import os

import pytest

from src.db.supabase_client import get_supabase_client
from src.config import DEFAULT_PARENT_CATEGORY_ID
from src.tiki_client.categories import to_category_rows


def test_env_vars_present() -> None:
    assert os.getenv("SUPABASE_URL"), "SUPABASE_URL must be set for tests"
    assert os.getenv("SUPABASE_SERVICE_KEY"), "SUPABASE_SERVICE_KEY must be set for tests"


def test_supabase_connection() -> None:
    client = get_supabase_client()
    # simple query that should not fail if schema exists
    client.table("category").select("id").limit(1).execute()


def test_category_mapping_does_not_crash() -> None:
    # Use local sample JSON from sample_data instead of hitting the network
    import json
    from pathlib import Path

    sample_path = Path(__file__).resolve().parents[1] / "sample_data" / "categories.json"
    with sample_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    rows = to_category_rows(raw.get("data", []))
    assert rows, "Expected at least one mapped category row"
    # Basic shape check
    for row in rows:
        assert "id" in row and row["id"] is not None
        assert "name" in row
