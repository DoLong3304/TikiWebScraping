import os
from typing import Any, Dict, List

from supabase import Client, create_client

from src.config import SUPABASE_SERVICE_KEY, SUPABASE_URL

_client: Client | None = None


def get_supabase_client(force_refresh: bool = False) -> Client:
    """Return a cached Supabase client instance.

    The client is created lazily and reused across calls to avoid repeatedly
    instantiating HTTP pools. ``force_refresh=True`` rebuilds the client using
    the latest environment variables.
    """

    global _client
    if _client is not None and not force_refresh:
        return _client

    url = SUPABASE_URL or os.getenv("SUPABASE_URL")
    key = SUPABASE_SERVICE_KEY or os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")

    _client = create_client(url, key)
    return _client


def upsert_categories(client: Client, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    client.table("category").upsert(rows).execute()


def upsert_products(client: Client, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    client.table("product").upsert(rows).execute()


def upsert_sellers(client: Client, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    client.table("seller").upsert(rows).execute()


def upsert_reviews(client: Client, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    client.table("review").upsert(rows).execute()
