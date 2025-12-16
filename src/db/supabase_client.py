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


def update_product_details_sql(client: Any, row: dict[str, Any]) -> None:
    """Update a subset of product columns using a raw SQL statement.

    This is used for detail-enrichment in update mode so that we never
    touch ``category_id`` via upsert, which could introduce NULLs or
    foreign-key issues for historic rows.
    """
    # Columns we allow detail API to overwrite. ``category_id`` is
    # intentionally excluded so it remains sourced from listings.
    columns = [
        "name",
        "brand",
        "brand_id",
        "price",
        "list_price",
        "original_price",
        "discount",
        "discount_rate",
        "rating_average",
        "review_count",
        "all_time_quantity_sold",
        "thumbnail_url",
        "tiki_url",
        "seller_id",
        "specifications",
        "badges",
        "badges_new",
        "badges_v3",
        "highlight",
        "extra",
        "master_id",
        "sku",
    ]
    product_id = row.get("id")
    if product_id is None:
        return

    payload: Dict[str, Any] = {}
    for col in columns:
        if col in row:
            payload[col] = row[col]

    if not payload:
        return

    # This issues: PATCH /product?id=eq.<id> with only the given columns.
    client.table("product").update(payload).eq("id", product_id).execute()
