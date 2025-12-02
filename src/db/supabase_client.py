import os
from typing import Any, Dict, List

from supabase import create_client, Client

from src.config import SUPABASE_URL, SUPABASE_SERVICE_KEY


def get_supabase_client() -> Client:
    url = SUPABASE_URL or os.getenv("SUPABASE_URL")
    key = SUPABASE_SERVICE_KEY or os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
    return create_client(url, key)


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
