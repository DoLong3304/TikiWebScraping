"""Extraction helpers for moving Tiki data into the `public` schema."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Literal

import httpx

from src.config import DEFAULT_PARENT_CATEGORY_ID, MAX_PAGES_PER_CATEGORY, MAX_REVIEW_PAGES_PER_PRODUCT
from src.db.supabase_client import (
    get_supabase_client,
    upsert_categories,
    upsert_products,
    upsert_reviews,
    upsert_sellers,
    update_product_details_sql,
)
from src.tiki_client.categories import fetch_categories, to_category_rows
from src.tiki_client.listings import fetch_all_listings_for_category, to_product_and_seller_rows
from src.tiki_client.products import fetch_product, to_product_row, to_seller_row
from src.tiki_client.reviews import fetch_all_reviews_for_product, to_review_rows
from src.tiki_client.sellers import fetch_seller, to_seller_row_from_widget

logger = logging.getLogger("tiki_extract")


@dataclass
class ExtractResult:
    categories: int = 0
    products: int = 0
    sellers: int = 0
    reviews: int = 0


def _existing_product_ids(client: Any) -> set[int]:
    res = client.table("product").select("id").execute()
    return {row["id"] for row in (res.data or [])}


async def extract_categories_async(parent_id: int = DEFAULT_PARENT_CATEGORY_ID) -> List[int]:
    logger.info("[1/4] Fetching categories for parent_id=%s", parent_id)
    try:
        raw = await fetch_categories(parent_id)
    except Exception as exc:  # pragma: no cover - network failure handling
        logger.error("[1/4] Failed to fetch categories for parent_id=%s: %s", parent_id, exc)
        return []

    rows = to_category_rows(raw)
    client = get_supabase_client()
    try:
        upsert_categories(client, rows)
    except Exception as exc:  # pragma: no cover - DB failure handling
        logger.error("[1/4] Failed to upsert categories: %s", exc)
        return []

    leaf_ids = [c["id"] for c in rows if c.get("is_leaf")]
    logger.info("[1/4] Stored %d categories (%d leaf)", len(rows), len(leaf_ids))
    return leaf_ids


async def extract_listings_for_categories_async(
    category_ids: Iterable[int],
    update_only_existing: bool = False,
    existing_product_ids: Optional[set[int]] = None,
) -> List[int]:
    client = get_supabase_client()
    all_product_ids: List[int] = []
    existing_product_ids = existing_product_ids or set()

    category_count = 0
    for idx, cid in enumerate(category_ids, start=1):
        category_count = idx
        logger.info("[2/4] Category %d: fetching listings (id=%s, up to %d pages)", idx, cid, MAX_PAGES_PER_CATEGORY)
        try:
            listings = await fetch_all_listings_for_category(cid)
        except Exception as exc:  # pragma: no cover - network failure handling
            logger.error("[2/4] Category %d (id=%s): failed to fetch listings: %s", idx, cid, exc)
            continue

        products, sellers = to_product_and_seller_rows(listings, cid)
        if update_only_existing:
            products = [p for p in products if p.get("id") in existing_product_ids]
            sellers = [s for s in sellers if s.get("id")]

        logger.info(
            "[2/4] Category %d: %d listings -> %d products, %d sellers",
            idx,
            len(listings),
            len(products),
            len(sellers),
        )
        try:
            if sellers:
                upsert_sellers(client, sellers)
            if products:
                upsert_products(client, products)
        except Exception as exc:  # pragma: no cover - DB failure handling
            logger.error("[2/4] Category %d (id=%s): failed to upsert products/sellers: %s", idx, cid, exc)
            continue
        all_product_ids.extend([p["id"] for p in products if p.get("id") is not None])

    logger.info("[2/4] Finished listings for %d categories; total distinct products: %d", category_count, len(set(all_product_ids)))
    return list(set(all_product_ids))


async def extract_product_details_async(
    product_ids: Iterable[int],
    *,
    mode: Literal["scrape", "update"],
    existing_product_ids: Optional[set[int]] = None,
) -> tuple[list[int], list[int]]:
    client = get_supabase_client()
    existing_ids: set[int] = set(existing_product_ids or set())

    incoming_ids = list(dict.fromkeys(int(pid) for pid in product_ids))
    if mode == "scrape":
        target_ids = [pid for pid in incoming_ids if pid not in existing_ids]
    else:
        target_ids = [pid for pid in incoming_ids if pid in existing_ids]

    logger.info("[3/4] Enriching %d products with detail API (mode=%s)", len(target_ids), mode)
    seen_seller_ids: set[int] = set()
    failed_ids: list[int] = []
    processed: list[int] = []

    for idx, pid in enumerate(target_ids, start=1):
        logger.info("[3/4] (%d/%d) Fetching product details for id=%s", idx, len(target_ids), pid)
        try:
            data = await fetch_product(pid)
        except httpx.ConnectTimeout:
            logger.warning("[3/4] Timeout fetching product %s; skipping", pid)
            failed_ids.append(pid)
            continue
        except Exception as exc:  # pragma: no cover - network failure handling
            logger.warning("[3/4] Error fetching product %s: %s", pid, exc)
            failed_ids.append(pid)
            continue

        product_row = to_product_row(data)
        processed.append(pid)
        seller_row = to_seller_row(data)
        if seller_row:
            try:
                upsert_sellers(client, [seller_row])
            except Exception as exc:  # pragma: no cover - DB failure handling
                logger.warning("[3/4] Failed to upsert base seller %s: %s", seller_row.get("id"), exc)
            sid = seller_row.get("id")
            if sid and sid not in seen_seller_ids:
                try:
                    seller_widget = await fetch_seller(sid)
                    widget_row = to_seller_row_from_widget(seller_widget)
                    if widget_row:
                        try:
                            upsert_sellers(client, [widget_row])
                            seen_seller_ids.add(sid)
                            logger.info("[3/4] Enriched seller %s from widget API", sid)
                        except Exception as exc:  # pragma: no cover - DB failure handling
                            logger.warning("[3/4] Failed to upsert enriched seller %s: %s", sid, exc)
                except Exception as exc:  # pragma: no cover - best-effort enrichment
                    logger.warning("[3/4] Failed to enrich seller %s: %s", sid, exc)

        try:
            if mode == "scrape":
                upsert_products(client, [product_row])
            else:
                update_product_details_sql(client, product_row)
        except Exception as exc:  # pragma: no cover - DB failure handling
            logger.warning("[3/4] Failed to persist product %s: %s", product_row.get("id"), exc)
            failed_ids.append(pid)
    logger.info("[3/4] Product detail enrichment complete")
    return failed_ids, processed


async def extract_reviews_for_products_async(product_ids: Iterable[int], start_index: int = 0) -> tuple[list[int], list[int]]:
    client = get_supabase_client()
    product_ids_list = list(product_ids)
    if start_index:
        product_ids_list = product_ids_list[start_index:]
    logger.info("[4/4] Fetching reviews for %d products (up to %d pages each)", len(product_ids_list), MAX_REVIEW_PAGES_PER_PRODUCT)
    failed_ids: list[int] = []
    processed_ids: list[int] = []

    for idx, pid in enumerate(product_ids_list, start=1):
        logger.info("[4/4] (%d/%d) Fetching reviews for product id=%s", idx, len(product_ids_list), pid)
        try:
            data = await fetch_all_reviews_for_product(pid)
        except httpx.ReadTimeout:
            logger.warning("[4/4] Timeout fetching reviews for product %s; skipping", pid)
            failed_ids.append(pid)
            continue
        except Exception as exc:
            logger.warning("[4/4] Error fetching reviews for product %s: %s", pid, exc)
            failed_ids.append(pid)
            continue
        review_rows, seller_rows = to_review_rows(data)
        if seller_rows:
            try:
                upsert_sellers(client, seller_rows)
            except Exception as exc:  # pragma: no cover - DB failure handling
                logger.warning("[4/4] Failed to upsert review sellers for product %s: %s", pid, exc)
        if review_rows:
            unique_by_id: dict[int, dict[str, Any]] = {}
            for r in review_rows:
                rid = r.get("id")
                if rid is None:
                    continue
                unique_by_id[rid] = r
            deduped_reviews = list(unique_by_id.values())
            try:
                upsert_reviews(client, deduped_reviews)
                processed_ids.append(pid)
            except Exception as exc:  # pragma: no cover - DB failure handling
                logger.warning("[4/4] Failed to upsert reviews for product %s: %s", pid, exc)
    if failed_ids:
        logger.warning("[4/4] Review sync complete with %d failures. Problem product_ids: %s", len(failed_ids), failed_ids)
    else:
        logger.info("[4/4] Review sync complete with no failures")
    return failed_ids, processed_ids


async def extract_sellers_only_async() -> None:
    client = get_supabase_client()
    res = client.table("seller").select("id").execute()
    seller_ids = [row["id"] for row in (res.data or [])]
    logger.info("[S] Sellers-only mode: refreshing %d sellers", len(seller_ids))

    for idx, sid in enumerate(seller_ids, start=1):
        try:
            logger.info("[S] (%d/%d) Fetching seller widget for id=%s", idx, len(seller_ids), sid)
            seller_widget = await fetch_seller(sid)
            widget_row = to_seller_row_from_widget(seller_widget)
            if widget_row:
                try:
                    upsert_sellers(client, [widget_row])
                except Exception as exc:  # pragma: no cover - DB failure handling
                    logger.warning("[S] Failed to upsert seller %s from widget: %s", sid, exc)
        except Exception as exc:  # pragma: no cover - best-effort refresh
            logger.warning("[S] Failed to refresh seller %s: %s", sid, exc)
    logger.info("[S] Sellers-only refresh complete")


async def extract_all_async(parent_id: int = DEFAULT_PARENT_CATEGORY_ID, mode: Literal["scrape", "update"] = "scrape") -> ExtractResult:
    client = get_supabase_client()
    existing_ids = _existing_product_ids(client)

    result = ExtractResult()

    leaf_ids = await extract_categories_async(parent_id)
    result.categories = len(leaf_ids)

    product_ids = await extract_listings_for_categories_async(
        leaf_ids,
        update_only_existing=(mode == "update"),
        existing_product_ids=existing_ids,
    )
    result.products = len(product_ids)

    failed_products, processed_products = await extract_product_details_async(
        product_ids,
        mode=mode,
        existing_product_ids=existing_ids,
    )
    result.sellers = len(processed_products)

    failed_reviews, processed_reviews = await extract_reviews_for_products_async(product_ids)
    result.reviews = len(processed_reviews)

    if mode == "scrape":
        await extract_sellers_only_async()

    return result


def extract_all(parent_id: int = DEFAULT_PARENT_CATEGORY_ID, mode: Literal["scrape", "update"] = "scrape") -> ExtractResult:
    return asyncio.run(extract_all_async(parent_id=parent_id, mode=mode))
