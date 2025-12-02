import asyncio
import logging
from typing import Iterable, List

from src.config import (
    DEFAULT_PARENT_CATEGORY_ID,
    MAX_PAGES_PER_CATEGORY,
    MAX_REVIEW_PAGES_PER_PRODUCT,
    RUN_MODE,
)
from src.db.supabase_client import (
    get_supabase_client,
    upsert_categories,
    upsert_products,
    upsert_reviews,
    upsert_sellers,
)
from src.tiki_client.categories import fetch_categories, to_category_rows
from src.tiki_client.listings import (
    fetch_all_listings_for_category,
    to_product_and_seller_rows,
)
from src.tiki_client.products import fetch_product, to_product_row, to_seller_row
import httpx
from src.tiki_client.reviews import fetch_all_reviews_for_product, to_review_rows
from src.tiki_client.sellers import fetch_seller, to_seller_row_from_widget


logger = logging.getLogger("tiki_pipeline")


async def sync_categories(parent_id: int = DEFAULT_PARENT_CATEGORY_ID) -> List[int]:
    logger.info("[1/4] Fetching categories for parent_id=%s", parent_id)
    raw = await fetch_categories(parent_id)
    rows = to_category_rows(raw)
    client = get_supabase_client()
    upsert_categories(client, rows)
    leaf_ids = [c["id"] for c in rows if c.get("is_leaf")]
    logger.info("[1/4] Stored %d categories (%d leaf)", len(rows), len(leaf_ids))
    return leaf_ids


async def sync_products_for_categories(category_ids: Iterable[int]) -> List[int]:
    client = get_supabase_client()
    all_product_ids: List[int] = []
    for idx, cid in enumerate(category_ids, start=1):
        logger.info("[2/4] Category %d: fetching listings (id=%s, up to %d pages)", idx, cid, MAX_PAGES_PER_CATEGORY)
        listings = await fetch_all_listings_for_category(cid)
        products, sellers = to_product_and_seller_rows(listings)
        logger.info(
            "[2/4] Category %d: %d listings -> %d products, %d sellers",
            idx,
            len(listings),
            len(products),
            len(sellers),
        )
        upsert_sellers(client, sellers)
        upsert_products(client, products)
        all_product_ids.extend([p["id"] for p in products if p.get("id") is not None])
    logger.info("[2/4] Finished listings for %d categories; total distinct products: %d", idx if category_ids else 0, len(set(all_product_ids)))
    return list(set(all_product_ids))


async def enrich_products_with_details(product_ids: Iterable[int]) -> None:
    client = get_supabase_client()
    product_ids_list = list(product_ids)
    logger.info("[3/4] Enriching %d products with detail API", len(product_ids_list))
    seen_seller_ids: set[int] = set()
    for idx, pid in enumerate(product_ids_list, start=1):
        logger.info("[3/4] (%d/%d) Fetching product details for id=%s", idx, len(product_ids_list), pid)
        data = await fetch_product(pid)
        product_row = to_product_row(data)
        seller_row = to_seller_row(data)
        if seller_row:
            upsert_sellers(client, [seller_row])
            sid = seller_row.get("id")
            if sid and sid not in seen_seller_ids:
                # Optionally enrich seller info via dedicated seller widget API
                try:
                    seller_widget = await fetch_seller(sid)
                    widget_row = to_seller_row_from_widget(seller_widget)
                    if widget_row:
                        upsert_sellers(client, [widget_row])
                        seen_seller_ids.add(sid)
                        logger.info("[3/4] Enriched seller %s from widget API", sid)
                except Exception as exc:  # pragma: no cover - best-effort enrichment
                    logger.warning("[3/4] Failed to enrich seller %s: %s", sid, exc)
        upsert_products(client, [product_row])
    logger.info("[3/4] Product detail enrichment complete")


async def sync_reviews_for_products(product_ids: Iterable[int], start_index: int = 0) -> None:
    client = get_supabase_client()
    product_ids_list = list(product_ids)
    if start_index:
        product_ids_list = product_ids_list[start_index:]
    logger.info("[4/4] Fetching reviews for %d products (up to %d pages each)", len(product_ids_list), MAX_REVIEW_PAGES_PER_PRODUCT)
    failed_ids: list[int] = []
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
            upsert_sellers(client, seller_rows)
        if review_rows:
            # Deduplicate reviews by id within this batch to satisfy ON CONFLICT
            unique_by_id = {}
            for r in review_rows:
                rid = r.get("id")
                if rid is None:
                    continue
                unique_by_id[rid] = r
            deduped_reviews = list(unique_by_id.values())
            upsert_reviews(client, deduped_reviews)
        logger.info("[4/4] Product %s: stored %d reviews", pid, len(review_rows))
    if failed_ids:
        logger.warning("[4/4] Review sync complete with %d failures. Problem product_ids: %s", len(failed_ids), failed_ids)
    else:
        logger.info("[4/4] Review sync complete with no failures")


async def sync_sellers_only() -> None:
    """Refresh seller records using existing seller IDs in the DB.

    This does not touch products or reviews; it only calls the seller widget
    API for each known seller id and upserts the enriched seller rows.
    """
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
                upsert_sellers(client, [widget_row])
        except Exception as exc:  # pragma: no cover - best-effort refresh
            logger.warning("[S] Failed to refresh seller %s: %s", sid, exc)
    logger.info("[S] Sellers-only refresh complete")


async def full_milk_run() -> None:
    logger.info("Run mode: %s", RUN_MODE)

    client = get_supabase_client()
    leaf_category_ids: List[int] = []
    product_ids: List[int] = []

    # Special sellers-only mode: refresh sellers and exit early
    if RUN_MODE == "sellers_only":
        await sync_sellers_only()
        return

    # 1. Categories
    if RUN_MODE in ("full", "products_pipeline", "listings_only"):
        leaf_category_ids = await sync_categories(DEFAULT_PARENT_CATEGORY_ID)
    elif RUN_MODE in ("enrich_only", "reviews_only"):
        res = client.table("category").select("id").eq("is_leaf", True).execute()
        leaf_category_ids = [row["id"] for row in (res.data or [])]
        logger.info("[1/4] Using %d existing leaf categories from DB", len(leaf_category_ids))
    elif RUN_MODE == "categories_only":
        await sync_categories(DEFAULT_PARENT_CATEGORY_ID)
        logger.info("[1/4] Categories-only run complete")
        return

    # 2. Listings / products
    if RUN_MODE in ("full", "products_pipeline", "listings_only"):
        product_ids = await sync_products_for_categories(leaf_category_ids)
    elif RUN_MODE in ("enrich_only", "reviews_only"):
        res = client.table("product").select("id").execute()
        product_ids = [row["id"] for row in (res.data or [])]
        logger.info("[2/4] Using %d existing products from DB", len(product_ids))

    if RUN_MODE == "listings_only":
        logger.info("[2/4] Listings-only run complete (no enrichment/reviews)")
        return

    # 3. Enrich products
    if RUN_MODE in ("full", "products_pipeline", "enrich_only"):
        await enrich_products_with_details(product_ids)
    else:
        logger.info("[3/4] Skipping product enrichment due to run mode")

    if RUN_MODE == "enrich_only":
        logger.info("[3/4] Enrich-only run complete (no reviews)")
        return

    # 4. Reviews
    if RUN_MODE in ("full", "reviews_only"):
        # Resume control: change start_index if you want to skip
        # some products that have already been processed.
        await sync_reviews_for_products(product_ids, start_index=89)
    else:
        logger.info("[4/4] Skipping review crawl due to run mode")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    asyncio.run(full_milk_run())


if __name__ == "__main__":
    main()
