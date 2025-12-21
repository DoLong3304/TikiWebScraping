import argparse
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Iterable, List, Literal, Optional, Any
from collections.abc import Callable

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
from src.tiki_client.listings import (
    fetch_all_listings_for_category,
    to_product_and_seller_rows,
)
from src.tiki_client.products import fetch_product, to_product_row, to_seller_row
import httpx
from src.tiki_client.reviews import fetch_all_reviews_for_product, to_review_rows
from src.tiki_client.sellers import fetch_seller, to_seller_row_from_widget
from src.pipeline.transform import run_full_transform, TransformResult
from src.pipeline.transform import TransformPlan, run_transform_with_plan
from src.pipeline.extract import (
    extract_categories_async,
    extract_listings_for_categories_async,
    extract_product_details_async,
    extract_reviews_for_products_async,
    extract_sellers_only_async,
)


logger = logging.getLogger("tiki_pipeline")


@dataclass
class RunPlan:
    """Declarative run plan shared by CLI and GUI.

    mode="scrape" allows creating new records from Tiki. mode="update" only
    refreshes existing records and will not insert brand new products/sellers.
    """

    categories_listings: bool = True
    products: bool = True
    reviews: bool = True
    sellers: bool = True
    mode: Literal["scrape", "update"] = "scrape"
    product_ids_override: Optional[List[int]] = None
    start_index_reviews: int = 0
    parent_category_id: int = DEFAULT_PARENT_CATEGORY_ID


@dataclass
class RunResult:
    errors: dict[str, list[str]] = field(default_factory=dict)
    failed_review_ids: list[int] = field(default_factory=list)
    failed_product_ids: list[int] = field(default_factory=list)
    product_ids_processed: list[int] = field(default_factory=list)


def _existing_product_ids(client: Any) -> List[int]:
    res = client.table("product").select("id").execute()
    return [row["id"] for row in (res.data or [])]


def _validate_plan(plan: RunPlan) -> None:
    if plan.mode not in ("scrape", "update"):
        raise ValueError("mode must be 'scrape' or 'update'")
    if not (plan.categories_listings or plan.products or plan.reviews or plan.sellers):
        raise ValueError("Select at least one stage to run")

    if plan.parent_category_id <= 0:
        raise ValueError("parent_category_id must be a positive category id")

    requires_source = plan.mode == "scrape" and (plan.products or plan.reviews or plan.sellers)
    if requires_source and not (plan.categories_listings or plan.product_ids_override):
        raise ValueError(
            "Scrape mode needs 'categories_listings' selected or explicit product_ids_override to seed new items"
        )

    if plan.start_index_reviews < 0:
        raise ValueError("start_index_reviews cannot be negative")


async def sync_categories(parent_id: int = DEFAULT_PARENT_CATEGORY_ID) -> List[int]:
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


async def sync_products_for_categories(
    category_ids: Iterable[int],
    update_only_existing: bool = False,
    existing_product_ids: Optional[set[int]] = None,
) -> List[int]:
    """Fetch listings for categories and upsert products/sellers.

    When ``update_only_existing`` is True, new products discovered in listings
    are filtered out so only already-known product IDs are updated.
    """

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


async def enrich_products_with_details(
    product_ids: Iterable[int],
    *,
    mode: Literal["scrape", "update"],
    existing_product_ids: Optional[set[int]] = None,
) -> tuple[list[int], list[int]]:
    """Enrich product details and return (failed_ids, processed_ids).

    Behaviour by mode:
    - scrape: only enrich products that *do not* exist in DB yet, using
      regular upsert (rows are expected to have valid category_id already
      from listings).
    - update: only enrich products that *do* exist in DB, and apply
      updates via a narrow SQL UPDATE so ``category_id`` is never touched.
    """

    client = get_supabase_client()
    existing_ids: set[int] = set(existing_product_ids or set())

    incoming_ids = list(dict.fromkeys(int(pid) for pid in product_ids))
    if mode == "scrape":
        # Skip already-known products entirely so we don't touch their
        # existing category_id or other fields when running a fresh scrape.
        target_ids = [pid for pid in incoming_ids if pid not in existing_ids]
    else:  # mode == "update"
        # Only update products that already exist in DB; ignore stray ids.
        target_ids = [pid for pid in incoming_ids if pid in existing_ids]

    logger.info(
        "[3/4] Enriching %d products with detail API (mode=%s)",
        len(target_ids),
        mode,
    )
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
                # Optionally enrich seller info via dedicated seller widget API
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
                # In scrape mode we may be inserting brand new rows that
                # already have category_id set from listings.
                upsert_products(client, [product_row])
            else:
                # In update mode, avoid upsert to keep category_id fully
                # controlled by the listings stage; update only other fields.
                update_product_details_sql(client, product_row)
        except Exception as exc:  # pragma: no cover - DB failure handling
            logger.warning("[3/4] Failed to persist product %s: %s", product_row.get("id"), exc)
            failed_ids.append(pid)
    logger.info("[3/4] Product detail enrichment complete")
    return failed_ids, processed


async def sync_reviews_for_products(product_ids: Iterable[int], start_index: int = 0) -> list[int]:
    """Fetch reviews and return a list of product IDs that failed."""

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
            try:
                upsert_sellers(client, seller_rows)
            except Exception as exc:  # pragma: no cover - DB failure handling
                logger.warning("[4/4] Failed to upsert review sellers for product %s: %s", pid, exc)
        if review_rows:
            # Deduplicate reviews by id within this batch to satisfy ON CONFLICT
            unique_by_id = {}
            for r in review_rows:
                rid = r.get("id")
                if rid is None:
                    continue
                unique_by_id[rid] = r
            deduped_reviews = list(unique_by_id.values())
            try:
                upsert_reviews(client, deduped_reviews)
            except Exception as exc:  # pragma: no cover - DB failure handling
                logger.warning("[4/4] Failed to upsert reviews for product %s: %s", pid, exc)
        logger.info("[4/4] Product %s: stored %d reviews", pid, len(review_rows))
    if failed_ids:
        logger.warning("[4/4] Review sync complete with %d failures. Problem product_ids: %s", len(failed_ids), failed_ids)
    else:
        logger.info("[4/4] Review sync complete with no failures")
    return failed_ids


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
                try:
                    upsert_sellers(client, [widget_row])
                except Exception as exc:  # pragma: no cover - DB failure handling
                    logger.warning("[S] Failed to upsert seller %s from widget: %s", sid, exc)
        except Exception as exc:  # pragma: no cover - best-effort refresh
            logger.warning("[S] Failed to refresh seller %s: %s", sid, exc)
    logger.info("[S] Sellers-only refresh complete")


def _log_error_summary(error_summary: dict[str, list[str]]) -> None:
    """Log a compact overview of issues encountered during the run.

    This does not affect control flow; it only provides a quick
    at-a-glance summary at the end so you can see which stages had
    problems without scrolling through the full log.
    """
    any_errors = any(error_summary.values())
    if not any_errors:
        logger.info("[SUMMARY] Pipeline completed with no recorded errors in tracked stages.")
        return

    logger.warning("[SUMMARY] Pipeline completed with issues. Per-stage overview below:")
    for stage, errors in error_summary.items():
        if not errors:
            logger.info("[SUMMARY] %s: OK", stage)
            continue
        logger.warning("[SUMMARY] %s: %d issue(s)", stage, len(errors))
        # Log only the first few messages per stage to avoid clutter.
        for idx, msg in enumerate(errors[:5], start=1):
            logger.warning("[SUMMARY]   (%d) %s", idx, msg)


async def extract_by_plan(
    plan: RunPlan,
    should_stop: Optional[Callable[[], bool]] = None,
) -> tuple[List[int], set[int], dict[str, list[str]], list[int], list[int]]:
    client = get_supabase_client()
    errors: dict[str, list[str]] = {
        "categories": [],
        "listings": [],
        "products_enrich": [],
        "reviews": [],
        "sellers": [],
    }

    existing_product_ids: set[int] = set()
    product_ids: List[int] = []
    failed_products: list[int] = []
    processed_products: list[int] = []
    failed_reviews: list[int] = []

    def _stopped() -> bool:
        return bool(should_stop and should_stop())

    if _stopped():
        logger.info("Stop requested before extract stages began")
        return product_ids, existing_product_ids, errors, failed_products, failed_reviews

    if plan.categories_listings:
        try:
            leaf_category_ids = await extract_categories_async(plan.parent_category_id)
        except Exception as exc:
            errors["categories"].append(str(exc))
            leaf_category_ids = []
        if not leaf_category_ids:
            errors["categories"].append("No leaf categories returned from categories stage")

        if _stopped():
            logger.info("Stop requested during categories stage")
            return product_ids, existing_product_ids, errors, failed_products, failed_reviews

        if plan.mode == "update":
            existing_product_ids = {pid for pid in _existing_product_ids(client)}
        try:
            product_ids = await extract_listings_for_categories_async(
                leaf_category_ids,
                update_only_existing=(plan.mode == "update"),
                existing_product_ids=existing_product_ids,
            )
        except Exception as exc:
            errors["listings"].append(str(exc))
            product_ids = []
        if plan.mode == "update" and not product_ids:
            errors["listings"].append("No products updated from listings in update mode")
        if _stopped():
            logger.info("Stop requested during listings stage")
            return product_ids, existing_product_ids, errors, failed_products, failed_reviews
    else:
        if plan.product_ids_override:
            product_ids = list({int(pid) for pid in plan.product_ids_override})
        else:
            product_ids = list(_existing_product_ids(client))

    if not existing_product_ids:
        existing_product_ids = {pid for pid in _existing_product_ids(client)}

    if plan.products:
        try:
            failed_products, processed_products = await extract_product_details_async(
                product_ids,
                mode=plan.mode,
                existing_product_ids=existing_product_ids,
            )
        except Exception as exc:
            errors["products_enrich"].append(str(exc))
        if _stopped():
            logger.info("Stop requested during product enrichment stage")
            return product_ids, existing_product_ids, errors, failed_products, failed_reviews
    else:
        logger.info("Skipping product enrichment by request")

    if plan.reviews:
        try:
            failed_reviews, _ = await extract_reviews_for_products_async(product_ids, start_index=plan.start_index_reviews)
        except Exception as exc:
            errors["reviews"].append(str(exc))
        if _stopped():
            logger.info("Stop requested during reviews stage")
            return product_ids, existing_product_ids, errors, failed_products, failed_reviews
    else:
        logger.info("Skipping review crawl by request")

    if plan.sellers:
        try:
            await extract_sellers_only_async()
        except Exception as exc:
            errors["sellers"].append(str(exc))
        if _stopped():
            logger.info("Stop requested during sellers stage")
    else:
        logger.info("Skipping seller refresh by request")

    return product_ids, existing_product_ids, errors, failed_products, failed_reviews


async def execute_plan(
    plan: RunPlan,
    *,
    should_stop: Optional[Callable[[], bool]] = None,
) -> RunResult:
    _validate_plan(plan)
    product_ids, existing_product_ids, errors, failed_products, failed_reviews = await extract_by_plan(
        plan,
        should_stop=should_stop,
    )

    _log_error_summary(errors)
    return RunResult(
        errors=errors,
        failed_review_ids=failed_reviews,
        failed_product_ids=failed_products,
        product_ids_processed=list({*product_ids}),
    )


TRANSFORM_STAGE_ALIASES = {
    "dim_category": "dim_category",
    "category": "dim_category",
    "dim_seller": "dim_seller",
    "seller": "dim_seller",
    "dim_product": "dim_product",
    "product": "dim_product",
    "product_ingredients": "product_ingredients",
    "ingredients": "product_ingredients",
    "fact_product_daily": "fact_product_daily",
    "product_daily": "fact_product_daily",
    "fact_seller_daily": "fact_seller_daily",
    "seller_daily": "fact_seller_daily",
    "review_clean": "review_clean",
    "reviews": "review_clean",
    "review_daily": "review_daily",
    "review_summary": "review_summary",
}


def _transform_plan_from_aliases(aliases: list[str]) -> TransformPlan:
    if not aliases:
        return TransformPlan()
    normalized = {TRANSFORM_STAGE_ALIASES.get(item, item) for item in aliases}
    plan = TransformPlan(
        dim_category="dim_category" in normalized,
        dim_seller="dim_seller" in normalized,
        dim_product="dim_product" in normalized,
        product_ingredients="product_ingredients" in normalized,
        fact_product_daily="fact_product_daily" in normalized,
        fact_seller_daily="fact_seller_daily" in normalized,
        review_clean="review_clean" in normalized,
        review_daily="review_daily" in normalized,
        review_summary="review_summary" in normalized,
    )
    return plan


async def run_transform_only(plan: Optional[TransformPlan] = None) -> TransformResult:
    """Run the requested transform stages (default: all)."""

    client = get_supabase_client()
    if plan is None:
        return run_full_transform(client)
    return run_transform_with_plan(plan, client)


def _parse_args() -> argparse.Namespace:
    valid_aliases = [
        "categories_listings",
        "categories",
        "listings",
        "products",
        "product",
        "reviews",
        "review",
        "sellers",
        "seller",
    ]
    parser = argparse.ArgumentParser(description="Run Tiki pipeline stages")
    parser.add_argument(
        "--data",
        nargs="+",
        choices=valid_aliases,
        default=["categories_listings", "products", "reviews", "sellers"],
        help="Stages to run in order",
    )
    parser.add_argument("--mode", choices=["scrape", "update"], default="scrape", help="Scrape new data or update only existing records")
    parser.add_argument("--product-ids", type=str, help="Comma-separated product IDs to focus (overrides discovery)")
    parser.add_argument("--start-index", type=int, default=0, help="Start index when resuming reviews")
    parser.add_argument("--parent-category", type=int, default=DEFAULT_PARENT_CATEGORY_ID, help="Root category id to crawl")
    parser.add_argument(
        "--run-transform",
        action="store_true",
        help="Run the cleaned-schema transform after the selected extract stages",
    )
    parser.add_argument(
        "--transform-stages",
        nargs="+",
        choices=sorted(TRANSFORM_STAGE_ALIASES.keys()),
        help="If set, only run the specified transform stages (default is all)",
    )
    return parser.parse_args()


def _plan_from_args(args: argparse.Namespace) -> RunPlan:
    alias_map = {
        "categories": "categories_listings",
        "listings": "categories_listings",
        "product": "products",
        "review": "reviews",
        "seller": "sellers",
    }
    data = {alias_map.get(item, item) for item in args.data}
    override_ids: Optional[List[int]] = None
    if args.product_ids:
        try:
            override_ids = [int(x.strip()) for x in args.product_ids.split(",") if x.strip()]
        except ValueError as exc:
            raise ValueError("product-ids must be integers separated by commas") from exc

    return RunPlan(
        categories_listings="categories_listings" in data,
        products="products" in data,
        reviews="reviews" in data,
        sellers="sellers" in data,
        mode=args.mode,
        product_ids_override=override_ids,
        start_index_reviews=args.start_index,
        parent_category_id=args.parent_category,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = _parse_args()
    plan = _plan_from_args(args)
    result = asyncio.run(execute_plan(plan))
    issues = sum(len(v) for v in result.errors.values())
    if issues:
        logger.warning("Run completed with %d recorded issue(s)", issues)
    if result.failed_review_ids:
        logger.warning("Review failures: %s", result.failed_review_ids)
    if result.failed_product_ids:
        logger.warning("Product enrichment failures: %s", result.failed_product_ids)

    if getattr(args, "run_transform", False):
        logger.info("Running cleaned transform as requested...")
        transform_plan = _transform_plan_from_aliases(args.transform_stages or [])
        transform_result = asyncio.run(run_transform_only(transform_plan))
        logger.info(
            "Transform completed: dim_category=%s, dim_seller=%s, dim_product=%s, ingredients=%s, product_daily=%s, seller_daily=%s, review_clean=%s, review_daily=%s, review_summary=%s",
            transform_result.dim_category_rows,
            transform_result.dim_seller_rows,
            transform_result.dim_product_rows,
            transform_result.product_ingredient_rows,
            transform_result.fact_product_daily_rows,
            transform_result.fact_seller_daily_rows,
            transform_result.review_clean_rows,
            transform_result.review_daily_rows,
            transform_result.review_summary_rows,
        )


if __name__ == "__main__":
    main()

