from typing import Any, Dict

import httpx

from src.config import TIKI_PRODUCT_URL


async def fetch_product(product_id: int) -> Dict[str, Any]:
    url = f"{TIKI_PRODUCT_URL}/{product_id}"
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()


def to_product_row(data: Dict[str, Any]) -> Dict[str, Any]:
    brand = data.get("brand") or {}
    current_seller = data.get("current_seller") or {}

    # IMPORTANT: Do NOT include "category_id" here.
    # Products are first inserted from listing pages via
    # ``to_product_and_seller_rows(..., category_id)`` where the
    # category id is guaranteed to exist in the ``category`` table.
    #
    # The detail API may:
    #   * return a different category than the listing, or
    #   * omit category information altogether.
    #
    # If we were to upsert a row that contains "category_id": None,
    # Supabase/Postgres would try to overwrite the existing non-null
    # category_id with NULL and violate the NOT NULL + FK constraint.
    #
    # By never sending "category_id" in this payload, the upsert
    # leaves the already-stored value untouched.

    return {
        "id": data.get("id"),
        "master_id": data.get("master_id"),
        "sku": data.get("sku"),
        "name": data.get("name"),
        "brand": brand.get("name"),
        "brand_id": brand.get("id"),
        "price": data.get("price"),
        "list_price": data.get("list_price"),
        "original_price": data.get("original_price"),
        "discount": data.get("discount"),
        "discount_rate": data.get("discount_rate"),
        "rating_average": data.get("rating_average"),
        "review_count": data.get("review_count"),
        "all_time_quantity_sold": data.get("all_time_quantity_sold"),
        "thumbnail_url": data.get("thumbnail_url"),
        "tiki_url": data.get("short_url") or data.get("url_path"),
        "seller_id": current_seller.get("id"),
        "specifications": data.get("specifications"),
        "badges": data.get("badges"),
        "badges_new": data.get("badges_new"),
        "badges_v3": data.get("badges_v3"),
        "highlight": data.get("highlight"),
        "extra": {
            "deal_specs": data.get("deal_specs"),
            "benefits": data.get("benefits"),
            "return_policy": data.get("return_policy"),
        },
    }


def to_seller_row(data: Dict[str, Any]) -> Dict[str, Any] | None:
    current_seller = data.get("current_seller") or {}
    seller_id = current_seller.get("id")
    if not seller_id:
        return None
    return {
        "id": seller_id,
        "name": current_seller.get("name") or "",
        "seller_type": None,
        "is_official": None,
        "rating": None,
    }
