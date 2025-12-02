from typing import Any, Dict, List, Tuple

import asyncio
import random

import httpx

from src.config import (
    TIKI_LISTING_URL,
    BASE_DELAY_SECONDS,
    JITTER_RANGE,
    MAX_PAGES_PER_CATEGORY,
)


async def fetch_listing_page(client: httpx.AsyncClient, category_id: int, page: int) -> Dict[str, Any]:
    params = {"category": category_id, "page": page}
    resp = await client.get(TIKI_LISTING_URL, params=params)
    resp.raise_for_status()
    return resp.json()


async def fetch_all_listings_for_category(category_id: int) -> List[Dict[str, Any]]:
    listings: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=10.0) as client:
        page = 1
        while page <= MAX_PAGES_PER_CATEGORY:
            data = await fetch_listing_page(client, category_id, page)
            items = data.get("data", [])
            paging = data.get("paging") or {}
            if not items:
                break
            listings.extend(items)
            current_page = paging.get("current_page", page)
            last_page = paging.get("last_page", page)
            if current_page >= last_page:
                break
            page += 1
            await asyncio.sleep(BASE_DELAY_SECONDS + random.uniform(0, JITTER_RANGE))
    return listings


def to_product_and_seller_rows(listings: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    products: List[Dict[str, Any]] = []
    sellers: Dict[int, Dict[str, Any]] = {}

    for item in listings:
        product_id = item.get("id")
        if not product_id:
            continue
        seller_id = item.get("seller_id")
        brand_name = item.get("brand_name")
        primary_category_path = item.get("primary_category_path")

        products.append(
            {
                "id": product_id,
                "sku": item.get("sku"),
                "name": item.get("name"),
                "brand": brand_name,
                "category_id": None,  # can be refined later using primary_category_path
                "price": item.get("price"),
                "list_price": item.get("list_price"),
                "original_price": item.get("original_price"),
                "discount": item.get("discount"),
                "discount_rate": item.get("discount_rate"),
                "rating_average": item.get("rating_average"),
                "review_count": item.get("review_count"),
                "all_time_quantity_sold": (item.get("quantity_sold") or {}).get("value"),
                "thumbnail_url": item.get("thumbnail_url"),
                "seller_id": seller_id,
                "extra": {
                    "primary_category_path": primary_category_path,
                    "impression_info": item.get("impression_info"),
                    "visible_impression_info": item.get("visible_impression_info"),
                },
            }
        )

        if seller_id and seller_id not in sellers:
            seller_type = None
            visible_info = item.get("visible_impression_info") or {}
            amplitude = visible_info.get("amplitude") or {}
            if amplitude:
                seller_type = amplitude.get("seller_type")
            sellers[seller_id] = {
                "id": seller_id,
                "name": amplitude.get("brand_name") or "",
                "seller_type": seller_type,
                "is_official": amplitude.get("is_official_store") == 1,
                "rating": None,
            }

    return products, list(sellers.values())
