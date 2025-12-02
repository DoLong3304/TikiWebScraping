from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import asyncio
import random

import httpx

from src.config import (
    TIKI_REVIEW_URL,
    BASE_DELAY_SECONDS,
    JITTER_RANGE,
    MAX_REVIEW_PAGES_PER_PRODUCT,
)


async def fetch_review_page(client: httpx.AsyncClient, product_id: int, page: int) -> Dict[str, Any]:
    params = {"product_id": product_id, "page": page}
    resp = await client.get(TIKI_REVIEW_URL, params=params)
    resp.raise_for_status()
    return resp.json()


async def fetch_all_reviews_for_product(product_id: int) -> Dict[str, Any]:
    all_data: List[Dict[str, Any]] = []
    summary: Dict[str, Any] = {}
    # Reviews can be slow; allow a generous timeout per request
    async with httpx.AsyncClient(timeout=30.0) as client:
        page = 1
        while page <= MAX_REVIEW_PAGES_PER_PRODUCT:
            try:
                data = await fetch_review_page(client, product_id, page)
            except httpx.ReadTimeout:
                # Safeguard: if a page times out, stop for this product
                return {"summary": summary, "reviews": all_data}
            if page == 1:
                summary = {
                    "rating_average": data.get("rating_average"),
                    "reviews_count": data.get("reviews_count"),
                    "stars": data.get("stars"),
                }
            items = data.get("data", [])
            paging = data.get("paging") or {}
            if not items:
                break
            all_data.extend(items)
            current_page = paging.get("current_page", page)
            last_page = paging.get("last_page", page)
            if current_page >= last_page:
                break
            page += 1
            await asyncio.sleep(BASE_DELAY_SECONDS + random.uniform(0, JITTER_RANGE))
    return {"summary": summary, "reviews": all_data}


def _ts_to_datetime(ts: int | None) -> datetime | None:
    if not ts:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def to_review_rows(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    reviews_raw = data.get("reviews", [])
    reviews: List[Dict[str, Any]] = []
    sellers: Dict[int, Dict[str, Any]] = {}

    for r in reviews_raw:
        review_id = r.get("id")
        if not review_id:
            continue
        customer = r.get("created_by") or {}
        seller = r.get("seller") or {}
        timeline = r.get("timeline") or {}

        seller_id = seller.get("id")
        if seller_id and seller_id not in sellers:
            sellers[seller_id] = {
                "id": seller_id,
                "name": seller.get("name") or "",
                "seller_type": None,
                "is_official": None,
                "rating": None,
            }

        created_at_ts = r.get("created_at")
        purchased_at_ts = customer.get("purchased_at")

        created_at = _ts_to_datetime(created_at_ts)
        purchased_at = _ts_to_datetime(purchased_at_ts)

        reviews.append(
            {
                "id": review_id,
                "product_id": r.get("product_id"),
                "customer_id": r.get("customer_id"),
                "title": r.get("title"),
                "content": r.get("content"),
                "rating": r.get("rating"),
                "thank_count": r.get("thank_count"),
                "comment_count": r.get("comment_count"),
                "created_at": created_at.isoformat() if created_at else None,
                "purchased": bool(customer.get("purchased")),
                "purchased_at": purchased_at.isoformat() if purchased_at else None,
                "attributes": r.get("attributes"),
                "suggestions": r.get("suggestions"),
                "seller_id": r.get("seller_id"),
                "extra": {
                    "product_attributes": r.get("product_attributes"),
                    "timeline": timeline,
                    "vote_attributes": r.get("vote_attributes"),
                    "delivery_rating": r.get("delivery_rating"),
                },
            }
        )

    return reviews, list(sellers.values())
