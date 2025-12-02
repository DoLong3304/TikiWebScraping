from typing import Any, Dict

import httpx

from src.config import TIKI_SELLER_URL


async def fetch_seller(seller_id: int) -> Dict[str, Any]:
    params = {"seller_id": seller_id}
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(TIKI_SELLER_URL, params=params)
        resp.raise_for_status()
        return resp.json()


def to_seller_row_from_widget(data: Dict[str, Any]) -> Dict[str, Any] | None:
    """Map seller widget JSON to the seller table schema.

    Sample structure (abbreviated) from widget:

    {
      "data": {
        "seller": {
          "id": 1,
          "name": "Tiki Trading",
          "is_official": true,
          "avg_rating_point": 4.6751,
          "review_count": 5572127,
          "total_follower": 511775,
          "store_id": 40395,
          "store_level": "OFFICIAL_STORE",
          "days_since_joined": 3066,
          "icon": "https://...",
          "url": "https://tiki.vn/cua-hang/tiki-trading",
          "badge_img": {...},
          "info": [...]
        }
      }
    }
    """

    root = data.get("data") or {}
    seller = root.get("seller") or root

    seller_id = seller.get("id")
    if not seller_id:
        return None

    # Backwards-compatible rating field (simple numeric rating)
    rating = None
    avg_rating_point = seller.get("avg_rating_point")
    if isinstance(avg_rating_point, (int, float)):
        rating = avg_rating_point

    return {
        "id": seller_id,
        "name": seller.get("name") or "",
        "seller_type": seller.get("store_level"),
        "is_official": bool(seller.get("is_official")),
        "rating": rating,
        "avg_rating_point": avg_rating_point,
        "review_count": seller.get("review_count"),
        "total_follower": seller.get("total_follower"),
        "store_id": seller.get("store_id"),
        "store_level": seller.get("store_level"),
        "days_since_joined": seller.get("days_since_joined"),
        "icon_url": seller.get("icon"),
        "profile_url": seller.get("url"),
        "badge_img": seller.get("badge_img"),
        "info": seller.get("info"),
    }
