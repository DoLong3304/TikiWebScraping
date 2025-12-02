from typing import Any, Dict, List

import httpx

from src.config import TIKI_CATEGORY_URL


async def fetch_categories(parent_id: int) -> List[Dict[str, Any]]:
    params = {"include": "children", "parent_id": parent_id}
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(TIKI_CATEGORY_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
    return data.get("data", [])


def to_category_rows(raw_categories: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for c in raw_categories:
        rows.append(
            {
                "id": c.get("id"),
                "parent_id": c.get("parent_id"),
                "name": c.get("name"),
                "level": c.get("level"),
                "url_key": c.get("url_key"),
                "url_path": c.get("url_path"),
                "status": c.get("status"),
                "include_in_menu": c.get("include_in_menu"),
                "product_count": c.get("product_count"),
                "is_leaf": c.get("is_leaf"),
                "meta_title": c.get("meta_title"),
                "meta_description": c.get("meta_description"),
                "thumbnail_url": c.get("thumbnail_url"),
            }
        )
    return rows
