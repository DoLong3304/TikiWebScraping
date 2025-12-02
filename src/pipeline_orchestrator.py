import asyncio
from typing import List

from src.config import DEFAULT_PARENT_CATEGORY_ID
from src.db.supabase_client import get_supabase_client, upsert_categories
from src.tiki_client.categories import fetch_categories, to_category_rows


async def sync_categories(parent_id: int = DEFAULT_PARENT_CATEGORY_ID) -> None:
    raw = await fetch_categories(parent_id)
    rows = to_category_rows(raw)
    client = get_supabase_client()
    upsert_categories(client, rows)


def main() -> None:
    asyncio.run(sync_categories())


if __name__ == "__main__":
    main()
