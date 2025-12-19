"""Utility helpers to drive the pipeline from a GUI.

This module keeps the existing CLI pipeline intact while exposing
smaller, composable operations that the GUI can orchestrate. All
functions are best-effort and defensive: failures are surfaced via
return values so the GUI can offer retries.
"""
from __future__ import annotations

import asyncio
import logging
import re
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import httpx

from src import config as cfg
from src.config import (
    BASE_DELAY_SECONDS,
    DEFAULT_PARENT_CATEGORY_ID,
    JITTER_RANGE,
    MAX_PAGES_PER_CATEGORY,
    MAX_REVIEW_PAGES_PER_PRODUCT,
)
from src.db.supabase_client import get_supabase_client
from src.pipeline.orchestrator import RunPlan, RunResult, execute_plan, run_transform_only
from src.pipeline.transform import TransformPlan
from src.pipeline.extract import extract_all
from src.tiki_client.categories import fetch_categories, to_category_rows
from src.tiki_client.listings import fetch_listing_page

logger = logging.getLogger("tiki_gui")


@dataclass
class RuntimeSettings:
    parent_category_id: int = DEFAULT_PARENT_CATEGORY_ID
    max_pages_per_category: int = MAX_PAGES_PER_CATEGORY
    max_review_pages_per_product: int = MAX_REVIEW_PAGES_PER_PRODUCT
    base_delay_seconds: float = BASE_DELAY_SECONDS
    jitter_range: float = JITTER_RANGE
    start_index_reviews: int = 0
    stats_category_limit: int = 10

    def apply_to_config(self) -> None:
        """Propagate in-memory settings to the global config module."""
        cfg.DEFAULT_PARENT_CATEGORY_ID = self.parent_category_id
        cfg.MAX_PAGES_PER_CATEGORY = self.max_pages_per_category
        cfg.MAX_REVIEW_PAGES_PER_PRODUCT = self.max_review_pages_per_product
        cfg.BASE_DELAY_SECONDS = self.base_delay_seconds
        cfg.JITTER_RANGE = self.jitter_range


class PipelineRunner:
    """Thin orchestrator used by the GUI.

    The public methods are synchronous to make UI wiring simple. Internally
    they spin an event loop with ``asyncio.run`` for the heavy-lifting.
    """

    def __init__(self, settings: Optional[RuntimeSettings] = None):
        self.settings = settings or RuntimeSettings()
        self.failed_review_ids: list[int] = []
        self.failed_product_ids: list[int] = []
        self._stop_requested = False
        self._active_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Connection checks
    # ------------------------------------------------------------------
    def test_tiki_connection(self) -> tuple[bool, str]:
        async def _probe() -> tuple[bool, str]:
            try:
                await fetch_categories(self.settings.parent_category_id)
                return True, "Tiki categories reachable"
            except Exception as exc:  # pragma: no cover - network guard
                return False, f"Tiki API check failed: {exc}"

        return asyncio.run(_probe())

    def test_supabase_connection(self) -> tuple[bool, str]:
        def _probe() -> tuple[bool, str]:
            try:
                client = get_supabase_client()
                client.table("category").select("id").limit(1).execute()
                return True, "Supabase reachable"
            except Exception as exc:  # pragma: no cover - network/creds guard
                return False, f"Supabase check failed: {exc}"

        return _probe()

    # ------------------------------------------------------------------
    # Stats helpers
    # ------------------------------------------------------------------
    def refresh_stats(self) -> dict[str, Dict[str, int]]:
        """Return a snapshot of Tiki vs Supabase counts.

        The Tiki product count is an estimate using the first listing page of
        each leaf category (limited for performance). Supabase counts are exact
        via ``count="exact"`` queries.
        """

        async def _tiki_counts() -> Dict[str, int]:
            leaf_ids: list[int] = []
            try:
                raw = await fetch_categories(self.settings.parent_category_id)
                rows = to_category_rows(raw)
                leaf_ids = [c["id"] for c in rows if c.get("is_leaf")]
            except Exception:  # pragma: no cover - network guard
                return {"categories": 0, "products_estimate": 0}

            # Limit for responsiveness; user can raise the cap in settings.
            leaf_ids = leaf_ids[: max(1, self.settings.stats_category_limit)]

            async def _one(cid: int) -> int:
                try:
                    async with httpx.AsyncClient(timeout=10.0) as client:
                        data = await fetch_listing_page(client, cid, 1)
                    paging = data.get("paging") or {}
                    return int(paging.get("total", 0)) or len(data.get("data", []))
                except Exception:  # pragma: no cover - network guard
                    return 0

            semaphore = asyncio.Semaphore(4)

            async def _with_limit(cid: int) -> int:
                async with semaphore:
                    return await _one(cid)

            totals = await asyncio.gather(*[_with_limit(cid) for cid in leaf_ids])
            return {"categories": len(leaf_ids), "products_estimate": sum(totals)}

        def _supabase_counts() -> Dict[str, int]:
            try:
                client = get_supabase_client()
            except Exception:  # pragma: no cover - config guard
                return {"categories": 0, "products": 0, "sellers": 0, "reviews": 0}

            def _count(table: str) -> int:
                try:
                    res = client.table(table).select("id", count="exact").limit(1).execute()
                    return res.count or 0
                except Exception:
                    return 0

            return {
                "categories": _count("category"),
                "products": _count("product"),
                "sellers": _count("seller"),
                "reviews": _count("review"),
            }

        tiki_counts = asyncio.run(_tiki_counts())
        supabase_counts = _supabase_counts()
        return {"tiki": tiki_counts, "supabase": supabase_counts}

    # ------------------------------------------------------------------
    # Run helpers
    # ------------------------------------------------------------------
    def run_plan(self, plan: RunPlan) -> Dict[str, List[str]]:
        """Execute a composed run plan and return an error summary."""

        self.settings.apply_to_config()
        plan.parent_category_id = self.settings.parent_category_id
        self._stop_requested = False
        result: RunResult = asyncio.run(
            execute_plan(
                plan,
                should_stop=lambda: self._stop_requested,
            )
        )
        self.failed_review_ids = result.failed_review_ids
        self.failed_product_ids = result.failed_product_ids
        return result.errors

    def run_extract(self, mode: str = "scrape") -> Dict[str, int]:
        """Convenience method to run the full extract stage synchronously."""

        self.settings.apply_to_config()
        self._stop_requested = False
        mode_val = mode if mode in {"scrape", "update"} else "scrape"
        result = extract_all(parent_id=self.settings.parent_category_id, mode=mode_val)
        return {
            "categories": result.categories,
            "products": result.products,
            "sellers": result.sellers,
            "reviews": result.reviews,
        }

    def run_transform(self, plan: Optional[TransformPlan] = None) -> Dict[str, int]:
        """Run the public -> cleaned transform synchronously."""

        self.settings.apply_to_config()
        self._stop_requested = False
        result = asyncio.run(run_transform_only(plan))
        return {
            "dim_category_rows": result.dim_category_rows,
            "dim_seller_rows": result.dim_seller_rows,
            "dim_product_rows": result.dim_product_rows,
            "product_ingredient_rows": result.product_ingredient_rows,
            "review_clean_rows": result.review_clean_rows,
            "review_daily_rows": result.review_daily_rows,
            "review_summary_rows": result.review_summary_rows,
        }

    def build_transform_plan(
        self,
        *,
        dim_category: bool,
        dim_seller: bool,
        dim_product: bool,
        product_ingredients: bool,
        review_clean: bool,
        review_daily: bool,
        review_summary: bool,
    ) -> TransformPlan:
        return TransformPlan(
            dim_category=dim_category,
            dim_seller=dim_seller,
            dim_product=dim_product,
            product_ingredients=product_ingredients,
            review_clean=review_clean,
            review_daily=review_daily,
            review_summary=review_summary,
        )

    def stop(self) -> None:
        """Signal cooperative cancellation for long-running tasks."""

        self._stop_requested = True

    # ------------------------------------------------------------------
    # Retry helpers
    # ------------------------------------------------------------------
    def retry_failed_reviews(self) -> List[int]:
        if not self.failed_review_ids:
            return []
        plan = RunPlan(
            categories_listings=False,
            products=False,
            reviews=True,
            sellers=False,
            mode="update",
            product_ids_override=self.failed_review_ids,
            parent_category_id=self.settings.parent_category_id,
        )
        self.run_plan(plan)
        return self.failed_review_ids

    # ------------------------------------------------------------------
    # SQL-ish query helper
    # ------------------------------------------------------------------
    def run_sql(self, query: str) -> Tuple[bool, str, List[Dict[str, any]]]:
        """Very small SQL subset to keep things safe.

        Only ``SELECT ... FROM <table> [LIMIT n]`` is supported. The method
        returns ``(ok, message, rows)`` so the GUI can surface the outcome.
        """
        query = query.strip().rstrip(";")
        m = re.match(r"select\s+(?P<cols>[\w\*, ]+)\s+from\s+(?P<table>[\w_]+)(?:\s+limit\s+(?P<limit>\d+))?",
                     query, flags=re.IGNORECASE)
        if not m:
            return False, "Only simple SELECT queries are supported (SELECT <cols> FROM <table> [LIMIT n])", []

        cols = m.group("cols") or "*"
        table = m.group("table")
        limit = m.group("limit")
        limit_val = int(limit) if limit else None

        try:
            client = get_supabase_client()
        except Exception as exc:  # pragma: no cover
            return False, f"Supabase client error: {exc}", []

        try:
            q = client.table(table).select(cols)
            if limit_val:
                q = q.limit(limit_val)
            res = q.execute()
            rows = res.data or []
            return True, f"Fetched {len(rows)} row(s)", rows
        except Exception as exc:  # pragma: no cover
            return False, f"Query failed: {exc}", []

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
