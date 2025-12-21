"""Transform raw data in `public` schema into cleaned, analytics-ready tables.

This module is read-only on the crawling pipeline: it only reads from the
existing `public` tables (category, product, seller, review) and writes into the
`cleaned` schema. It can be run after a scraping session or on demand.

Missing or malformed fields from JSON/JSONB columns are mapped to ``None`` so
Supabase/Postgres stores NULL in the cleaned layer. For any future NOT NULL
columns, callers can choose a placeholder value to be find-and-replace later.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional
import hashlib
import json
import re
from collections import defaultdict

from supabase import Client

from src.db.supabase_client import get_supabase_client


def _cleaned_table(client: Client, table_name: str):
    return client.schema("cleaned").table(table_name)


@dataclass
class TransformResult:
    """Simple status holder for transform runs."""

    dim_category_rows: int = 0
    dim_seller_rows: int = 0
    dim_product_rows: int = 0
    product_ingredient_rows: int = 0
    fact_product_daily_rows: int = 0
    fact_seller_daily_rows: int = 0
    review_clean_rows: int = 0
    review_daily_rows: int = 0
    review_summary_rows: int = 0


@dataclass
class TransformPlan:
    """Flags describing which transform stages to execute."""

    dim_category: bool = True
    dim_seller: bool = True
    dim_product: bool = True
    product_ingredients: bool = True
    fact_product_daily: bool = True
    fact_seller_daily: bool = True
    review_clean: bool = True
    review_daily: bool = True
    review_summary: bool = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_public_rows(client: Client, table: str, columns: str = "*") -> List[Dict[str, Any]]:
    """Fetch all rows from a public table in paginated batches."""

    rows: List[Dict[str, Any]] = []
    chunk_size = 1000
    start = 0
    while True:
        end = start + chunk_size - 1
        res = client.table(table).select(columns).range(start, end).execute()
        batch = res.data or []
        rows.extend(batch)
        if len(batch) < chunk_size:
            break
        start += chunk_size
    return rows


def _get_cleaned_rows(client: Client, table: str, columns: str = "*") -> List[Dict[str, Any]]:
    """Fetch all rows from a cleaned-table using schema-aware pagination."""

    rows: List[Dict[str, Any]] = []
    chunk_size = 1000
    start = 0
    while True:
        end = start + chunk_size - 1
        res = (
            _cleaned_table(client, table)
            .select(columns)
            .range(start, end)
            .execute()
        )
        batch = res.data or []
        rows.extend(batch)
        if len(batch) < chunk_size:
            break
        start += chunk_size
    return rows


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None
    return None


def _date_sk(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def _build_date_row(d: date) -> Dict[str, Any]:
    return {
        "date_sk": _date_sk(d),
        "date": d.isoformat(),
        "year": d.year,
        "quarter": (d.month - 1) // 3 + 1,
        "month": d.month,
        "day": d.day,
        "day_of_week": d.isoweekday(),
        "is_weekend": d.isoweekday() in (6, 7),
    }


def _ensure_dim_date(client: Client, dates: List[date]) -> int:
    unique_dates = {d for d in dates if d}
    if not unique_dates:
        return 0
    res = _cleaned_table(client, "dim_date").select("date").execute()
    existing = {row["date"] for row in (res.data or [])}
    inserts: List[Dict[str, Any]] = []
    for d in sorted(unique_dates):
        if d.isoformat() in existing:
            continue
        inserts.append(_build_date_row(d))
    if not inserts:
        return 0
    _cleaned_table(client, "dim_date").upsert(inserts, on_conflict="date").execute()
    return len(inserts)


def _hash_customer_id(customer_id: Any) -> Optional[str]:
    if customer_id in (None, ""):
        return None
    data = str(customer_id).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Category dimension
# ---------------------------------------------------------------------------


def sync_dim_category(client: Optional[Client] = None) -> int:
    """Populate ``cleaned.dim_category`` from ``public.category``."""

    client = client or get_supabase_client()
    rows = _get_public_rows(client, "category")
    if not rows:
        return 0

    existing_map: dict[int, int] = {}
    try:
        res = _cleaned_table(client, "dim_category").select("category_id, category_sk").execute()
        existing_map = {row["category_id"]: row["category_sk"] for row in (res.data or [])}
    except Exception:
        existing_map = {}

    final_sk_map: dict[int, int] = {}
    for r in rows:
        cat_id = r.get("id")
        if cat_id is None:
            continue
        final_sk_map[cat_id] = existing_map.get(cat_id) or cat_id

    cleaned_rows: List[Dict[str, Any]] = []
    for r in rows:
        cat_id = r.get("id")
        if cat_id is None:
            continue
        parent_id = r.get("parent_id")
        cleaned_rows.append(
            {
                "category_sk": final_sk_map.get(cat_id),
                "category_id": cat_id,
                "parent_category_id": parent_id,
                "parent_category_sk": final_sk_map.get(parent_id) if parent_id is not None else None,
                "name": r.get("name"),
                "level": r.get("level"),
                "url_key": r.get("url_key"),
                "url_path": r.get("url_path"),
                "status": r.get("status"),
                "include_in_menu": r.get("include_in_menu"),
                "is_leaf": r.get("is_leaf"),
                "product_count": r.get("product_count"),
                "meta_title": r.get("meta_title"),
                "meta_description": r.get("meta_description"),
                "thumbnail_url": r.get("thumbnail_url"),
            }
        )

    _cleaned_table(client, "dim_category").upsert(cleaned_rows, on_conflict="category_id").execute()
    return len(cleaned_rows)


# ---------------------------------------------------------------------------
# Seller dimension
# ---------------------------------------------------------------------------


def sync_dim_seller(client: Optional[Client] = None) -> int:
    """Populate ``cleaned.dim_seller`` from ``public.seller``."""

    client = client or get_supabase_client()
    rows = _get_public_rows(client, "seller")
    if not rows:
        return 0

    existing_map: dict[int, int] = {}
    try:
        res = _cleaned_table(client, "dim_seller").select("seller_id, seller_sk").execute()
        existing_map = {row["seller_id"]: row["seller_sk"] for row in (res.data or [])}
    except Exception:
        existing_map = {}

    cleaned_rows: List[Dict[str, Any]] = []
    for r in rows:
        cleaned_rows.append(
            {
                "seller_sk": existing_map.get(r.get("id")) or r.get("id"),
                "seller_id": r.get("id"),
                "name": r.get("name"),
                "seller_type": r.get("seller_type"),
                "is_official": r.get("is_official"),
                "store_id": r.get("store_id"),
                "store_level": r.get("store_level"),
                "profile_url": r.get("url"),
                "icon_url": r.get("icon"),
                "days_since_joined": r.get("days_since_joined"),
                "total_follower": r.get("total_follower"),
                "rating": r.get("avg_rating_point"),
                "avg_rating_point": r.get("avg_rating_point"),
                "review_count": r.get("review_count"),
            }
        )

    _cleaned_table(client, "dim_seller").upsert(cleaned_rows, on_conflict="seller_id").execute()
    return len(cleaned_rows)


# ---------------------------------------------------------------------------
# Product dimension + specifications parsing
# ---------------------------------------------------------------------------


def _parse_specifications(spec: Any) -> Dict[str, Any]:
    """Extract structured attributes from the nested ``specifications`` JSON."""

    out: Dict[str, Any] = {
        "brand_country": None,
        "origin": None,
        "expiry_time": None,
        "capacity_raw": None,
        "product_weight_raw": None,
        "suitable_age_raw": None,
        "is_warranty_applied": None,
        "is_organic": None,
        "regional_specialties": None,
        "organization_name": None,
        "organization_address": None,
    }

    if not spec:
        return out

    try:
        groups = spec
        if isinstance(groups, str):
            import json

            groups = json.loads(groups)
    except Exception:
        return out

    if not isinstance(groups, list):
        return out

    for group in groups:
        attrs = group.get("attributes") if isinstance(group, dict) else None
        if not isinstance(attrs, list):
            continue
        for attr in attrs:
            if not isinstance(attr, dict):
                continue
            code = attr.get("code")
            value = attr.get("value")
            if not code:
                continue
            match code:
                case "brand_country":
                    out["brand_country"] = value
                case "origin":
                    out["origin"] = value
                case "expiry_time":
                    out["expiry_time"] = value
                case "capacity":
                    out["capacity_raw"] = value
                case "product_weight":
                    out["product_weight_raw"] = value
                case "suitable_age_for_use":
                    out["suitable_age_raw"] = value
                case "is_warranty_applied":
                    if isinstance(value, str):
                        low = value.strip().lower()
                        if low in {"có", "co", "yes", "true"}:
                            out["is_warranty_applied"] = True
                        elif low in {"không", "khong", "no", "false"}:
                            out["is_warranty_applied"] = False
                        else:
                            out["is_warranty_applied"] = None
                    else:
                        out["is_warranty_applied"] = None
                case "Organic":
                    if isinstance(value, str):
                        out["is_organic"] = value.strip().lower() in {"có", "co", "yes", "true"}
                case "regional_specialties":
                    out["regional_specialties"] = value
                case "Organization_name":
                    out["organization_name"] = value
                case "Organization_address":
                    out["organization_address"] = value
                case _:
                    continue

    return out


def _derive_age_fields(suitable_age_raw: Optional[str]) -> Dict[str, Any]:
    """Derive ``min_age_years`` and ``age_segment`` from raw age text."""

    if not suitable_age_raw:
        return {"min_age_years": None, "age_segment": None}

    text = suitable_age_raw.strip().lower()
    min_age: Optional[float] = None
    segment: Optional[str] = None

    import re

    m = re.search(r"(\d+)(?:\s*-\s*\d+)?\s*\+?", text)
    if m:
        min_age = float(m.group(1))

    if "trẻ" in text or "tre" in text:
        if min_age is not None and min_age <= 1:
            segment = "under_1_or_1_plus"
        elif min_age is not None and min_age <= 3:
            segment = "kids_1_3"
        elif min_age is not None and min_age <= 12:
            segment = "kids_4_12"
        else:
            segment = "kids_unspecified"
    elif "gia đình" in text or "gia dinh" in text:
        segment = "family"
    elif text in {"không", "khong", "none", "all"}:
        segment = "unspecified"
    else:
        if min_age is not None:
            if min_age <= 1:
                segment = "1_plus"
            elif min_age <= 3:
                segment = "kids_1_3"
            elif min_age <= 12:
                segment = "kids_4_12"
            else:
                segment = "adult"

    return {"min_age_years": min_age, "age_segment": segment}


def sync_dim_product(client: Optional[Client] = None) -> int:
    """Populate ``cleaned.dim_product`` from ``public.product``."""

    client = client or get_supabase_client()
    rows = _get_public_rows(client, "product")
    if not rows:
        return 0

    cat_res = _cleaned_table(client, "dim_category").select("category_sk, category_id").execute()
    cat_map = {c["category_id"]: c["category_sk"] for c in (cat_res.data or [])}

    seller_res = _cleaned_table(client, "dim_seller").select("seller_sk, seller_id").execute()
    seller_map = {s["seller_id"]: s["seller_sk"] for s in (seller_res.data or [])}

    product_res = _cleaned_table(client, "dim_product").select("product_id, product_sk").execute()
    product_map = {p["product_id"]: p["product_sk"] for p in (product_res.data or [])}

    cleaned_rows: List[Dict[str, Any]] = []
    for r in rows:
        pid = r.get("id")
        if pid is None:
            continue
        category_id = r.get("category_id")
        category_sk = cat_map.get(category_id)
        if category_sk is None:
            continue

        seller_id = r.get("seller_id")
        seller_sk = seller_map.get(seller_id) if seller_id is not None else None

        spec_fields = _parse_specifications(r.get("specifications"))
        age_fields = _derive_age_fields(spec_fields.get("suitable_age_raw"))

        cleaned_rows.append(
            {
                "product_sk": product_map.get(pid) or pid,
                "product_id": pid,
                "category_sk": category_sk,
                "seller_sk": seller_sk,
                "master_id": r.get("master_id"),
                "sku": r.get("sku"),
                "name": r.get("name"),
                "brand_id": r.get("brand_id"),
                "brand_name": r.get("brand"),
                "brand_slug": r.get("brand_slug"),
                "brand_country": spec_fields.get("brand_country"),
                "origin": spec_fields.get("origin"),
                "expiry_time": spec_fields.get("expiry_time"),
                "is_warranty_applied": spec_fields.get("is_warranty_applied"),
                "is_baby_milk": r.get("is_baby_milk"),
                "is_acoholic_drink": r.get("is_acoholic_drink"),
                "is_fresh": r.get("is_fresh"),
                "capacity_raw": spec_fields.get("capacity_raw"),
                "unit_volume_ml": None,
                "product_weight_raw": spec_fields.get("product_weight_raw"),
                "unit_weight_g": None,
                "suitable_age_raw": spec_fields.get("suitable_age_raw"),
                "min_age_years": age_fields.get("min_age_years"),
                "age_segment": age_fields.get("age_segment"),
                "is_organic": spec_fields.get("is_organic"),
                "regional_specialties": spec_fields.get("regional_specialties"),
                "organization_name": spec_fields.get("organization_name"),
                "organization_address": spec_fields.get("organization_address"),
                "thumbnail_url": r.get("thumbnail_url"),
                "tiki_url": r.get("tiki_url"),
                "product_first_seen_at": r.get("created_at"),
                "product_last_updated_at": r.get("updated_at"),
            }
        )

    if not cleaned_rows:
        return 0

    _cleaned_table(client, "dim_product").upsert(cleaned_rows, on_conflict="product_id").execute()
    return len(cleaned_rows)


def _extract_thanh_phan(spec: Any) -> Optional[str]:
    """Extract the first ``thanh_phan`` value from specifications JSON."""

    if not spec:
        return None

    try:
        groups = spec
        if isinstance(groups, str):
            import json

            groups = json.loads(groups)
    except Exception:
        return None

    if not isinstance(groups, list):
        return None

    for group in groups:
        attrs = group.get("attributes") if isinstance(group, dict) else None
        if not isinstance(attrs, list):
            continue
        for attr in attrs:
            if not isinstance(attr, dict):
                continue
            if attr.get("code") == "thanh_phan":
                return attr.get("value")
    return None


def sync_product_ingredients(client: Optional[Client] = None) -> int:
    """Populate ``cleaned.product_ingredients`` from ``public.product.specifications``."""

    client = client or get_supabase_client()
    rows = _get_public_rows(client, "product")
    if not rows:
        return 0

    dim_res = _cleaned_table(client, "dim_product").select("product_sk, product_id").execute()
    id_to_sk = {r["product_id"]: r["product_sk"] for r in (dim_res.data or [])}

    existing_map: dict[tuple[int, str], int] = {}
    next_sk = 1
    try:
        cur = _cleaned_table(client, "product_ingredients").select("product_ingredient_sk, product_sk, source_code").execute()
        rows_existing = cur.data or []
        for row in rows_existing:
            key = (row["product_sk"], row.get("source_code") or "thanh_phan")
            existing_map[key] = row["product_ingredient_sk"]
        if rows_existing:
            next_sk = max(row["product_ingredient_sk"] for row in rows_existing) + 1
    except Exception:
        existing_map = {}

    def _next_sk_for(product_sk: int, source_code: str) -> int:
        nonlocal next_sk
        key = (product_sk, source_code)
        if key in existing_map:
            return existing_map[key]
        existing_map[key] = next_sk
        next_sk += 1
        return existing_map[key]

    inserts: List[Dict[str, Any]] = []
    for r in rows:
        pid = r.get("id")
        if pid is None:
            continue
        product_sk = id_to_sk.get(pid)
        if product_sk is None:
            continue
        value = _extract_thanh_phan(r.get("specifications"))
        if not value:
            continue
        source_code = "thanh_phan"
        ingredient_sk = _next_sk_for(product_sk, source_code)
        inserts.append(
            {
                "product_ingredient_sk": ingredient_sk,
                "product_sk": product_sk,
                "source_code": source_code,
                "ingredient_text_raw": value,
                "ingredient_text_clean": None,
            }
        )

    if not inserts:
        return 0

    _cleaned_table(client, "product_ingredients").upsert(inserts, on_conflict="product_sk,source_code").execute()
    return len(inserts)


# ---------------------------------------------------------------------------
# Daily fact snapshots (single-run friendly)
# ---------------------------------------------------------------------------


def sync_fact_product_daily(snapshot_date: Optional[date] = None, client: Optional[Client] = None) -> int:
    """Snapshot product metrics into ``cleaned.fact_product_daily``.

    Designed for ad-hoc runs (no scheduler); captures the current state of
    ``public.product`` with a single ``date_sk`` and ``snapshot_at`` timestamp.
    """

    client = client or get_supabase_client()
    rows = _get_public_rows(client, "product")
    if not rows:
        return 0

    dim_res = _cleaned_table(client, "dim_product").select("product_id, product_sk, category_sk, seller_sk").execute()
    dim_map = {row["product_id"]: row for row in (dim_res.data or [])}

    snapshot = snapshot_date or datetime.now(timezone.utc).date()
    _ensure_dim_date(client, [snapshot])
    date_sk = _date_sk(snapshot)
    now_iso = datetime.now(timezone.utc).isoformat()

    inserts: List[Dict[str, Any]] = []
    for r in rows:
        pid = r.get("id")
        dim_row = dim_map.get(pid)
        if not dim_row:
            continue
        category_sk = dim_row.get("category_sk")
        if category_sk is None:
            continue
        product_sk = dim_row.get("product_sk")
        seller_sk = dim_row.get("seller_sk")

        price = r.get("price")
        list_price = r.get("list_price")
        price_vs_list_percent = None
        if list_price not in (None, 0):
            try:
                base = float(list_price)
                if price is not None:
                    price_vs_list_percent = round(((base - float(price)) / base) * 100, 2)
            except (TypeError, ValueError):
                price_vs_list_percent = None

        product_daily_sk = product_sk * 100000 + date_sk

        inserts.append(
            {
                "product_daily_sk": product_daily_sk,
                "product_sk": product_sk,
                "date_sk": date_sk,
                "category_sk": category_sk,
                "seller_sk": seller_sk,
                "price": price,
                "list_price": list_price,
                "original_price": r.get("original_price"),
                "discount": r.get("discount"),
                "discount_rate": r.get("discount_rate"),
                "rating_average": r.get("rating_average"),
                "review_count_cumulative": r.get("review_count"),
                "all_time_quantity_sold_cumulative": r.get("all_time_quantity_sold"),
                "price_vs_list_percent": price_vs_list_percent,
                "snapshot_at": now_iso,
            }
        )

    if not inserts:
        return 0

    _cleaned_table(client, "fact_product_daily").upsert(
        inserts,
        on_conflict="product_sk,date_sk",
    ).execute()
    return len(inserts)


def sync_fact_seller_daily(snapshot_date: Optional[date] = None, client: Optional[Client] = None) -> int:
    """Snapshot seller metrics into ``cleaned.fact_seller_daily``.

    Like ``sync_fact_product_daily``, this is intended for single-run usage and
    records one row per seller for the chosen snapshot date.
    """

    client = client or get_supabase_client()
    rows = _get_public_rows(client, "seller")
    if not rows:
        return 0

    dim_res = _cleaned_table(client, "dim_seller").select("seller_id, seller_sk").execute()
    seller_map = {row["seller_id"]: row["seller_sk"] for row in (dim_res.data or [])}

    snapshot = snapshot_date or datetime.now(timezone.utc).date()
    _ensure_dim_date(client, [snapshot])
    date_sk = _date_sk(snapshot)
    now_iso = datetime.now(timezone.utc).isoformat()

    inserts: List[Dict[str, Any]] = []
    for r in rows:
        sid = r.get("id")
        seller_sk = seller_map.get(sid)
        if seller_sk is None:
            continue

        days_since_joined = r.get("days_since_joined")
        try:
            days_active = int(days_since_joined) if days_since_joined is not None else None
        except (TypeError, ValueError):
            days_active = None

        seller_daily_sk = seller_sk * 100000 + date_sk

        inserts.append(
            {
                "seller_daily_sk": seller_daily_sk,
                "seller_sk": seller_sk,
                "date_sk": date_sk,
                "rating": r.get("rating"),
                "avg_rating_point": r.get("avg_rating_point"),
                "review_count_cumulative": r.get("review_count"),
                "total_follower_cumulative": r.get("total_follower"),
                "days_since_joined": days_since_joined,
                "days_active": days_active,
                "snapshot_at": now_iso,
            }
        )

    if not inserts:
        return 0

    _cleaned_table(client, "fact_seller_daily").upsert(
        inserts,
        on_conflict="seller_sk,date_sk",
    ).execute()
    return len(inserts)


# ---------------------------------------------------------------------------
# Reviews
# ---------------------------------------------------------------------------


def sync_review_clean(client: Optional[Client] = None) -> int:
    client = client or get_supabase_client()
    rows = _get_public_rows(client, "review")
    if not rows:
        return 0

    product_res = _cleaned_table(client, "dim_product").select("product_id, product_sk").execute()
    product_map = {row["product_id"]: row["product_sk"] for row in (product_res.data or [])}

    seller_res = _cleaned_table(client, "dim_seller").select("seller_id, seller_sk").execute()
    seller_map = {row["seller_id"]: row["seller_sk"] for row in (seller_res.data or [])}

    existing_res = _cleaned_table(client, "review_clean").select("review_id, review_sk").execute()
    existing_map = {row["review_id"]: row["review_sk"] for row in (existing_res.data or [])}

    date_candidates: List[date] = []
    inserts: List[Dict[str, Any]] = []

    for r in rows:
        review_id = r.get("id")
        product_id = r.get("product_id")
        rating = r.get("rating")
        if review_id is None or product_id is None or rating is None:
            continue
        product_sk = product_map.get(product_id)
        if product_sk is None:
            continue
        seller_sk = seller_map.get(r.get("seller_id"))

        created_dt = _parse_datetime(r.get("created_at"))
        purchased_dt = _parse_datetime(r.get("purchased_at"))
        if created_dt:
            date_candidates.append(created_dt.date())
        if purchased_dt:
            date_candidates.append(purchased_dt.date())

        content = r.get("content") or ""
        content_length = len(content) if content else None
        word_count = len(content.split()) if content else None

        attributes = r.get("attributes")
        has_images = None
        image_count = None
        if isinstance(attributes, dict):
            images = attributes.get("images") or attributes.get("photos")
            if isinstance(images, list):
                image_count = len(images)
                has_images = image_count > 0

        days_used = None
        if created_dt and purchased_dt:
            diff = created_dt - purchased_dt
            days_used = diff.days if diff.days >= 0 else None

        insert_row = {
            "review_sk": existing_map.get(review_id) or review_id,
            "review_id": review_id,
            "product_sk": product_sk,
            "seller_sk": seller_sk,
            "customer_id_hash": _hash_customer_id(r.get("customer_id")),
            "rating": rating,
            "created_at": created_dt.isoformat() if created_dt else r.get("created_at"),
            "purchased": r.get("purchased"),
            "purchased_at": purchased_dt.isoformat() if purchased_dt else r.get("purchased_at"),
            "thank_count": r.get("thank_count"),
            "comment_count": r.get("comment_count"),
            "title": r.get("title"),
            "content": content or None,
            "content_length": content_length,
            "word_count": word_count,
            "has_images": has_images,
            "image_count": image_count,
            "days_used_at_review": days_used,
            "delivery_date": None,
            "delivery_time_hours": None,
            "delivery_time_rating": None,
            "shipper_attitude_rating": None,
            "delivery_time_slot_rating": None,
            "packing_quality_rating": None,
            "customer_total_review": None,
            "customer_total_thank": None,
            "loaded_at": datetime.now(timezone.utc).isoformat(),
        }
        extra_fields = _parse_review_extra(r.get("extra"))
        if insert_row["days_used_at_review"] is None and extra_fields.get("days_used_at_review") is not None:
            try:
                insert_row["days_used_at_review"] = int(extra_fields["days_used_at_review"])
            except (TypeError, ValueError):
                insert_row["days_used_at_review"] = None
        elif insert_row["days_used_at_review"] is not None:
            try:
                insert_row["days_used_at_review"] = int(insert_row["days_used_at_review"])
            except (TypeError, ValueError):
                insert_row["days_used_at_review"] = None
        for key in (
            "delivery_date",
            "delivery_time_hours",
            "delivery_time_rating",
            "shipper_attitude_rating",
            "delivery_time_slot_rating",
            "packing_quality_rating",
        ):
            if extra_fields.get(key) is not None:
                insert_row[key] = extra_fields[key]
        if extra_fields.get("delivery_date_obj"):
            date_candidates.append(extra_fields["delivery_date_obj"])

        inserts.append(insert_row)

    ensured = _ensure_dim_date(client, date_candidates)

    if not inserts:
        return 0

    _cleaned_table(client, "review_clean").upsert(inserts, on_conflict="review_id").execute()
    return len(inserts)


def _fetch_review_clean_rows(client: Client) -> List[Dict[str, Any]]:
    return _get_cleaned_rows(
        client,
        "review_clean",
        "product_sk,rating,created_at,thank_count,comment_count,purchased",
    )


def _parse_review_extra(extra: Any) -> Dict[str, Any]:
    fields: Dict[str, Any] = {
        "delivery_date": None,
        "delivery_date_obj": None,
        "delivery_time_hours": None,
        "delivery_time_rating": None,
        "shipper_attitude_rating": None,
        "delivery_time_slot_rating": None,
        "packing_quality_rating": None,
        "days_used_at_review": None,
    }

    if not extra:
        return fields
    data = extra
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return fields
    if not isinstance(data, dict):
        return fields

    timeline = data.get("timeline")
    review_dt = None
    if isinstance(timeline, dict):
        delivery_dt = _parse_datetime(timeline.get("delivery_date"))
        review_dt = _parse_datetime(timeline.get("review_created_date"))
        if delivery_dt:
            fields["delivery_date"] = delivery_dt.isoformat()
            fields["delivery_date_obj"] = delivery_dt.date()
        if delivery_dt and review_dt:
            diff_hours = (review_dt - delivery_dt).total_seconds() / 3600
            if diff_hours >= 0:
                fields["delivery_time_hours"] = round(diff_hours, 2)
        content = timeline.get("content")
        if isinstance(content, str):
            match = re.search(r"(\d+[\.,]?\d*)\s*(giờ|ngày)", content.lower())
            if match:
                value = float(match.group(1).replace(",", "."))
                unit = match.group(2)
                days = value / 24 if "giờ" in unit else value
                fields["days_used_at_review"] = round(days, 2)

    delivery_rating = data.get("delivery_rating")
    if isinstance(delivery_rating, list):
        for item in delivery_rating:
            if not isinstance(item, dict):
                continue
            question = (item.get("question") or "").strip().lower()
            option = item.get("option")
            if not option:
                continue
            if "thời gian giao hàng" in question:
                fields["delivery_time_rating"] = option
            elif "thái độ" in question:
                fields["shipper_attitude_rating"] = option
            elif "giờ giao hàng" in question:
                fields["delivery_time_slot_rating"] = option
            elif "đóng gói" in question:
                fields["packing_quality_rating"] = option

    return fields


def sync_fact_product_review_agg_daily(client: Optional[Client] = None) -> int:
    client = client or get_supabase_client()
    rows = _fetch_review_clean_rows(client)
    if not rows:
        return 0

    aggregates: Dict[tuple[int, int], Dict[str, Any]] = {}
    dates_needed: List[date] = []
    for row in rows:
        created_dt = _parse_datetime(row.get("created_at"))
        if not created_dt:
            continue
        created_date = created_dt.date()
        dates_needed.append(created_date)
        date_sk = _date_sk(created_date)
        product_sk = row.get("product_sk")
        if product_sk is None:
            continue
        key = (product_sk, date_sk)
        agg = aggregates.setdefault(
            key,
            {
                "count": 0,
                "sum_rating": 0,
                "sum_rating_sq": 0,
                "rating_counts": defaultdict(int),
                "thank_sum": 0,
                "comment_sum": 0,
                "purchased_count": 0,
                "non_purchased_count": 0,
            },
        )
        rating = row.get("rating") or 0
        agg["count"] += 1
        agg["sum_rating"] += rating
        agg["sum_rating_sq"] += rating * rating
        agg["rating_counts"][int(rating)] += 1
        agg["thank_sum"] += row.get("thank_count") or 0
        agg["comment_sum"] += row.get("comment_count") or 0
        if row.get("purchased"):
            agg["purchased_count"] += 1
        else:
            agg["non_purchased_count"] += 1

    ensured = _ensure_dim_date(client, dates_needed)

    inserts: List[Dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for (product_sk, date_sk), agg in aggregates.items():
        count = agg["count"]
        if not count:
            continue
        avg_rating = agg["sum_rating"] / count
        variance = (agg["sum_rating_sq"] / count) - (avg_rating ** 2)
        variance = max(variance, 0)
        stddev = variance ** 0.5
        rating_counts = agg["rating_counts"]
        insert_row = {
            "product_review_agg_daily_sk": product_sk * 100000 + date_sk,
            "product_sk": product_sk,
            "date_sk": date_sk,
            "review_count": count,
            "avg_rating": round(avg_rating, 3),
            "rating_1_count": rating_counts.get(1, 0),
            "rating_2_count": rating_counts.get(2, 0),
            "rating_3_count": rating_counts.get(3, 0),
            "rating_4_count": rating_counts.get(4, 0),
            "rating_5_count": rating_counts.get(5, 0),
            "thank_count_sum": agg["thank_sum"],
            "comment_count_sum": agg["comment_sum"],
            "purchased_review_count": agg["purchased_count"],
            "non_purchased_review_count": agg["non_purchased_count"],
            "rating_stddev": round(stddev, 4),
            "last_aggregated_at": now_iso,
        }
        inserts.append(insert_row)

    if not inserts:
        return 0

    _cleaned_table(client, "fact_product_review_agg_daily").upsert(
        inserts,
        on_conflict="product_sk,date_sk",
    ).execute()
    return len(inserts)


def sync_fact_product_review_summary(client: Optional[Client] = None) -> int:
    client = client or get_supabase_client()
    rows = _fetch_review_clean_rows(client)
    if not rows:
        return 0

    summary: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        product_sk = row.get("product_sk")
        rating = row.get("rating") or 0
        if product_sk is None:
            continue
        agg = summary.setdefault(
            product_sk,
            {
                "count": 0,
                "sum_rating": 0,
                "rating_counts": defaultdict(int),
            },
        )
        agg["count"] += 1
        agg["sum_rating"] += rating
        agg["rating_counts"][int(rating)] += 1

    inserts: List[Dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for product_sk, agg in summary.items():
        count = agg["count"]
        if not count:
            continue
        avg_rating = agg["sum_rating"] / count
        rating_counts = agg["rating_counts"]
        def _pct(star: int) -> Optional[float]:
            value = rating_counts.get(star, 0)
            return round(value * 100 / count, 2) if count else None

        insert_row = {
            "product_review_summary_sk": product_sk,
            "product_sk": product_sk,
            "rating_average": round(avg_rating, 3),
            "reviews_count": count,
            "star_1_count": rating_counts.get(1, 0),
            "star_2_count": rating_counts.get(2, 0),
            "star_3_count": rating_counts.get(3, 0),
            "star_4_count": rating_counts.get(4, 0),
            "star_5_count": rating_counts.get(5, 0),
            "star_1_percent": _pct(1),
            "star_2_percent": _pct(2),
            "star_3_percent": _pct(3),
            "star_4_percent": _pct(4),
            "star_5_percent": _pct(5),
            "snapshot_at": now_iso,
        }
        inserts.append(insert_row)

    if not inserts:
        return 0

    _cleaned_table(client, "fact_product_review_summary").upsert(
        inserts,
        on_conflict="product_sk",
    ).execute()
    return len(inserts)


def run_transform_with_plan(plan: TransformPlan, client: Optional[Client] = None) -> TransformResult:
    """Execute only the transform stages enabled in ``plan``."""

    client = client or get_supabase_client()
    result = TransformResult()

    if plan.dim_category:
        result.dim_category_rows = sync_dim_category(client)
    if plan.dim_seller:
        result.dim_seller_rows = sync_dim_seller(client)
    if plan.dim_product:
        result.dim_product_rows = sync_dim_product(client)
    if plan.product_ingredients:
        result.product_ingredient_rows = sync_product_ingredients(client)
    if plan.fact_product_daily:
        result.fact_product_daily_rows = sync_fact_product_daily(client=client)
    if plan.fact_seller_daily:
        result.fact_seller_daily_rows = sync_fact_seller_daily(client=client)
    if plan.review_clean:
        result.review_clean_rows = sync_review_clean(client)
    if plan.review_daily:
        result.review_daily_rows = sync_fact_product_review_agg_daily(client)
    if plan.review_summary:
        result.review_summary_rows = sync_fact_product_review_summary(client)

    return result


def run_full_transform(client: Optional[Client] = None) -> TransformResult:
    """Run every transform stage in sequence (legacy behaviour)."""

    return run_transform_with_plan(TransformPlan(), client)

