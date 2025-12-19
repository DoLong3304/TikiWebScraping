# **Tiki Web Scraping**

- Async ETL that discovers Tiki categories, paginates listings, enriches products, captures reviews, and stores everything in Supabase (Postgres).
- Works for any Tiki category id; the historic default is the milk parent category (8273) but you can override per run.
- Two entry points: CLI for automation and a Tkinter GUI for guided runs, health checks, stats, and a safe SQL playground.

---

## How the ETL works

- **Discover**: fetch child categories for a chosen parent category id.
- **List**: crawl paginated listings for each leaf category with jittered delays.
- **Enrich**: pull full product details and seller widget data, upserting into Supabase.
- **Review**: fetch paginated reviews per product with basic deduplication.
- **Persist**: upsert into `category`, `product`, `seller`, and `review` tables (schema in `supabase_schema.sql`).

---

## Project structure

- `supabase_schema.sql` – tables and indexes for Supabase.
- `requirements.txt` – Python dependencies.
- `src/config.py` – API endpoints, defaults, and tunable limits (env-driven).
- `src/db/supabase_client.py` – cached Supabase client + upsert helpers.
- `src/tiki_client/` – API clients for categories, listings, products, reviews, sellers.
- `src/pipeline/orchestrator.py` – CLI + shared orchestration logic.
- `src/gui/gui_app.py` / `src/gui/pipeline_runner.py` – Tkinter control panel wrapper.
- `sample_data/` – offline samples for quick validation.
- `tests/test_pipeline_smoke.py` – minimal connectivity and mapping test.

---

## Prerequisites

- Python 3.10+ on your machine or Colab runtime.
- Supabase project with a service role key.
- Network access to `tiki.vn` and your Supabase endpoint.

---

## Setup

1. Clone the repo and install deps

```powershell
cd "C:\Users\dohai\PycharmProjects\TikiWebScraping2"
python -m pip install -r requirements.txt
```

2. Provision Supabase schema

- In the Supabase SQL editor, run the contents of `supabase_schema.sql`.

3. Configure environment

- Create `.env` and fill the following info:
  - `SUPABASE_URL=https://<project>.supabase.co`
  - `SUPABASE_SERVICE_KEY=<service_role_key>`
  - `TIKI_PARENT_CATEGORY_ID=<category_id>` (optional; default 8273)
  - Optional tuning: `TIKI_MAX_PAGES_PER_CATEGORY`, `TIKI_MAX_REVIEW_PAGES_PER_PRODUCT`, `TIKI_BASE_DELAY_SECONDS`, `TIKI_JITTER_RANGE`.

---

## Run (CLI)

Stages always run in order: categories/listings → products → reviews → sellers.

- Full scrape + update (default parent category):

```powershell
python -m src.pipeline.orchestrator
```

- Update existing records only:

```powershell
python -m src.pipeline.orchestrator --mode update
```

- Target a different category root:

```powershell
python -m src.pipeline.orchestrator --parent-category 8322
```

- Reviews only for specific products:

```powershell
python -m src.pipeline.orchestrator --data reviews --mode update --product-ids 123,456
```

- Skip sellers refresh:

```powershell
python -m src.pipeline.orchestrator --data categories_listings products reviews --mode scrape
```

Args of interest: `--data` (stages), `--mode` (`scrape`|`update`), `--product-ids`, `--start-index`, `--parent-category`.

---

## Run (GUI)

```powershell
python -m src.gui.gui_app
```

What you get: connectivity checks, runtime settings (parent category, paging caps, delays), ordered run plan, retry failed reviews, Supabase vs Tiki stats, and a guarded SQL editor (`SELECT ... FROM <table> [LIMIT n]`).

---

## Run on Colab

```python
!git clone YOUR_REPO_URL
%cd TikiWebScraping2
!pip install -r requirements.txt

import os
os.environ["SUPABASE_URL"] = "https://your-project-id.supabase.co"
os.environ["SUPABASE_SERVICE_KEY"] = "your-service-role-key"
os.environ["TIKI_PARENT_CATEGORY_ID"] = "8273"  # or any category id

!python -m src.pipeline.orchestrator --data categories_listings products
```

---

## Troubleshooting (Q&A)

- **"Missing SUPABASE_URL/SUPABASE_SERVICE_KEY"**: export them in the shell or place them in `.env` and restart the shell/IDE.
- **"Permission denied on Supabase"**: confirm you are using the service role key, not the anon key.
- **"No leaf categories returned"**: verify `TIKI_PARENT_CATEGORY_ID` is valid and reachable from your network.
- **"Pipeline hangs on reviews"**: reduce `TIKI_MAX_REVIEW_PAGES_PER_PRODUCT` or increase `TIKI_BASE_DELAY_SECONDS` to be gentler.
- **"Rate limited by Tiki"**: lower concurrency via `TIKI_MAX_CONCURRENT_REQUESTS` (the HTTP client already jitters between pages).

---

## Example use cases

- Build a price tracker for any Tiki category id.
- Enrich a subset of products by passing `--product-ids` for focused refresh.
- Compare Tiki estimates vs Supabase counts using the GUI stats tab before running a full crawl.
- Export review text from Supabase to downstream analytics/BI tools.

---

## Tests

```powershell
pytest -q
```

`tests/test_pipeline_smoke.py` checks env wiring, Supabase connectivity, and basic category mapping using local sample data.

---

## Notes

- The default parent category remains the historic milk category for backward compatibility; override it per run for other domains.
- Supabase client is cached per process to reduce overhead.
- Network calls are best-effort; errors are logged and summarized per stage so runs can continue.

---

## Cleaned schema transform

The project includes a post-scrape transform step that normalizes raw data in the `public` schema into an analysis-ready `cleaned` schema.

Key components:

- `cleaned_schema.sql` – SQL DDL for the `cleaned` schema (dimensions, facts, review tables, feature table).
- `src/pipeline/transform.py` – orchestrates the transform:
  - `sync_dim_category` – `public.category` → `cleaned.dim_category`.
  - `sync_dim_seller` – `public.seller` → `cleaned.dim_seller`.
  - `sync_dim_product` – `public.product` → `cleaned.dim_product` (including spec-derived fields like `capacity_raw`, `suitable_age_raw`, `is_organic`, etc.).
  - `sync_product_ingredients` – extracts `thanh_phan` (ingredients) from `public.product.specifications` into `cleaned.product_ingredients`.
- `src/pipeline/extract.py` – higher-level extract helpers that pull data from Tiki APIs into the raw `public` tables (`category`, `product`, `seller`, `review`) using the same logic as the orchestrator, but with a simpler synchronous API.

The transform code is read-only with respect to the crawling pipeline: it only reads from `public` tables and writes into `cleaned`.

### Running the transform

After running a crawl (either via CLI, GUI, or `extract_all`), you can populate the cleaned schema from Python:

```python
from src.pipeline.transform import run_full_transform

result = run_full_transform()  # uses the default Supabase client
print(result)
# TransformResult(dim_category_rows=..., dim_seller_rows=..., dim_product_rows=..., product_ingredient_rows=...)
```

Or, if you prefer an async entrypoint, you can use the helper in `src/pipeline/orchestrator.py`:

```python
import asyncio
from src.pipeline.orchestrator import run_transform_only

result = asyncio.run(run_transform_only())
print(result)
```

Both approaches leave the existing crawling code untouched and simply normalize whatever data already exists in `public` into the `cleaned` schema. Missing or malformed JSON fields are mapped to `NULL` so you can gradually improve coverage over time.
