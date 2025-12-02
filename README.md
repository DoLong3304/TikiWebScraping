# Tiki Milk Products Scraping Project

This project scrapes Tiki product data (focused on milk categories by default, but extensible to other categories) and stores it in a Supabase (Postgres) database. It is designed to run locally, on Google Colab, or via the Colab extension inside VS Code.

---

## 1. Project structure (high level)

- `supabase_schema.sql` – SQL to create tables in your Supabase Postgres database.
- `requirements.txt` – Python dependencies.
- `.env.example` – sample environment variables (copy to `.env`).
- `src/config.py` – configuration for Tiki endpoints, rate limiting, and Supabase.
- `src/db/supabase_client.py` – Supabase client and upsert helpers.
- `src/tiki_client/` – Tiki API clients:
  - `categories.py` – fetches subcategories.
  - `listings.py` – fetches product listings per category with pagination.
  - `products.py` – fetches detailed product info.
  - `reviews.py` – fetches paginated product reviews.
  - `sellers.py` – fetches rich seller info from the seller widget API.
- `src/pipeline/orchestrator.py` – orchestration of scraping stages and run modes.
- `tests/test_pipeline_smoke.py` – simple smoke test for the pipeline (once you configure Supabase and Python).

---

## 2. Supabase project setup

1. **Create a Supabase project**

   - Go to https://supabase.com and sign in.
   - Create a new project (choose a region and a strong database password).

2. **Get connection details**

   - In the Supabase dashboard, open your project.
   - Go to **Project Settings → API** and note:
     - `Project URL` (e.g. `https://your-project-id.supabase.co`).
     - `service_role` key (keep this secret; used by backend only).

3. **Create database schema**

   - In the Supabase dashboard, go to **SQL Editor**.
   - Create a new query.
   - Paste the contents of `supabase_schema.sql` from this repo.
   - Run the script; it will create the `category`, `product`, `seller`, `review`, and `product_category` tables and indexes.

4. **(Optional) Check tables**

   - Go to **Table Editor** and confirm the tables exist.

5. **Security note**
   - For this educational backend-only project, you’ll typically use the `service_role` key from trusted environments (local terminal or Colab). Do **not** expose it in any public frontend.

---

## 3. Environment variables

Copy `.env.example` to `.env` and fill in values:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Then edit `.env`:

- `SUPABASE_URL=https://your-project-id.supabase.co`
- `SUPABASE_SERVICE_KEY=your-service-role-key`
- (Optional) tune rate limiting: `TIKI_MAX_CONCURRENT_REQUESTS`, `TIKI_BASE_DELAY_SECONDS`, etc.
- (Optional) control which stages run: `TIKI_RUN_MODE` (see below).

You can also export env vars directly in your shell instead of using `.env`.

---

## 4. Installing Python dependencies

From the project root:

```bash
pip install -r requirements.txt
```

On Windows PowerShell (once Python is installed and on PATH):

```powershell
cd "C:\Users\dohai\PycharmProjects\TikiWebScraping2"
python -m pip install -r requirements.txt
```

If you prefer a virtual environment, create and activate it before running the install command.

---

## 5. Running the pipeline locally

Ensure environment variables are set (either via `.env` loaded by your IDE or directly in the shell).

### 5.1 Run modes

The orchestrator supports several **run modes** controlled by `TIKI_RUN_MODE`:

- `full` (default):
  - Categories → listings/products → product detail enrichment (including seller widget enrichment) → reviews.
- `categories_only`:
  - Only refresh categories under the configured parent (default `8273`), then stop.
- `listings_only`:
  - Refresh categories and product listings; upsert basic products and sellers, but **skip** product detail enrichment and reviews.
- `products_pipeline`:
  - Categories → listings/products → product detail enrichment (including seller widget enrichment), but **skip** reviews.
- `enrich_only`:
  - Reuse existing products from the database and only re-enrich product details and sellers (no new listings, no reviews).
- `reviews_only`:
  - Reuse existing products from the database and only refresh reviews for those products.
- `sellers_only`:
  - Reuse existing seller IDs from the `seller` table and refresh them via the seller widget API only (no product or review changes).

### 5.2 Example commands (Windows PowerShell)

- Full pipeline:

  ```powershell
  $env:TIKI_RUN_MODE="full"
  python -m src.pipeline.orchestrator
  ```

- Categories only:

  ```powershell
  $env:TIKI_RUN_MODE="categories_only"
  python -m src.pipeline.orchestrator
  ```

- Listings + products + seller enrichment (no reviews):

  ```powershell
  $env:TIKI_RUN_MODE="products_pipeline"
  python -m src.pipeline.orchestrator
  ```

- Listings only (no enrichment, no reviews):

  ```powershell
  $env:TIKI_RUN_MODE="listings_only"
  python -m src.pipeline.orchestrator
  ```

- Enrich products and sellers only:

  ```powershell
  $env:TIKI_RUN_MODE="enrich_only"
  python -m src.pipeline.orchestrator
  ```

- Reviews only:

  ```powershell
  $env:TIKI_RUN_MODE="reviews_only"
  python -m src.pipeline.orchestrator
  ```

- Sellers only (widget-based enrichment for existing sellers):

  ```powershell
  $env:TIKI_RUN_MODE="sellers_only"
  python -m src.pipeline.orchestrator
  ```

---

## 6. Running on Google Colab

### 6.1 Regular Colab notebook (in browser)

In a new Colab notebook cell:

```python
!git clone YOUR_REPO_URL
%cd TikiWebScraping2
!pip install -r requirements.txt

import os
os.environ["SUPABASE_URL"] = "https://your-project-id.supabase.co"
os.environ["SUPABASE_SERVICE_KEY"] = "your-service-role-key"

!python -m src.pipeline.orchestrator
```

Replace `YOUR_REPO_URL` and Supabase credentials with your values.

### 6.2 Colab extension inside VS Code

With the Colab extension already installed in VS Code:

1. Open this project folder in VS Code.
2. Use the Colab extension command (e.g. **Open in Colab** or similar, depending on extension version) to create or connect to a Colab-backed notebook.
3. In the Colab-backed notebook within VS Code, run the same commands as above:

```python
!git clone YOUR_REPO_URL
%cd TikiWebScraping2
!pip install -r requirements.txt

import os
os.environ["SUPABASE_URL"] = "https://your-project-id.supabase.co"
os.environ["SUPABASE_SERVICE_KEY"] = "your-service-role-key"

!python -m src.pipeline.orchestrator
```

You’re still executing on Colab’s cloud runtime, but authoring from VS Code.

---

## 7. Smoke test module

There is a simple smoke test at `tests/test_pipeline_smoke.py` that:

- Verifies Supabase environment variables are available.
- Instantiates a Supabase client.
- Optionally runs a very small subset of the pipeline (e.g. just category sync) to ensure connectivity.

To run tests (once Python is installed and `pytest` is available):

```bash
pytest -q
```

On Windows PowerShell:

```powershell
pytest -q
```

If `pytest` is not installed, you can install it with `pip install pytest` or run the test module directly with `python -m tests.test_pipeline_smoke`.

---

## 8. Next steps and extensions

- Add more sophisticated incremental logic (e.g. only re-scrape reviews for products where counts have changed).
- Add data cleaning and normalization steps on top of the raw tables.
- Connect Power BI or other BI tools directly to Supabase for visualization.
- Enable `pgvector` in Supabase for LLM-based analytics over review text.
