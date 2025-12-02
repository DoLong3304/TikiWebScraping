import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # Optional: if python-dotenv is not installed, just skip
    pass

TIKI_BASE_URL = "https://tiki.vn"

# Category endpoints
TIKI_CATEGORY_URL = f"{TIKI_BASE_URL}/api/v2/categories"
TIKI_LISTING_URL = f"{TIKI_BASE_URL}/api/personalish/v1/blocks/listings"
TIKI_PRODUCT_URL = f"{TIKI_BASE_URL}/api/v2/products"
TIKI_REVIEW_URL = f"{TIKI_BASE_URL}/api/v2/reviews"
TIKI_SELLER_URL = "https://api.tiki.vn/product-detail/v2/widgets/seller"


DEFAULT_PARENT_CATEGORY_ID = 8273  # Sữa nước parent

MAX_CONCURRENT_REQUESTS = int(os.getenv("TIKI_MAX_CONCURRENT_REQUESTS", "3"))
BASE_DELAY_SECONDS = float(os.getenv("TIKI_BASE_DELAY_SECONDS", "1.0"))
JITTER_RANGE = float(os.getenv("TIKI_JITTER_RANGE", "0.5"))
RETRY_MAX_ATTEMPTS = int(os.getenv("TIKI_RETRY_MAX_ATTEMPTS", "4"))

MAX_PAGES_PER_CATEGORY = int(os.getenv("TIKI_MAX_PAGES_PER_CATEGORY", "500"))
MAX_REVIEW_PAGES_PER_PRODUCT = int(os.getenv("TIKI_MAX_REVIEW_PAGES_PER_PRODUCT", "500"))


SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# Orchestration run mode:
# "full", "categories_only", "listings_only",
# "products_pipeline", "enrich_only", "reviews_only",
# "sellers_only"
RUN_MODE = os.getenv("TIKI_RUN_MODE", "full")
