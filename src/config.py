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


# Parent category controls the entry point for discovery. Default keeps the
# historic milk category (8273) but can be overridden per run via env var.
DEFAULT_PARENT_CATEGORY_ID = int(os.getenv("TIKI_PARENT_CATEGORY_ID", "8273"))

BASE_DELAY_SECONDS = float(os.getenv("TIKI_BASE_DELAY_SECONDS", "1.0"))
JITTER_RANGE = float(os.getenv("TIKI_JITTER_RANGE", "0.5"))

MAX_PAGES_PER_CATEGORY = int(os.getenv("TIKI_MAX_PAGES_PER_CATEGORY", "500"))
MAX_REVIEW_PAGES_PER_PRODUCT = int(os.getenv("TIKI_MAX_REVIEW_PAGES_PER_PRODUCT", "500"))


SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
