-- Cleaned analytics schema for Tiki data
--
-- This schema is designed to be populated from the existing `public` tables
-- created by this project (`category`, `product`, `seller`, `review`).
-- It avoids JSON where possible and keeps only analysis-ready fields for
-- visualization, insight analysis, machine learning, and sentiment work.

create schema if not exists cleaned;

-- =====================
-- DIMENSIONS
-- =====================

-- Category dimension: stable category hierarchy and metadata
create table if not exists cleaned.dim_category (
    category_sk         bigserial primary key,
    category_id         bigint      not null unique, -- public.category.id
    parent_category_id  bigint,                      -- public.category.parent_id
    parent_category_sk  bigint references cleaned.dim_category(category_sk),
    name                text        not null,
    level               smallint,
    url_key             text,
    url_path            text,
    status              text,
    include_in_menu     boolean,
    is_leaf             boolean,
    product_count       integer,
    meta_title          text,
    meta_description    text,
    thumbnail_url       text,
    created_at          timestamptz default now()
);

create index if not exists idx_dim_category_parent_id
    on cleaned.dim_category(parent_category_id);

create index if not exists idx_dim_category_is_leaf
    on cleaned.dim_category(is_leaf);


-- Seller dimension: stable seller attributes and quality signals
create table if not exists cleaned.dim_seller (
    seller_sk        bigserial primary key,
    seller_id        bigint      not null unique, -- public.seller.id
    name             text        not null,
    seller_type      text,
    is_official      boolean,
    store_id         integer,
    store_level      text,
    profile_url      text,
    icon_url         text,
    days_since_joined integer,
    total_follower   bigint,
    rating           numeric(3,2),
    avg_rating_point numeric(4,3),
    review_count     bigint,
    created_at       timestamptz default now()
);

create index if not exists idx_dim_seller_name
    on cleaned.dim_seller(name);


-- Product dimension: core product identity and relatively static attributes
create table if not exists cleaned.dim_product (
    product_sk              bigserial primary key,
    product_id              bigint      not null unique, -- public.product.id
    category_sk             bigint      not null references cleaned.dim_category(category_sk),
    seller_sk               bigint references cleaned.dim_seller(seller_sk),
    master_id               bigint,
    sku                     text,
    name                    text        not null,
    brand_id                bigint,
    brand_name              text,
    brand_slug              text,
    brand_country           text,  -- from specifications where available
    origin                  text,  -- from specifications
    expiry_time             text,  -- from specifications
    is_warranty_applied     boolean,
    is_baby_milk            boolean,
    is_acoholic_drink       boolean,
    is_fresh                boolean,
    -- Additional attributes derived from specifications
    capacity_raw            text,        -- raw capacity string (e.g. "48x110ml", "180ml")
    unit_volume_ml          numeric(10,2), -- parsed approximate volume per unit in ml
    product_weight_raw      text,        -- raw weight string
    unit_weight_g           numeric(10,2), -- parsed approximate weight per unit in grams
    suitable_age_raw        text,        -- raw age string from suitable_age_for_use
    min_age_years           numeric(4,1), -- derived minimum age in years when possible
    age_segment             text,        -- categorized age segment (e.g. kids_1_3, family, unspecified)
    is_organic              boolean,     -- from Organic = 'Có'
    regional_specialties    text,        -- e.g. Miền Nam
    organization_name       text,        -- responsible organization name
    organization_address    text,        -- responsible organization address
    thumbnail_url           text,
    tiki_url                text,
    product_first_seen_at   timestamptz,
    product_last_updated_at timestamptz,
    created_at              timestamptz default now()
);

create index if not exists idx_dim_product_category_sk
    on cleaned.dim_product(category_sk);

create index if not exists idx_dim_product_seller_sk
    on cleaned.dim_product(seller_sk);

create index if not exists idx_dim_product_brand_name
    on cleaned.dim_product(brand_name);


-- Date dimension for consistent time-based analysis
create table if not exists cleaned.dim_date (
    date_sk     integer primary key, -- e.g. 20251216
    date        date    not null unique,
    year        integer,
    quarter     smallint,
    month       smallint,
    day         smallint,
    day_of_week smallint,
    is_weekend  boolean
);

create unique index if not exists idx_dim_date_date
    on cleaned.dim_date(date);


-- =====================
-- INGREDIENTS / THANH_PHAN
-- =====================

-- Store thanh_phan (ingredients / nutrition text) separately to keep dim_product lean
create table if not exists cleaned.product_ingredients (
    product_ingredient_sk  bigserial primary key,
    product_sk             bigint not null references cleaned.dim_product(product_sk) on delete cascade,
    source_code            text   not null default 'thanh_phan', -- e.g. 'thanh_phan', 'thanh_phan_manual'
    ingredient_text_raw    text   not null,       -- original value from specifications (may include HTML)
    ingredient_text_clean  text,                  -- optional cleaned/plain-text version
    language               text,                  -- optional language code if detected
    loaded_at              timestamptz default now(),
    unique (product_sk, source_code)
);

create index if not exists idx_product_ingredients_product
    on cleaned.product_ingredients(product_sk);


-- =====================
-- FACT TABLES (TIME SERIES)
-- =====================

-- Daily product-level metrics built from public.product and joins to dims
create table if not exists cleaned.fact_product_daily (
    product_daily_sk                   bigserial primary key,
    date_sk                            integer not null references cleaned.dim_date(date_sk),
    product_sk                         bigint  not null references cleaned.dim_product(product_sk),
    category_sk                        bigint  not null references cleaned.dim_category(category_sk),
    seller_sk                          bigint references cleaned.dim_seller(seller_sk),
    price                              numeric(12,2),
    list_price                         numeric(12,2),
    original_price                     numeric(12,2),
    discount                           numeric(12,2),
    discount_rate                      numeric(5,2),
    rating_average                     numeric(3,2),
    review_count_cumulative            integer,
    all_time_quantity_sold_cumulative  integer,
    price_vs_list_percent              numeric(6,2), -- (list_price - price)/list_price*100
    snapshot_at                        timestamptz not null
);

create unique index if not exists ux_fact_product_daily_product_date
    on cleaned.fact_product_daily(product_sk, date_sk);

create index if not exists idx_fact_product_daily_date_sk
    on cleaned.fact_product_daily(date_sk);

create index if not exists idx_fact_product_daily_category_sk
    on cleaned.fact_product_daily(category_sk);

create index if not exists idx_fact_product_daily_seller_sk
    on cleaned.fact_product_daily(seller_sk);


-- Daily seller-level metrics built from public.seller
create table if not exists cleaned.fact_seller_daily (
    seller_daily_sk           bigserial primary key,
    date_sk                   integer not null references cleaned.dim_date(date_sk),
    seller_sk                 bigint  not null references cleaned.dim_seller(seller_sk),
    rating                    numeric(3,2),
    avg_rating_point          numeric(4,3),
    review_count_cumulative   bigint,
    total_follower_cumulative bigint,
    days_since_joined         integer,
    days_active               integer,
    snapshot_at               timestamptz not null
);

create unique index if not exists ux_fact_seller_daily_seller_date
    on cleaned.fact_seller_daily(seller_sk, date_sk);

create index if not exists idx_fact_seller_daily_date_sk
    on cleaned.fact_seller_daily(date_sk);


-- Listing/impression context from listing API, joined to products/sellers
-- This table is optional but highly useful for understanding exposure and
-- marketing context. It only keeps fields that can be stably derived from
-- listing responses and which are relevant for analysis.
create table if not exists cleaned.fact_listing_impression_daily (
    listing_impression_daily_sk   bigserial primary key,
    date_sk                       integer not null references cleaned.dim_date(date_sk),
    product_sk                    bigint  not null references cleaned.dim_product(product_sk),
    seller_sk                     bigint references cleaned.dim_seller(seller_sk),
    list_page                     integer,    -- page number in listing
    position                      integer,    -- position on the page
    price_card                    numeric(12,2),
    discount_rate_card            numeric(5,2),
    quantity_sold_card            integer,
    rating_card                   numeric(3,2),
    number_of_reviews_card        integer,
    delivery_date_estimate        date,
    delivery_zone                 text,
    standard_delivery_estimate_days numeric(6,2),
    tikinow_delivery_estimate_days  numeric(6,2),
    is_ad                         boolean,
    is_authentic                  boolean,
    is_flash_deal                 boolean,
    is_freeship_xtra              boolean,
    is_top_brand                  boolean,
    tiki_verified                 boolean,
    layout                        text,
    order_route                   text,
    origin_card                   text,
    search_rank                   integer,
    snapshot_at                   timestamptz not null
);

create index if not exists idx_fact_listing_impression_daily_date
    on cleaned.fact_listing_impression_daily(date_sk);

create index if not exists idx_fact_listing_impression_daily_product
    on cleaned.fact_listing_impression_daily(product_sk);


-- =====================
-- REVIEWS AND SENTIMENT
-- =====================

-- Cleaned review-level table built from public.review and product/seller dims.
-- Keeps only fields that are directly useful for analysis and ML.
create table if not exists cleaned.review_clean (
    review_sk          bigserial primary key,
    review_id          bigint      not null unique,  -- public.review.id
    product_sk         bigint      not null references cleaned.dim_product(product_sk),
    seller_sk          bigint references cleaned.dim_seller(seller_sk),
    customer_id_hash   text,  -- hash of customer_id from source
    rating             smallint   not null,
    created_at         timestamptz not null,
    purchased          boolean,
    purchased_at       timestamptz,
    thank_count        integer,
    comment_count      integer,
    title              text,
    content            text,
    content_length     integer,
    word_count         integer,
    has_images         boolean,
    image_count        integer,
    days_used_at_review integer,      -- derived from delivery vs review dates when available
    delivery_date      timestamptz,
    delivery_time_hours numeric(8,2), -- approximate delivery time
    delivery_time_rating        text, -- e.g. "Giao đúng hẹn"
    shipper_attitude_rating     text, -- e.g. "Lịch sự"
    delivery_time_slot_rating   text, -- e.g. "Có hẹn giờ trước"
    packing_quality_rating      text, -- e.g. "Cẩn thận"
    customer_total_review       integer,
    customer_total_thank        integer,
    loaded_at          timestamptz default now()
);

create index if not exists idx_review_clean_product_sk
    on cleaned.review_clean(product_sk);

create index if not exists idx_review_clean_seller_sk
    on cleaned.review_clean(seller_sk);

create index if not exists idx_review_clean_created_at
    on cleaned.review_clean(created_at);

create index if not exists idx_review_clean_product_rating
    on cleaned.review_clean(product_sk, rating);


-- Daily per-product review aggregates built from review_clean
create table if not exists cleaned.fact_product_review_agg_daily (
    product_review_agg_daily_sk bigserial primary key,
    date_sk                     integer not null references cleaned.dim_date(date_sk),
    product_sk                  bigint  not null references cleaned.dim_product(product_sk),
    review_count                integer not null,
    avg_rating                  numeric(3,2) not null,
    rating_1_count              integer not null default 0,
    rating_2_count              integer not null default 0,
    rating_3_count              integer not null default 0,
    rating_4_count              integer not null default 0,
    rating_5_count              integer not null default 0,
    thank_count_sum             integer,
    comment_count_sum           integer,
    purchased_review_count      integer,
    non_purchased_review_count  integer,
    rating_stddev               numeric(4,3),
    last_aggregated_at          timestamptz not null
);

create unique index if not exists ux_fact_product_review_agg_daily_product_date
    on cleaned.fact_product_review_agg_daily(product_sk, date_sk);

create index if not exists idx_fact_product_review_agg_daily_date
    on cleaned.fact_product_review_agg_daily(date_sk);


-- Overall review summary per product (current cumulative snapshot)
create table if not exists cleaned.fact_product_review_summary (
    product_review_summary_sk bigserial primary key,
    product_sk                bigint not null unique references cleaned.dim_product(product_sk),
    rating_average            numeric(3,2) not null,
    reviews_count             integer      not null,
    star_1_count              integer      not null,
    star_2_count              integer      not null,
    star_3_count              integer      not null,
    star_4_count              integer      not null,
    star_5_count              integer      not null,
    star_1_percent            numeric(5,2),
    star_2_percent            numeric(5,2),
    star_3_percent            numeric(5,2),
    star_4_percent            numeric(5,2),
    star_5_percent            numeric(5,2),
    snapshot_at               timestamptz not null
);


-- =====================
-- FEATURE TABLES (ML-READY)
-- =====================

-- Daily product feature table assembled from the above facts and dims.
-- This is intentionally narrow to start and can be extended as new
-- experiments are added.
create table if not exists cleaned.feature_product_daily (
    product_feature_daily_sk        bigserial primary key,
    date_sk                         integer not null references cleaned.dim_date(date_sk),
    product_sk                      bigint  not null references cleaned.dim_product(product_sk),
    category_sk                     bigint references cleaned.dim_category(category_sk),
    seller_sk                       bigint references cleaned.dim_seller(seller_sk),
    price                           numeric(12,2),
    list_price                      numeric(12,2),
    discount_rate                   numeric(5,2),
    price_change_1d                 numeric(12,2),
    price_change_7d                 numeric(12,2),
    discount_rate_7d_avg            numeric(5,2),
    review_count_cumulative         integer,
    new_reviews_1d                  integer,
    new_reviews_7d                  integer,
    all_time_quantity_sold_cumulative integer,
    sold_1d                         integer,
    sold_7d                         integer,
    sentiment_score_avg_7d          numeric(4,3),
    negative_review_share_7d        numeric(4,3),
    seller_rating                   numeric(3,2),
    seller_total_follower_cumulative bigint,
    category_level                  smallint,
    category_is_leaf                boolean,
    target_label                    numeric,      -- optional target (e.g., future sales)
    features_generated_at           timestamptz not null
);

create unique index if not exists ux_feature_product_daily_product_date
    on cleaned.feature_product_daily(product_sk, date_sk);

create index if not exists idx_feature_product_daily_date
    on cleaned.feature_product_daily(date_sk);

create index if not exists idx_feature_product_daily_category
    on cleaned.feature_product_daily(category_sk);

create index if not exists idx_feature_product_daily_seller
    on cleaned.feature_product_daily(seller_sk);

