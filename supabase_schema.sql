-- Supabase/Postgres schema for Tiki milk (and generic) products

create table if not exists public.category (
    id              bigint primary key,
    parent_id       bigint references public.category(id) on delete set null,
    name            text not null,
    level           smallint,
    url_key         text,
    url_path        text,
    status          text,
    include_in_menu boolean,
    product_count   integer,
    is_leaf         boolean,
    meta_title      text,
    meta_description text,
    thumbnail_url   text
);

create index if not exists idx_category_parent_id on public.category(parent_id);
create index if not exists idx_category_is_leaf on public.category(is_leaf);

create table if not exists public.seller (
    id                bigint primary key,
    name              text not null,
    seller_type       text,
    is_official       boolean,
    rating            numeric(3,2),
    avg_rating_point  numeric(4,3),
    review_count      bigint,
    total_follower    bigint,
    store_id          integer,
    store_level       text,
    days_since_joined integer,
    icon_url          text,
    profile_url       text,
    badge_img         jsonb,
    info              jsonb,
    created_at        timestamptz default now()
);

create index if not exists idx_seller_name on public.seller using gin (to_tsvector('simple', name));

create table if not exists public.product (
    id               bigint primary key,
    master_id        bigint,
    sku              text,
    name             text not null,
    brand            text,
    brand_id         bigint,
    category_id      bigint references public.category(id),
    price            numeric(12,2),
    list_price       numeric(12,2),
    original_price   numeric(12,2),
    discount         numeric(12,2),
    discount_rate    numeric(5,2),
    rating_average   numeric(3,2),
    review_count     integer,
    all_time_quantity_sold integer,
    thumbnail_url    text,
    tiki_url         text,
    seller_id        bigint references public.seller(id),
    specifications   jsonb,
    badges           jsonb,
    badges_new       jsonb,
    badges_v3        jsonb,
    highlight        jsonb,
    extra            jsonb,
    created_at       timestamptz default now(),
    updated_at       timestamptz default now()
);

create index if not exists idx_product_category_id on public.product(category_id);
create index if not exists idx_product_seller_id on public.product(seller_id);
create index if not exists idx_product_brand on public.product(brand);

create table if not exists public.product_category (
    product_id  bigint references public.product(id) on delete cascade,
    category_id bigint references public.category(id) on delete cascade,
    primary key (product_id, category_id)
);

create table if not exists public.review (
    id             bigint primary key,
    product_id     bigint references public.product(id) on delete cascade,
    customer_id    bigint,
    title          text,
    content        text,
    rating         smallint,
    thank_count    integer,
    comment_count  integer,
    created_at     timestamptz,
    purchased      boolean,
    purchased_at   timestamptz,
    attributes     jsonb,
    suggestions    jsonb,
    seller_id      bigint,
    extra          jsonb
);

create index if not exists idx_review_product_id on public.review(product_id);
create index if not exists idx_review_rating on public.review(rating);
create index if not exists idx_review_created_at on public.review(created_at);

