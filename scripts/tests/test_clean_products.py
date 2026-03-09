from __future__ import annotations

from scripts.clean_products import (
    canonical_product_url,
    clean_products,
    dedupe_records,
    extract_memory_specs,
    infer_rating_from_text,
    infer_review_count_from_text,
    is_placeholder_value,
    parse_price_inr,
)


def test_canonical_product_url_removes_tracking_params() -> None:
    source = (
        "https://www.flipkart.com/apple-macbook-neo-a18-pro-2026-pro-8-gb-256-gb-ssd-tahoe-mhfa4hn-a/"
        "p/itm9fce39e65bd7e?pid=COMHH8C57Y6W6NZU&lid=LSTCOMHH8C57Y6W6NZUASDOLA"
        "&marketplace=FLIPKART&q=macbook+neo"
    )
    assert (
        canonical_product_url(source)
        == "https://www.flipkart.com/apple-macbook-neo-a18-pro-2026-pro-8-gb-256-gb-ssd-tahoe-mhfa4hn-a/p/itm9fce39e65bd7e"
    )


def test_parse_price_inr_parses_currency_string() -> None:
    assert parse_price_inr("₹1,79,900") == 179900
    assert parse_price_inr("Rs. 69,900") == 69900


def test_extract_memory_specs_detects_ram_and_storage() -> None:
    title = "Apple MacBook Neo A18 Pro (8 GB/256 GB SSD)"
    ram, storage = extract_memory_specs(title)
    assert ram == "8 GB"
    assert storage == "256 GB SSD"


def test_extract_memory_specs_from_hyphenated_slug() -> None:
    slug = "apple-macbook-neo-a18-pro-2026-pro-8-gb-512-gb-ssd-tahoe-mhfe4hn-a"
    ram, storage = extract_memory_specs(slug)
    assert ram == "8 GB"
    assert storage == "512 GB SSD"


def test_placeholder_detection() -> None:
    assert is_placeholder_value("Unknown") is True
    assert is_placeholder_value("") is True
    assert is_placeholder_value("Apple") is False


def test_deduplication_uses_processor_as_identity_part() -> None:
    rows = [
        {
            "brand": "Apple",
            "model": "MacBook Neo",
            "ram": "8 GB",
            "storage": "256 GB SSD",
            "processor": "A18 Pro",
            "price_inr": 69900,
            "product_url": "https://example.com/p/1",
            "category": "laptop",
            "variant": None,
            "display": None,
            "os": "macOS",
            "rating": None,
            "review_count": None,
            "availability": None,
        },
        {
            "brand": "Apple",
            "model": "MacBook Neo",
            "ram": "8 GB",
            "storage": "256 GB SSD",
            "processor": "A18 Pro",
            "price_inr": 69900,
            "product_url": "https://example.com/p/1?x=1",
            "category": "laptop",
            "variant": None,
            "display": None,
            "os": "macOS",
            "rating": None,
            "review_count": None,
            "availability": None,
        },
        {
            "brand": "Apple",
            "model": "MacBook Neo",
            "ram": "8 GB",
            "storage": "256 GB SSD",
            "processor": "M4",
            "price_inr": 79900,
            "product_url": "https://example.com/p/2",
            "category": "laptop",
            "variant": None,
            "display": None,
            "os": "macOS",
            "rating": None,
            "review_count": None,
            "availability": None,
        },
    ]

    deduped = dedupe_records(rows)
    assert len(deduped) == 2


def test_clean_products_collapses_flipkart_variant_duplicates() -> None:
    source = [
        {
            "title": "Apple Macbook Neo A18 Pro(2026)",
            "price": "₹69,900",
            "product_url": "https://www.flipkart.com/apple-macbook-neo-a18-pro-2026-pro-8-gb-256-gb-ssd-tahoe-mhfa4hn-a/p/itm9fce39e65bd7e?pid=COMHH8C57Y6W6NZU&srno=s_1_1",
        },
        {
            "title": "Trending Pre Order Apple Macbook Neo A18 Pro(2026)",
            "price": "₹79,900",
            "product_url": "https://www.flipkart.com/apple-macbook-neo-a18-pro-2026-pro-8-gb-512-gb-ssd-tahoe-mhfe4hn-a/p/itm97a16be54dbaf?pid=COMHH8C5SFCGJFGD&srno=s_1_2",
        },
        {
            "title": "Pre Order Apple Macbook Neo A18 Pro(2026)",
            "price": "₹79,900",
            "product_url": "https://www.flipkart.com/apple-macbook-neo-a18-pro-2026-pro-8-gb-512-gb-ssd-tahoe-mhfg4hn-a/p/itmf87d0895be647?pid=COMHH8C5HWN68S3T&srno=s_1_3",
        },
        {
            "title": "Pre Order Apple Macbook Neo A18 Pro(2026)",
            "price": "₹69,900",
            "product_url": "https://www.flipkart.com/apple-macbook-neo-a18-pro-2026-pro-8-gb-256-gb-ssd-tahoe-mhff4hn-a/p/itm8c46a289d349a?pid=COMHH8C5GRDQYNCY&srno=s_1_4",
        },
        {
            "title": "Pre Order Apple Macbook Neo A18 Pro(2026)",
            "price": "₹79,900",
            "product_url": "https://www.flipkart.com/apple-macbook-neo-a18-pro-2026-pro-8-gb-512-gb-ssd-tahoe-mhfc4hn-a/p/itm810de98c2fd1b?pid=COMHH8C5FSDNHCMN&srno=s_1_5",
        },
    ]

    cleaned = clean_products(source)
    assert len(cleaned) == 2
    signatures = {(row["ram"], row["storage"], row["price_inr"]) for row in cleaned}
    assert signatures == {
        ("8 GB", "256 GB SSD", 69900),
        ("8 GB", "512 GB SSD", 79900),
    }


def test_infer_rating_and_review_from_text() -> None:
    text = (
        "Apple iPhone 15 Pro Max 4.6 2,835 Ratings & 195 Reviews "
        "512 GB ROM 17.02 cm (6.7 inch) Super Retina XDR Display"
    )
    assert infer_rating_from_text(text) == 4.6
    assert infer_review_count_from_text(text) == 195
