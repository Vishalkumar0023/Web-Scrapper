from app.services.extractor import infer_fields, parse_rows_from_html, transform_rows_for_prompt_schema


COMPLEX_LISTING_HTML = """
<html>
  <body>
    <nav>
      <ul>
        <li><a href="/menu/deals">Shop by Category and Offers</a></li>
        <li><a href="/menu/services">Services and Support Center</a></li>
        <li><a href="/menu/account">Account and Profile Settings</a></li>
        <li><a href="/menu/about">About and Contact Information</a></li>
      </ul>
    </nav>

    <section id="results">
      <div class="cards">
        <div class="product-card">
          <a href="/product/alpha"><span class="title">Wireless Mouse Alpha</span></a>
          <div class="price-wrap"><span class="price">$19.99</span></div>
          <span class="rating">4.6 out of 5 stars</span>
        </div>
        <div class="product-card">
          <a href="/product/beta"><span class="title">Wireless Mouse Beta</span></a>
          <div class="price-wrap"><span class="price">$24.99</span></div>
          <span class="rating">4.3 out of 5 stars</span>
        </div>
        <div class="product-card">
          <a href="/product/gamma"><span class="title">Wireless Mouse Gamma</span></a>
          <div class="price-wrap"><span class="price">$29.99</span></div>
          <span class="rating">4.8 out of 5 stars</span>
        </div>
      </div>
    </section>
  </body>
</html>
"""


NESTED_CARDS_HTML = """
<html>
  <body>
    <ul class="catalog">
      <li class="entry">
        <div class="content">
          <a href="/p/1"><span class="name">Noise Cancelling Headphones</span></a>
          <div class="meta">
            <span class="amount">₹2,499</span>
            <span class="score">4.3/5</span>
          </div>
        </div>
      </li>
      <li class="entry">
        <div class="content">
          <a href="/p/2"><span class="name">Mechanical Keyboard Pro</span></a>
          <div class="meta">
            <span class="amount">₹5,999</span>
            <span class="score">4.7/5</span>
          </div>
        </div>
      </li>
    </ul>
  </body>
</html>
"""

PRICE_CODE_HTML = """
<html>
  <body>
    <div class="list">
      <article class="item">
        <a href="/sku/1001"><h3>Premium Monitor 27</h3></a>
        <span class="price">USD 1,299.00</span>
        <span class="rating">4,9/5</span>
      </article>
      <article class="item">
        <a href="/sku/1002"><h3>Premium Monitor 32</h3></a>
        <span class="price">USD 1,599.00</span>
        <span class="rating">4,7/5</span>
      </article>
    </div>
  </body>
</html>
"""

NOISY_MARKETPLACE_HTML = """
<html>
  <body>
    <section class="results">
      <div class="product-card">
        <a href="https://example.com/p1">
          Add to Compare Apple iPhone 15 Pro Max (White Titanium, 512 GB) 4.6 2,835 Ratings & 195 Reviews 512 GB ROM 17.02 cm (6.7 inch) Super Retina XDR Display
        </a>
        <span class="price">₹1,79,900</span>
      </div>
      <div class="product-card">
        <a href="https://example.com/p2">
          Currently unavailable Add to Compare Apple iPhone 16 Pro Max (Black Titanium, 256 GB) 4.7 17,752 Ratings & 913 Reviews 256 GB ROM 17.53 cm (6.9 inch) Super Retina XDR Display
        </a>
        <span class="price">₹1,34,900</span>
      </div>
    </section>
  </body>
</html>
"""

HETEROGENEOUS_CARD_STRUCTURE_HTML = """
<html>
  <body>
    <section class="results">
      <div class="product-card variant-a">
        <a href="/p/1"><h2>Apple iPhone 15 Pro Max (White Titanium, 512 GB)</h2></a>
        <span class="price">₹1,79,900</span>
        <span class="rating">4.6 2,835 Ratings</span>
      </div>
      <div class="product-card variant-b">
        <div class="badges"><span class="promo">Top Choice</span></div>
        <a href="/p/2"><h2>Apple iPhone 15 Pro Max (Natural Titanium, 1 TB)</h2></a>
        <span class="price">₹1,99,900</span>
        <span class="rating">4.6 2,835 Ratings</span>
      </div>
      <div class="product-card variant-a">
        <a href="/p/3"><h2>Apple iPhone 15 Pro Max (Black Titanium, 512 GB)</h2></a>
        <span class="price">₹1,79,900</span>
        <span class="rating">4.6 2,835 Ratings</span>
      </div>
      <div class="product-card variant-b">
        <div class="availability">Currently unavailable</div>
        <a href="/p/4"><h2>Apple iPhone 15 Pro Max (Black Titanium, 1 TB)</h2></a>
        <span class="price">₹1,99,900</span>
        <span class="rating">4.6 2,835 Ratings</span>
      </div>
      <div class="product-card variant-a">
        <a href="/p/5"><h2>Apple iPhone 15 Pro Max (White Titanium, 1 TB)</h2></a>
        <span class="price">₹1,99,900</span>
        <span class="rating">4.6 2,835 Ratings</span>
      </div>
      <div class="product-card variant-b">
        <div class="availability">Coming Soon</div>
        <a href="/p/6"><h2>Apple iPhone 15 Pro Max (Blue Titanium, 512 GB)</h2></a>
        <span class="price">₹1,79,900</span>
        <span class="rating">4.6 2,835 Ratings</span>
      </div>
    </section>
  </body>
</html>
"""

FLIPKART_MACBOOK_NOISY_HTML = """
<html>
  <body>
    <div class="results">
      <a href="https://www.flipkart.com/computers/pr?sid=6bo&marketplace=FLIPKART">Computers</a>
      <div class="product-card">
        <a href="https://www.flipkart.com/apple-macbook-neo-a18-pro-2026-pro-8-gb-256-gb-ssd-tahoe-mhfa4hn-a/p/itm9fce39e65bd7e?pid=COMHH8C57Y6W6NZU">
          Pre Order Apple Macbook Neo A18 Pro(2026) Pro 8 GB/256 GB SSD/Tahoe
        </a>
        <span class="price">₹69,900</span>
      </div>
      <div class="product-card">
        <a href="https://www.flipkart.com/apple-macbook-neo-a18-pro-2026-pro-8-gb-512-gb-ssd-tahoe-mhfe4hn-a/p/itm97a16be54dbaf?pid=COMHH8C5SFCGJFGD">
          Trending Pre Order Apple Macbook Neo A18 Pro(2026) Pro 8 GB/512 GB SSD/Tahoe
        </a>
        <span class="price">₹79,900</span>
      </div>
      <div class="product-card">
        <a href="https://www.flipkart.com/apple-macbook-neo-a18-pro-2026-pro-8-gb-512-gb-ssd-tahoe-mhfg4hn-a/p/itmf87d0895be647?pid=COMHH8C5HWN68S3T">
          Pre Order Apple Macbook Neo A18 Pro(2026) Pro 8 GB/512 GB SSD/Tahoe
        </a>
        <span class="price">₹79,900</span>
      </div>
      <div class="product-card">
        <a href="https://www.flipkart.com/apple-macbook-neo-a18-pro-2026-pro-8-gb-256-gb-ssd-tahoe-mhff4hn-a/p/itm8c46a289d349a?pid=COMHH8C5GRDQYNCY">
          Pre Order Apple Macbook Neo A18 Pro(2026) Pro 8 GB/256 GB SSD/Tahoe
        </a>
        <span class="price">₹69,900</span>
      </div>
    </div>
  </body>
</html>
"""

LAPTOP_DETAILED_METADATA_HTML = """
<html>
  <body>
    <section class="results">
      <div class="product-card">
        <a href="https://www.flipkart.com/apple-macbook-air-m4-24-gb-512-gb-ssd-macos-sequoia-mc6v4hn-a/p/itm482836944277e?pid=COMAAA111">
          Apple MacBook Air M4 - (24 GB/512 GB SSD/macOS Sequoia/33.02 cm (13 inch) Display) 4.8 2 Reviews
        </a>
        <span class="price">₹1,32,990</span>
      </div>
      <div class="product-card">
        <a href="https://www.flipkart.com/motobook-60-pro-full-metal-oled-ai-pc-intel-core-ultra-5-225h-16-gb-512-gb-ssd-windows-11-home/p/itmabc222?pid=COMBBB222">
          Motobook 60 Pro Full Metal OLED AI PC Intel Core Ultra 5 225H - (16 GB/512 GB SSD/Windows 11 Home/35.56 cm (14 inch) Display) 4.5 128 Reviews
        </a>
        <span class="price">₹74,990</span>
      </div>
      <div class="product-card">
        <a href="https://www.flipkart.com/motobook-14-core-5-series-2-210h-16-gb-512-gb-ssd-windows-11/p/itmccc333?pid=COMCCC333">
          Motobook 14 Intel Core 5 Series 2 210H - (16 GB/512 GB SSD/Windows 11/35.56 cm (14 inch) Display)
        </a>
        <span class="price">₹59,990</span>
      </div>
    </section>
  </body>
</html>
"""

MODEL_CLEANUP_REGRESSION_HTML = """
<html>
  <body>
    <section class="results">
      <div class="product-card">
        <a href="https://www.flipkart.com/apple-macbook-air-m3-24-gb-512-gb-ssd-macos-sequoia-mc8m4hn-a/p/itmddd444?pid=COMDDD444">
          Apple MacBook Air M3 - MC8M4HN/A Apple M3 (24 GB/512 GB SSD/macOS Sequoia/34.54 cm (13.6 inch) Display)
        </a>
        <span class="price">₹1,19,990</span>
      </div>
      <div class="product-card">
        <a href="https://www.flipkart.com/samsung-galaxy-book4-metal-intel-core-i5-13th-gen-1335u-np750xgj-kg1in/p/itmeee555?pid=COMEEE555">
          Samsung Galaxy Book4 Metal Intel Core i5 13th Gen 1335U - NP750XGJ-KG1IN (16 GB/512 GB SSD/Windows 11 Home/39.62 cm (15.6 inch) Display)
        </a>
        <span class="price">₹72,990</span>
      </div>
    </section>
  </body>
</html>
"""

APPLE_M5_MODEL_HTML = """
<html>
  <body>
    <section class="results">
      <div class="product-card">
        <a href="https://www.flipkart.com/apple-macbook-air-m5-16-gb-512-gb-ssd-macos-sequoia-mchx4hn-a/p/itmfff666?pid=COMFFF666">
          Apple MacBook Air (M5, 2026) - (16 GB/512 GB SSD/macOS Sequoia/34.54 cm (13.6 inch) Display)
        </a>
        <span class="price">₹1,49,990</span>
      </div>
    </section>
  </body>
</html>
"""

CROSS_MARKETPLACE_RESOLUTION_HTML = """
<html>
  <body>
    <section class="results">
      <div class="product-card">
        <a href="https://www.flipkart.com/apple-macbook-air-m4-24-gb-512-gb-ssd-macos-sequoia-mc6v4hn-a/p/itm482836944277e?pid=COMAAA111">
          Apple MacBook Air M4 - MC6V4HN/A (24 GB/512 GB SSD/macOS Sequoia/33.02 cm (13 inch) Display)
        </a>
        <span class="price">₹1,32,990</span>
      </div>
      <div class="product-card">
        <a href="https://www.amazon.in/apple-macbook-air-13-inch-mc6v4hn-a/dp/B0TEST1234?tag=tracking123">
          Apple MacBook Air Laptop - MC6V4HN/A (24 GB/512 GB SSD/macOS Sequoia/13 inch Display)
        </a>
        <span class="price">₹1,33,990</span>
      </div>
    </section>
  </body>
</html>
"""


def test_parse_rows_prefers_product_cards_over_nav_items() -> None:
    fields = infer_fields("Extract title, price, rating, product URL")
    rows, warnings = parse_rows_from_html(
        html=COMPLEX_LISTING_HTML,
        base_url="https://shop.example.com/search",
        fields=fields,
        max_rows=10,
    )

    assert warnings == []
    assert len(rows) == 3
    assert rows[0]["title"].startswith("Wireless Mouse")
    assert all(str(row["product_url"]).startswith("https://shop.example.com/product/") for row in rows)
    assert all(row["price"] for row in rows)
    assert all(row["rating"] for row in rows)


def test_parse_rows_extracts_nested_prices_and_ratings() -> None:
    fields = infer_fields("Extract title, price, rating, product URL")
    rows, _warnings = parse_rows_from_html(
        html=NESTED_CARDS_HTML,
        base_url="https://electronics.example.com",
        fields=fields,
        max_rows=10,
    )

    assert len(rows) == 2
    assert rows[0]["title"] == "Noise Cancelling Headphones"
    assert rows[0]["price"] == "₹2,499"
    assert rows[0]["rating"] == "4.3"
    assert rows[1]["title"] == "Mechanical Keyboard Pro"
    assert rows[1]["product_url"] == "https://electronics.example.com/p/2"


def test_parse_rows_handles_currency_code_and_comma_rating() -> None:
    fields = infer_fields("Extract title, price, rating, product URL")
    rows, warnings = parse_rows_from_html(
        html=PRICE_CODE_HTML,
        base_url="https://store.example.com",
        fields=fields,
        max_rows=10,
    )

    assert warnings == []
    assert len(rows) == 2
    assert rows[0]["title"] == "Premium Monitor 27"
    assert rows[0]["price"] == "USD 1,299.00"
    assert rows[0]["rating"] == "4.9"


def test_parse_rows_cleans_noisy_title_and_extracts_decimal_rating() -> None:
    fields = infer_fields("Extract title, price, rating, product URL")
    rows, warnings = parse_rows_from_html(
        html=NOISY_MARKETPLACE_HTML,
        base_url="https://example.com/search?q=iphone",
        fields=fields,
        max_rows=10,
    )

    assert warnings == []
    assert len(rows) == 2
    assert rows[0]["title"] == "Apple iPhone 15 Pro Max (White Titanium, 512 GB)"
    assert rows[0]["price"] == "₹1,79,900"
    assert rows[0]["rating"] == "4.6"
    assert rows[0]["product_url"] == "https://example.com/p1"

    assert rows[1]["title"] == "Apple iPhone 16 Pro Max (Black Titanium, 256 GB)"
    assert rows[1]["price"] == "₹1,34,900"
    assert rows[1]["rating"] == "4.7"
    assert rows[1]["product_url"] == "https://example.com/p2"


def test_parse_rows_extracts_review_count_when_requested() -> None:
    fields = infer_fields("Extract title, price, rating, review_count, product URL")
    rows, warnings = parse_rows_from_html(
        html=NOISY_MARKETPLACE_HTML,
        base_url="https://example.com/search?q=iphone",
        fields=fields,
        max_rows=10,
    )

    assert warnings == []
    assert len(rows) == 2
    assert rows[0]["review_count"] == 195
    assert rows[1]["review_count"] == 913


def test_parse_rows_keeps_dom_order_when_card_signatures_vary() -> None:
    fields = infer_fields("Extract title, price, rating, product URL")
    rows, warnings = parse_rows_from_html(
        html=HETEROGENEOUS_CARD_STRUCTURE_HTML,
        base_url="https://example.com/search?q=iphone",
        fields=fields,
        max_rows=20,
    )

    assert warnings == []
    assert len(rows) == 6
    assert rows[0]["title"] == "Apple iPhone 15 Pro Max (White Titanium, 512 GB)"
    assert rows[1]["title"] == "Apple iPhone 15 Pro Max (Natural Titanium, 1 TB)"
    assert rows[2]["title"] == "Apple iPhone 15 Pro Max (Black Titanium, 512 GB)"
    assert rows[3]["title"] == "Apple iPhone 15 Pro Max (Black Titanium, 1 TB)"
    assert rows[4]["title"] == "Apple iPhone 15 Pro Max (White Titanium, 1 TB)"
    assert rows[5]["title"] == "Apple iPhone 15 Pro Max (Blue Titanium, 512 GB)"


def test_parse_rows_extracts_availability_when_requested() -> None:
    fields = infer_fields("Extract title, price, rating, product URL, availability")
    rows, warnings = parse_rows_from_html(
        html=HETEROGENEOUS_CARD_STRUCTURE_HTML,
        base_url="https://example.com/search?q=iphone",
        fields=fields,
        max_rows=20,
    )

    assert warnings == []
    assert len(rows) == 6
    assert rows[0]["availability"] == ""
    assert rows[3]["availability"] == "currently unavailable"
    assert rows[5]["availability"] == "coming soon"


def test_parse_rows_drops_category_row_and_cleans_macbook_title_prefixes() -> None:
    fields = infer_fields("Extract title, price, rating, product URL")
    rows, warnings = parse_rows_from_html(
        html=FLIPKART_MACBOOK_NOISY_HTML,
        base_url="https://www.flipkart.com/search?q=macbook+neo",
        fields=fields,
        max_rows=20,
    )

    assert warnings == []
    assert len(rows) == 4
    assert all(row["title"] == "Apple Macbook Neo A18 Pro(2026)" for row in rows)
    assert all("computers/pr" not in str(row["product_url"]).lower() for row in rows)
    assert rows[0]["price"] == "₹69,900"
    assert rows[1]["price"] == "₹79,900"
    assert rows[2]["price"] == "₹79,900"
    assert rows[3]["price"] == "₹69,900"


def test_transform_rows_for_structured_product_schema() -> None:
    fields = infer_fields("Extract title, price, rating, product URL")
    rows, warnings = parse_rows_from_html(
        html=FLIPKART_MACBOOK_NOISY_HTML,
        base_url="https://www.flipkart.com/search?q=macbook+neo",
        fields=fields,
        max_rows=20,
    )
    assert warnings == []
    assert len(rows) == 4

    new_fields, transformed_rows, transform_warnings = transform_rows_for_prompt_schema(
        fields=fields,
        rows=rows,
        prompt="Extract brand, category, model, ram, storage, price_inr, rating, product_url",
        page_url="https://www.flipkart.com/search?q=macbook+neo",
    )

    assert transform_warnings == [
        "structured_product_schema_applied",
        "structured_product_rows_deduped",
    ]
    assert [field.name for field in new_fields] == [
        "brand",
        "category",
        "product_family",
        "model",
        "parent_product_id",
        "variant_id",
        "canonical_product_id",
        "cluster_id",
        "cluster_confidence",
        "global_entity_id",
        "match_confidence",
        "sku",
        "sku_confidence",
        "ram",
        "storage",
        "processor",
        "display",
        "os_family",
        "os_version",
        "os",
        "is_canonical_name",
        "name_source",
        "price_inr",
        "rating",
        "review_count",
        "review_scope",
        "review_count_timestamp",
        "availability",
        "product_url",
    ]

    assert len(transformed_rows) == 2
    first = transformed_rows[0]
    assert first["brand"] == "Apple"
    assert first["category"] == "laptop"
    assert first["product_family"] == "MacBook Neo"
    assert first["model"] == "MacBook Neo A18 Pro"
    assert str(first["parent_product_id"]).startswith("pp_")
    assert str(first["variant_id"]).startswith("var_")
    assert str(first["canonical_product_id"]).startswith("cpv1_")
    assert str(first["cluster_id"]).startswith("clu_")
    assert str(first["global_entity_id"]).startswith("ge_")
    assert 0.55 <= float(first["cluster_confidence"]) <= 0.99
    assert 0.5 <= float(first["match_confidence"]) <= 0.99
    assert first["sku"] == "MHFA4HN-A"
    assert first["sku_confidence"] == 0.7
    assert first["ram"] == "8 GB"
    assert first["storage"] == "256 GB SSD"
    assert first["processor"] == "Apple A18 Pro"
    assert first["display"] is None
    assert first["os_family"] == "macOS"
    assert first["os_version"] is None
    assert first["os"] == "macOS"
    assert first["is_canonical_name"] is False
    assert first["name_source"] == "marketplace_naming"
    assert first["price_inr"] == 69900
    assert first["rating"] is None
    assert first["review_count"] is None
    assert first["review_scope"] is None
    assert first["review_count_timestamp"] is None
    assert first["availability"] is None
    assert (
        first["product_url"]
        == "https://www.flipkart.com/apple-macbook-neo-a18-pro-2026-pro-8-gb-256-gb-ssd-tahoe-mhfa4hn-a/p/itm9fce39e65bd7e"
    )

    second = transformed_rows[1]
    assert second["product_family"] == "MacBook Neo"
    assert str(second["parent_product_id"]).startswith("pp_")
    assert str(second["variant_id"]).startswith("var_")
    assert str(second["canonical_product_id"]).startswith("cpv1_")
    assert str(second["cluster_id"]).startswith("clu_")
    assert str(second["global_entity_id"]).startswith("ge_")
    assert 0.55 <= float(second["cluster_confidence"]) <= 0.99
    assert 0.5 <= float(second["match_confidence"]) <= 0.99
    assert second["sku"] == "MHFE4HN-A"
    assert second["sku_confidence"] == 0.7
    assert second["ram"] == "8 GB"
    assert second["storage"] == "512 GB SSD"
    assert second["processor"] == "Apple A18 Pro"
    assert second["display"] is None
    assert second["os_family"] == "macOS"
    assert second["os_version"] is None
    assert second["os"] == "macOS"
    assert second["is_canonical_name"] is False
    assert second["name_source"] == "marketplace_naming"
    assert second["price_inr"] == 79900
    assert second["review_count"] is None
    assert second["review_scope"] is None
    assert second["review_count_timestamp"] is None
    assert second["availability"] is None
    assert (
        second["product_url"]
        == "https://www.flipkart.com/apple-macbook-neo-a18-pro-2026-pro-8-gb-512-gb-ssd-tahoe-mhfe4hn-a/p/itm97a16be54dbaf"
    )
    assert first["parent_product_id"] == second["parent_product_id"]
    assert first["variant_id"] != second["variant_id"]
    assert first["canonical_product_id"] != second["canonical_product_id"]
    assert first["global_entity_id"] != second["global_entity_id"]


def test_transform_structured_schema_extracts_display_rating_and_reviews_from_raw_text() -> None:
    prompt = (
        "Extract brand, category, model, ram, storage, processor, display, os, "
        "price_inr, rating, review_count, availability, product_url"
    )
    fields = infer_fields(prompt)
    rows, warnings = parse_rows_from_html(
        html=NOISY_MARKETPLACE_HTML,
        base_url="https://example.com/search?q=iphone",
        fields=fields,
        max_rows=10,
    )
    assert warnings == []
    assert len(rows) == 2

    _new_fields, transformed_rows, transform_warnings = transform_rows_for_prompt_schema(
        fields=fields,
        rows=rows,
        prompt=prompt,
        page_url="https://example.com/search?q=iphone",
    )
    assert transform_warnings == ["structured_product_schema_applied"]
    assert len(transformed_rows) == 2

    first = transformed_rows[0]
    assert first["display"] == "6.7 inch"
    assert first["os_family"] is None
    assert first["os_version"] is None
    assert first["rating"] == 4.6
    assert first["review_count"] == 195
    assert first["review_scope"] == "variant"
    assert str(first["review_count_timestamp"]).endswith("Z")
    assert first["availability"] is None

    second = transformed_rows[1]
    assert second["display"] == "6.9 inch"
    assert second["rating"] == 4.7
    assert second["review_count"] == 913
    assert second["review_scope"] == "variant"
    assert str(second["review_count_timestamp"]).endswith("Z")
    assert second["availability"] == "currently unavailable"


def test_transform_structured_schema_extracts_pre_order_availability_from_raw_text() -> None:
    prompt = (
        "Extract brand, category, model, ram, storage, processor, display, os, "
        "price_inr, rating, review_count, availability, product_url"
    )
    fields = infer_fields(prompt)
    rows, warnings = parse_rows_from_html(
        html=FLIPKART_MACBOOK_NOISY_HTML,
        base_url="https://www.flipkart.com/search?q=macbook+neo",
        fields=fields,
        max_rows=20,
    )
    assert warnings == []
    assert len(rows) == 4

    _new_fields, transformed_rows, transform_warnings = transform_rows_for_prompt_schema(
        fields=fields,
        rows=rows,
        prompt=prompt,
        page_url="https://www.flipkart.com/search?q=macbook+neo",
    )
    assert transform_warnings == [
        "structured_product_schema_applied",
        "structured_product_rows_deduped",
    ]
    assert len(transformed_rows) == 2
    assert transformed_rows[0]["availability"] == "pre order"
    assert transformed_rows[1]["availability"] == "pre order"


def test_transform_structured_schema_normalizes_model_processor_and_os_details() -> None:
    prompt = (
        "Extract brand, category, model, ram, storage, processor, display, os, "
        "price_inr, rating, review_count, availability, product_url"
    )
    fields = infer_fields(prompt)
    rows, warnings = parse_rows_from_html(
        html=LAPTOP_DETAILED_METADATA_HTML,
        base_url="https://www.flipkart.com/search?q=laptops",
        fields=fields,
        max_rows=10,
    )
    assert warnings == []
    assert len(rows) == 3

    _new_fields, transformed_rows, transform_warnings = transform_rows_for_prompt_schema(
        fields=fields,
        rows=rows,
        prompt=prompt,
        page_url="https://www.flipkart.com/search?q=laptops",
    )
    assert transform_warnings == ["structured_product_schema_applied"]
    assert len(transformed_rows) == 3

    first = transformed_rows[0]
    assert first["product_family"] == "MacBook Air"
    assert first["model"] == "MacBook Air M4"
    assert first["sku"] == "MC6V4HN-A"
    assert first["sku_confidence"] == 0.7
    assert first["processor"] == "Apple M4"
    assert first["display"] == "13 inch"
    assert first["os_family"] == "macOS"
    assert first["os_version"] == "Sequoia"
    assert first["os"] == "macOS Sequoia"
    assert first["is_canonical_name"] is True
    assert first["name_source"] == "catalog_pattern"
    assert first["rating"] == 4.8
    assert first["review_count"] == 2
    assert first["review_scope"] == "variant"
    assert str(first["review_count_timestamp"]).endswith("Z")

    second = transformed_rows[1]
    assert second["product_family"] == "Motobook 60"
    assert second["model"] == "Motobook 60 Pro"
    assert second["sku"] is None
    assert second["sku_confidence"] is None
    assert second["processor"] == "Intel Core Ultra 5 225H"
    assert second["display"] == "14 inch"
    assert second["os_family"] == "Windows"
    assert second["os_version"] == "11 Home"
    assert second["os"] == "Windows 11 Home"
    assert second["is_canonical_name"] is True
    assert second["name_source"] == "catalog_pattern"
    assert second["review_count"] == 128
    assert second["review_scope"] == "variant"
    assert str(second["review_count_timestamp"]).endswith("Z")

    third = transformed_rows[2]
    assert third["product_family"] == "Motobook 14"
    assert third["model"] == "Motobook 14"
    assert third["sku"] is None
    assert third["sku_confidence"] is None
    assert third["processor"] == "Intel Core 5 Series 2 210H"
    assert third["os_family"] == "Windows"
    assert third["os_version"] == "11"
    assert third["os"] == "Windows 11"
    assert third["is_canonical_name"] is True
    assert third["name_source"] == "catalog_pattern"


def test_transform_structured_schema_cleans_model_sku_and_processor_regressions() -> None:
    prompt = (
        "Extract brand, category, model, ram, storage, processor, display, os, "
        "price_inr, rating, review_count, availability, product_url"
    )
    fields = infer_fields(prompt)
    rows, warnings = parse_rows_from_html(
        html=MODEL_CLEANUP_REGRESSION_HTML,
        base_url="https://www.flipkart.com/search?q=laptops",
        fields=fields,
        max_rows=10,
    )
    assert warnings == []
    assert len(rows) == 2

    _new_fields, transformed_rows, transform_warnings = transform_rows_for_prompt_schema(
        fields=fields,
        rows=rows,
        prompt=prompt,
        page_url="https://www.flipkart.com/search?q=laptops",
    )
    assert transform_warnings == ["structured_product_schema_applied"]
    assert len(transformed_rows) == 2

    first = transformed_rows[0]
    assert first["model"] == "MacBook Air M3"
    assert first["sku"] == "MC8M4HN-A"
    assert first["sku_confidence"] == 0.95
    assert first["processor"] == "Apple M3"
    assert first["display"] == "13.6 inch"
    assert first["os_family"] == "macOS"
    assert first["os_version"] == "Sequoia"
    assert first["os"] == "macOS Sequoia"
    assert first["is_canonical_name"] is True
    assert first["name_source"] == "catalog_pattern"

    second = transformed_rows[1]
    assert second["model"] == "Galaxy Book4"
    assert second["product_family"] == "Galaxy Book4"
    assert second["sku"] == "NP750XGJ-KG1IN"
    assert second["sku_confidence"] == 0.95
    assert second["ram"] == "16 GB"
    assert second["storage"] == "512 GB SSD"
    assert second["processor"] == "Intel Core i5"
    assert second["display"] == "15.6 inch"
    assert second["os_family"] == "Windows"
    assert second["os_version"] == "11 Home"
    assert second["os"] == "Windows 11 Home"
    assert second["is_canonical_name"] is True
    assert second["name_source"] == "catalog_pattern"


def test_transform_structured_schema_keeps_apple_m_series_in_model() -> None:
    prompt = (
        "Extract brand, category, model, ram, storage, processor, display, os, "
        "price_inr, rating, review_count, availability, product_url"
    )
    fields = infer_fields(prompt)
    rows, warnings = parse_rows_from_html(
        html=APPLE_M5_MODEL_HTML,
        base_url="https://www.flipkart.com/search?q=macbook+air",
        fields=fields,
        max_rows=10,
    )
    assert "container_detection_fallback" in warnings
    assert len(rows) == 1

    _new_fields, transformed_rows, transform_warnings = transform_rows_for_prompt_schema(
        fields=fields,
        rows=rows,
        prompt=prompt,
        page_url="https://www.flipkart.com/search?q=macbook+air",
    )
    assert transform_warnings == ["structured_product_schema_applied"]
    assert len(transformed_rows) == 1

    row = transformed_rows[0]
    assert row["product_family"] == "MacBook Air"
    assert row["model"] == "MacBook Air M5"
    assert row["processor"] == "Apple M5"
    assert row["sku"] == "MCHX4HN-A"
    assert row["sku_confidence"] == 0.7
    assert row["os_family"] == "macOS"
    assert row["os_version"] == "Sequoia"
    assert row["is_canonical_name"] is True
    assert row["name_source"] == "catalog_pattern"


def test_transform_structured_schema_assigns_stable_global_entity_across_marketplaces_with_same_sku() -> None:
    prompt = (
        "Extract brand, category, model, ram, storage, processor, display, os, "
        "price_inr, rating, review_count, availability, product_url"
    )
    fields = infer_fields(prompt)
    rows, warnings = parse_rows_from_html(
        html=CROSS_MARKETPLACE_RESOLUTION_HTML,
        base_url="https://example.com/search?q=macbook+air",
        fields=fields,
        max_rows=10,
    )
    assert warnings == []
    assert len(rows) == 2

    _new_fields, transformed_rows, transform_warnings = transform_rows_for_prompt_schema(
        fields=fields,
        rows=rows,
        prompt=prompt,
        page_url="https://example.com/search?q=macbook+air",
    )
    assert transform_warnings == ["structured_product_schema_applied"]
    assert len(transformed_rows) == 2

    first = transformed_rows[0]
    second = transformed_rows[1]

    assert first["sku"] == "MC6V4HN-A"
    assert second["sku"] == "MC6V4HN-A"
    assert first["sku_confidence"] == 0.95
    assert second["sku_confidence"] == 0.95
    assert first["global_entity_id"] == second["global_entity_id"]
    assert first["cluster_id"] == second["cluster_id"]
    assert float(first["match_confidence"]) >= 0.9
    assert float(second["match_confidence"]) >= 0.9
