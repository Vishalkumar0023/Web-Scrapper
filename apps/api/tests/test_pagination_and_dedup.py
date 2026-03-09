from app.services import extractor
from app.services.fetcher import PageFetchResult


def _fake_fetch_factory(url_to_html: dict[str, str]):
    def _fake_fetch_page_html(url: str, timeout_seconds: int, allow_playwright_fallback: bool = True, playwright_timeout_seconds: int | None = None):
        if url not in url_to_html:
            raise AssertionError(f"Unexpected URL requested: {url}")
        return PageFetchResult(html=url_to_html[url], source="http", warnings=[])

    return _fake_fetch_page_html


def _listing_page(items: list[tuple[str, str, str, str]], footer: str = "") -> str:
    cards = []
    for title, href, price, rating in items:
        cards.append(
            (
                "<div class='product-card'>"
                f"<a href='{href}'><h3>{title}</h3></a>"
                f"<span class='price'>{price}</span>"
                f"<span class='rating'>{rating}</span>"
                "</div>"
            )
        )
    return f"<html><body><section class='results'>{''.join(cards)}</section>{footer}</body></html>"


def test_scrape_full_follows_load_more_anchor(monkeypatch) -> None:
    page1 = _listing_page(
        [
            ("Item One", "/p/1", "$10.00", "4.2 out of 5 stars"),
            ("Item Two", "/p/2", "$12.00", "4.4 out of 5 stars"),
        ],
        footer="<a class='load-more' href='/search?page=2'>Load more</a>",
    )
    page2 = _listing_page(
        [
            ("Item Three", "/p/3", "$15.00", "4.1 out of 5 stars"),
            ("Item Four", "/p/4", "$17.00", "4.6 out of 5 stars"),
        ]
    )

    url_map = {
        "https://shop.example.com/search?page=1": page1,
        "https://shop.example.com/search?page=2": page2,
    }
    monkeypatch.setattr(extractor, "fetch_page_html", _fake_fetch_factory(url_map))

    result = extractor.scrape_full(
        url="https://shop.example.com/search?page=1",
        prompt="Extract title, price, rating, product URL",
        max_pages=5,
        max_rows=20,
        timeout_seconds=2,
        playwright_fallback_enabled=False,
    )

    assert result.pages_processed == 2
    assert len(result.rows) == 4
    assert "pagination_load_more_detected" in result.warnings


def test_scrape_full_uses_query_increment_when_load_more_has_no_url(monkeypatch) -> None:
    page1 = _listing_page(
        [
            ("Item A", "/a", "$20.00", "4.2 out of 5 stars"),
            ("Item B", "/b", "$21.00", "4.0 out of 5 stars"),
        ],
        footer="<button class='load-more'>Load More</button>",
    )
    page2 = _listing_page(
        [
            ("Item C", "/c", "$22.00", "4.3 out of 5 stars"),
        ]
    )

    url_map = {
        "https://shop.example.com/search?page=1": page1,
        "https://shop.example.com/search?page=2": page2,
    }
    monkeypatch.setattr(extractor, "fetch_page_html", _fake_fetch_factory(url_map))

    result = extractor.scrape_full(
        url="https://shop.example.com/search?page=1",
        prompt="Extract title, price, rating, product URL",
        max_pages=3,
        max_rows=10,
        timeout_seconds=2,
        playwright_fallback_enabled=False,
    )

    assert result.pages_processed == 2
    assert len(result.rows) == 3
    assert "pagination_query_increment_used" in result.warnings


def test_scrape_full_stops_on_duplicate_ratio_cutoff(monkeypatch) -> None:
    page1 = _listing_page(
        [
            ("Item 1", "/i/1", "$10.00", "4.1 out of 5 stars"),
            ("Item 2", "/i/2", "$11.00", "4.2 out of 5 stars"),
        ],
        footer="<a rel='next' href='/search?page=2'>Next</a>",
    )
    # Two rows are duplicates from page1, one row is new => high duplicate ratio.
    page2 = _listing_page(
        [
            ("Item 1", "/i/1", "$10.00", "4.1 out of 5 stars"),
            ("Item 2", "/i/2", "$11.00", "4.2 out of 5 stars"),
            ("Item 3", "/i/3", "$12.00", "4.3 out of 5 stars"),
        ]
    )

    url_map = {
        "https://shop.example.com/search?page=1": page1,
        "https://shop.example.com/search?page=2": page2,
    }
    monkeypatch.setattr(extractor, "fetch_page_html", _fake_fetch_factory(url_map))

    result = extractor.scrape_full(
        url="https://shop.example.com/search?page=1",
        prompt="Extract title, price, rating, product URL",
        max_pages=5,
        max_rows=20,
        timeout_seconds=2,
        playwright_fallback_enabled=False,
        duplicate_row_cutoff_ratio=0.6,
    )

    assert result.partial is True
    assert result.pages_processed == 1
    assert len(result.rows) == 2
    assert "duplicate_row_cutoff_reached" in result.warnings
