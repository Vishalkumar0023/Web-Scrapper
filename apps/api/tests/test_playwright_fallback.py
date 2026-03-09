import httpx

from app.services import fetcher
from app.services.extractor import infer_fields, parse_rows_from_html


def _raise_http_error(*_args, **_kwargs):
    raise httpx.HTTPError("boom")


def test_fetch_page_html_uses_playwright_after_http_failure(monkeypatch) -> None:
    monkeypatch.setattr(fetcher, "fetch_html_http", _raise_http_error)
    monkeypatch.setattr(
        fetcher,
        "fetch_html_playwright",
        lambda url, timeout_seconds: "<html><body><a href='/a'>A</a><a href='/b'>B</a></body></html>",
    )

    result = fetcher.fetch_page_html(
        url="https://example.com",
        timeout_seconds=2,
        allow_playwright_fallback=True,
        playwright_timeout_seconds=3,
    )

    assert result.source == "playwright"
    assert "http_fetch_failed" in result.warnings
    assert "source_playwright_fallback" in result.warnings


def test_preview_parse_with_playwright_html(monkeypatch) -> None:
    monkeypatch.setattr(fetcher, "fetch_html_http", _raise_http_error)
    monkeypatch.setattr(
        fetcher,
        "fetch_html_playwright",
        lambda url, timeout_seconds: (
            "<html><body>"
            "<div class='item-card'><a href='/1'><h2>Alpha</h2></a><p>$10</p><p>4.5 out of 5 stars</p></div>"
            "<div class='item-card'><a href='/2'><h2>Beta</h2></a><p>$11</p><p>4.2 out of 5 stars</p></div>"
            "</body></html>"
        ),
    )

    outcome = fetcher.fetch_page_html(
        url="https://example.com",
        timeout_seconds=2,
        allow_playwright_fallback=True,
    )
    fields = infer_fields("Extract title, price, rating, product URL")
    rows, warnings = parse_rows_from_html(
        html=outcome.html,
        base_url="https://example.com",
        fields=fields,
        max_rows=10,
    )

    assert warnings == []
    assert len(rows) == 2
    assert rows[0]["title"] == "Alpha"
