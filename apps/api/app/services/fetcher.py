from __future__ import annotations

import importlib
import re
from dataclasses import dataclass, field

import httpx
from bs4 import BeautifulSoup


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    )
}

JS_SHELL_PATTERNS = [
    re.compile(r"enable javascript", re.IGNORECASE),
    re.compile(r"javascript is required", re.IGNORECASE),
    re.compile(r"loading\.\.\.", re.IGNORECASE),
]


@dataclass(frozen=True)
class PageFetchResult:
    html: str
    source: str
    warnings: list[str] = field(default_factory=list)


class PageFetchError(RuntimeError):
    pass


def fetch_page_html(
    url: str,
    timeout_seconds: int,
    allow_playwright_fallback: bool = True,
    playwright_timeout_seconds: int | None = None,
) -> PageFetchResult:
    warnings: list[str] = []

    try:
        http_html = fetch_html_http(url=url, timeout_seconds=timeout_seconds)
        if allow_playwright_fallback and looks_like_js_shell(http_html):
            warnings.append("http_js_shell_detected")
            fallback_html = fetch_html_playwright(
                url=url,
                timeout_seconds=playwright_timeout_seconds or max(timeout_seconds * 2, timeout_seconds + 4),
            )
            if fallback_html:
                warnings.append("source_playwright_fallback")
                return PageFetchResult(html=fallback_html, source="playwright", warnings=warnings)
            warnings.append("playwright_unavailable_or_failed")

        return PageFetchResult(html=http_html, source="http", warnings=warnings)
    except Exception:
        warnings.append("http_fetch_failed")

    if allow_playwright_fallback:
        fallback_html = fetch_html_playwright(
            url=url,
            timeout_seconds=playwright_timeout_seconds or max(timeout_seconds * 2, timeout_seconds + 4),
        )
        if fallback_html:
            warnings.append("source_playwright_fallback")
            return PageFetchResult(html=fallback_html, source="playwright", warnings=warnings)
        warnings.append("playwright_unavailable_or_failed")

    raise PageFetchError("Failed to fetch page via HTTP and Playwright fallback")


def fetch_html_http(url: str, timeout_seconds: int) -> str:
    response = httpx.get(url, headers=DEFAULT_HEADERS, follow_redirects=True, timeout=timeout_seconds)
    response.raise_for_status()
    return response.text


def fetch_html_playwright(url: str, timeout_seconds: int) -> str | None:
    if not has_playwright():
        return None

    from playwright.sync_api import sync_playwright

    timeout_ms = timeout_seconds * 1000

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                context = browser.new_context()
                page = context.new_page()
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                try:
                    page.wait_for_load_state("networkidle", timeout=timeout_ms)
                except Exception:
                    # Some pages never reach network idle; continue with current render.
                    pass
                html = page.content()
                context.close()
                return html
            finally:
                browser.close()
    except Exception:
        return None


def has_playwright() -> bool:
    return importlib.util.find_spec("playwright") is not None


def looks_like_js_shell(html: str) -> bool:
    if not html:
        return True

    sample = html[:2000]
    for pattern in JS_SHELL_PATTERNS:
        if pattern.search(sample):
            return True

    soup = BeautifulSoup(html, "html.parser")
    scripts = len(soup.find_all("script"))
    anchors = len(soup.find_all("a"))
    text_length = len(soup.get_text(" ", strip=True))

    # Heuristic: many scripts + very low meaningful text usually indicates client-rendered shell.
    if scripts >= 8 and text_length < 120 and anchors <= 2:
        return True

    return False
