from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import app.services.extractor as extractor
from app.services.fetcher import PageFetchResult


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "regression"
MANIFEST_PATH = FIXTURE_ROOT / "manifest.json"
MANIFEST = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _fixture_text(filename: str) -> str:
    return (FIXTURE_ROOT / filename).read_text(encoding="utf-8")


def _fixture_json(filename: str) -> dict[str, Any]:
    return json.loads((FIXTURE_ROOT / filename).read_text(encoding="utf-8"))


def _fake_fetch_factory(url_to_html: dict[str, str]):
    def _fake_fetch_page_html(
        url: str,
        timeout_seconds: int,
        allow_playwright_fallback: bool = True,
        playwright_timeout_seconds: int | None = None,
    ):
        del timeout_seconds, allow_playwright_fallback, playwright_timeout_seconds
        if url not in url_to_html:
            raise AssertionError(f"Unexpected URL requested in fixture test: {url}")
        return PageFetchResult(html=url_to_html[url], source="http", warnings=[])

    return _fake_fetch_page_html


def _case_id(case: dict[str, Any]) -> str:
    case_id = case.get("id")
    if not isinstance(case_id, str) or not case_id:
        raise AssertionError("Fixture case is missing a non-empty 'id'")
    return case_id


@pytest.mark.parametrize("case", MANIFEST.get("parse_cases", []), ids=_case_id)
def test_regression_parse_fixtures(case: dict[str, Any]) -> None:
    fields = extractor.infer_fields("Extract title, price, rating, product URL")
    html = _fixture_text(case["html"])
    expected = _fixture_json(case["expected"])

    rows, warnings = extractor.parse_rows_from_html(
        html=html,
        base_url=case["base_url"],
        fields=fields,
        max_rows=int(case.get("max_rows", 20)),
    )

    assert warnings == expected["warnings"]
    assert rows == expected["rows"]


@pytest.mark.parametrize("case", MANIFEST.get("full_scrape_cases", []), ids=_case_id)
def test_regression_full_scrape_fixtures(case: dict[str, Any], monkeypatch) -> None:
    url_to_html = {entry["url"]: _fixture_text(entry["html"]) for entry in case["pages"]}
    monkeypatch.setattr(extractor, "fetch_page_html", _fake_fetch_factory(url_to_html))
    expected = _fixture_json(case["expected"])

    result = extractor.scrape_full(
        url=case["start_url"],
        prompt="Extract title, price, rating, product URL",
        max_pages=int(case.get("max_pages", 5)),
        max_rows=int(case.get("max_rows", 20)),
        timeout_seconds=2,
        playwright_fallback_enabled=False,
    )

    assert result.rows == expected["rows"]
    assert result.pages_processed == expected["pages_processed"]
    assert result.partial is bool(expected["partial"])
    for warning in expected.get("warnings_contains", []):
        assert warning in result.warnings


def test_regression_fixture_manifest_files_exist() -> None:
    assert isinstance(MANIFEST.get("version"), int)
    assert MANIFEST["version"] >= 1

    for case in MANIFEST.get("parse_cases", []):
        assert (FIXTURE_ROOT / case["html"]).exists()
        assert (FIXTURE_ROOT / case["expected"]).exists()

    for case in MANIFEST.get("full_scrape_cases", []):
        assert (FIXTURE_ROOT / case["expected"]).exists()
        for page in case["pages"]:
            assert (FIXTURE_ROOT / page["html"]).exists()
