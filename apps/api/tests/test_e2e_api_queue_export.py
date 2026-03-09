from __future__ import annotations

import time
from pathlib import Path
from urllib.parse import urlparse

import app.services.extractor as extractor_module
import app.worker as worker_module
from fastapi.testclient import TestClient

from app.main import app, export_storage
from app.services.fetcher import PageFetchResult


client = TestClient(app)
FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "regression"


def _fixture_text(filename: str) -> str:
    return (FIXTURE_ROOT / filename).read_text(encoding="utf-8")


def _fake_fetch_factory(url_to_html: dict[str, str]):
    def _fake_fetch_page_html(
        url: str,
        timeout_seconds: int,
        allow_playwright_fallback: bool = True,
        playwright_timeout_seconds: int | None = None,
    ):
        del timeout_seconds, allow_playwright_fallback, playwright_timeout_seconds
        if url not in url_to_html:
            raise AssertionError(f"Unexpected URL requested in e2e fixture test: {url}")
        return PageFetchResult(html=url_to_html[url], source="http", warnings=[])

    return _fake_fetch_page_html


def test_e2e_run_queue_export_download(monkeypatch) -> None:
    fake_fetch = _fake_fetch_factory(
        {
            "https://shop.example.com/catalog?page=1": _fixture_text("load_more_catalog_v1.page1.html"),
            "https://shop.example.com/catalog?page=2": _fixture_text("load_more_catalog_v1.page2.html"),
        }
    )
    monkeypatch.setattr(worker_module, "fetch_page_html", fake_fetch)
    monkeypatch.setattr(extractor_module, "fetch_page_html", fake_fetch)

    run_response = client.post(
        "/api/v1/scrape/run",
        json={
            "project_id": f"proj_e2e_{time.time_ns()}",
            "url": "https://shop.example.com/catalog?page=1",
            "prompt": "Extract title, price, rating, product URL",
            "max_pages": 4,
            "max_rows": 20,
        },
    )
    assert run_response.status_code == 200
    run_payload = run_response.json()
    job_id = run_payload["job_id"]
    assert run_payload["status"] == "queued"

    final_status = None
    for _ in range(120):
        status_response = client.get(f"/api/v1/jobs/{job_id}")
        assert status_response.status_code == 200
        final_status = status_response.json()["status"]
        if final_status in {"success", "partial_success", "failed"}:
            break
        time.sleep(0.05)

    assert final_status in {"success", "partial_success"}

    rows_response = client.get(f"/api/v1/jobs/{job_id}/rows?offset=0&limit=100")
    assert rows_response.status_code == 200
    rows_payload = rows_response.json()
    assert rows_payload["total_rows"] == 4
    assert rows_payload["rows"][0]["title"] == "Alpha Backpack"

    export_response = client.post(
        f"/api/v1/export/{job_id}",
        json={
            "format": "csv",
            "selected_columns": ["title", "price"],
        },
    )
    assert export_response.status_code == 200
    export_payload = export_response.json()
    assert export_payload["status"] == "ready"
    assert export_payload["export_id"].startswith("exp_")

    if export_storage.backend == "local":
        parsed = urlparse(export_payload["file_url"])
        download_path = f"{parsed.path}?{parsed.query}"
        download_response = client.get(download_path)
        assert download_response.status_code == 200
        csv_body = download_response.content.decode("utf-8")
        assert "title,price" in csv_body
        assert "Alpha Backpack,$31.00" in csv_body
