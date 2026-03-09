from __future__ import annotations

import time
from pathlib import Path
from uuid import uuid4

import pytest

import app.services.extractor as extractor_module
import app.worker as worker_module
from app.models import JobMode, JobRecord, JobStatus
from app.queue import RunJobMessage, create_queue
from app.services.extractor import infer_fields
from app.services.fetcher import PageFetchResult
from app.store import create_store
from app.worker import start_embedded_worker, stop_embedded_worker


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
            raise AssertionError(f"Unexpected URL requested in integration fixture test: {url}")
        return PageFetchResult(html=url_to_html[url], source="http", warnings=[])

    return _fake_fetch_page_html


def test_worker_integration_postgres_redis(monkeypatch, integration_stack_config) -> None:
    database_url, redis_url = integration_stack_config
    queue_key = f"queue:scrape:integration:{uuid4().hex[:10]}"

    try:
        store = create_store("postgres", database_url)
        queue = create_queue("redis", redis_url, queue_key)
    except Exception as exc:
        pytest.skip(f"Integration infrastructure unavailable: {exc}")

    fake_fetch = _fake_fetch_factory(
        {
            "https://shop.example.com/catalog?page=1": _fixture_text("load_more_catalog_v1.page1.html"),
            "https://shop.example.com/catalog?page=2": _fixture_text("load_more_catalog_v1.page2.html"),
        }
    )
    monkeypatch.setattr(worker_module, "fetch_page_html", fake_fetch)
    monkeypatch.setattr(extractor_module, "fetch_page_html", fake_fetch)

    job_id = f"job_int_{uuid4().hex[:10]}"
    project_id = f"proj_int_{uuid4().hex[:10]}"
    store.upsert_job(
        JobRecord(
            job_id=job_id,
            project_id=project_id,
            mode=JobMode.full,
            status=JobStatus.queued,
            input_url="https://shop.example.com/catalog?page=1",
            prompt="Extract title, price, rating, product URL",
            max_pages=4,
            max_rows=20,
            fields=infer_fields("Extract title, price, rating, product URL"),
        )
    )

    handle = start_embedded_worker(
        store=store,
        queue=queue,
        scrape_timeout_seconds=2,
        playwright_fallback_enabled=False,
        worker_concurrency=1,
        worker_max_retries=0,
    )
    try:
        queue.enqueue(
            RunJobMessage(
                job_id=job_id,
                project_id=project_id,
                url="https://shop.example.com/catalog?page=1",
                prompt="Extract title, price, rating, product URL",
                max_pages=4,
                max_rows=20,
                max_attempts=0,
                idempotency_key=job_id,
            )
        )

        final_status: JobStatus | None = None
        deadline = time.time() + 20
        while time.time() < deadline:
            status = store.job_status(job_id)
            if status and status.status in {JobStatus.success, JobStatus.partial_success, JobStatus.failed}:
                final_status = status.status
                break
            time.sleep(0.1)

        assert final_status in {JobStatus.success, JobStatus.partial_success}

        job = store.get_job(job_id)
        assert job is not None
        assert len(job.rows) == 4
        assert job.progress.pages_processed == 2
        assert any("pagination_load_more_detected" == warning for warning in job.warnings)
        assert job.rows[0]["title"] == "Alpha Backpack"
        assert job.rows[0]["product_url"] == "https://shop.example.com/product/101"
    finally:
        stop_embedded_worker(handle)
        if getattr(queue, "backend", "") == "redis":
            queue._client.delete(queue._queue_key, queue._dead_letter_key)  # type: ignore[attr-defined]
