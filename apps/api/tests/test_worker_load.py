from __future__ import annotations

import os
import time
from uuid import uuid4

import pytest

import app.services.extractor as extractor_module
import app.worker as worker_module
from app.models import JobMode, JobRecord, JobStatus
from app.queue import InMemoryJobQueue, RunJobMessage
from app.services.fetcher import PageFetchResult
from app.store import InMemoryStore
from app.worker import start_embedded_worker, stop_embedded_worker


SIMPLE_LISTING_HTML = """
<html>
  <body>
    <section class="catalog">
      <article class="card">
        <a href="/item/1"><h2>Load Test Item One</h2></a>
        <span class="price">$10.00</span>
        <span class="rating">4.1 out of 5 stars</span>
      </article>
      <article class="card">
        <a href="/item/2"><h2>Load Test Item Two</h2></a>
        <span class="price">$12.00</span>
        <span class="rating">4.3 out of 5 stars</span>
      </article>
    </section>
  </body>
</html>
"""


def _fake_fetch_page_html(
    url: str,
    timeout_seconds: int,
    allow_playwright_fallback: bool = True,
    playwright_timeout_seconds: int | None = None,
):
    del url, timeout_seconds, allow_playwright_fallback, playwright_timeout_seconds
    return PageFetchResult(html=SIMPLE_LISTING_HTML, source="http", warnings=[])


def test_worker_load_baseline(monkeypatch) -> None:
    enabled = os.getenv("RUN_LOAD_TESTS", "").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        pytest.skip("Set RUN_LOAD_TESTS=1 to run worker load baseline test")

    monkeypatch.setattr(worker_module, "fetch_page_html", _fake_fetch_page_html)
    monkeypatch.setattr(extractor_module, "fetch_page_html", _fake_fetch_page_html)

    store = InMemoryStore()
    queue = InMemoryJobQueue()
    worker_concurrency = 4
    job_count = 40

    for index in range(job_count):
        job_id = f"job_load_{index}_{uuid4().hex[:8]}"
        store.upsert_job(
            JobRecord(
                job_id=job_id,
                project_id="proj_load",
                mode=JobMode.full,
                status=JobStatus.queued,
                input_url="https://load.example.com/catalog?page=1",
                prompt="Extract title, price, rating, product URL",
                max_pages=1,
                max_rows=10,
            )
        )
        queue.enqueue(
            RunJobMessage(
                job_id=job_id,
                project_id="proj_load",
                url="https://load.example.com/catalog?page=1",
                prompt="Extract title, price, rating, product URL",
                max_pages=1,
                max_rows=10,
                max_attempts=0,
                idempotency_key=job_id,
            )
        )

    handle = start_embedded_worker(
        store=store,
        queue=queue,
        scrape_timeout_seconds=2,
        playwright_fallback_enabled=False,
        worker_concurrency=worker_concurrency,
        worker_max_retries=0,
    )
    started_at = time.perf_counter()
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            terminal = 0
            failed = 0
            for job_id, job in list(store._jobs.items()):  # type: ignore[attr-defined]
                if job.status in {JobStatus.success, JobStatus.partial_success, JobStatus.failed}:
                    terminal += 1
                if job.status == JobStatus.failed:
                    failed += 1
            if terminal == job_count:
                break
            time.sleep(0.1)

        completed_at = time.perf_counter()
        duration_seconds = max(0.001, completed_at - started_at)
        throughput = job_count / duration_seconds

        statuses = [job.status for job in store._jobs.values()]  # type: ignore[attr-defined]
        assert all(status in {JobStatus.success, JobStatus.partial_success} for status in statuses)
        # Conservative baseline for local runs to catch severe regressions.
        assert throughput >= 1.0
    finally:
        stop_embedded_worker(handle)
