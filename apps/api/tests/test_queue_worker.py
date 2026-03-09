import time

import pytest

import app.queue as queue_module
from app.models import FieldInfo, JobMode, JobRecord, JobStatus
from app.queue import InMemoryJobQueue, RedisJobQueue, RunJobMessage
from app.store import InMemoryStore
from app.worker import process_run_job, start_embedded_worker, stop_embedded_worker


def test_process_run_job_updates_store() -> None:
    store = InMemoryStore()
    seed = JobRecord(
        job_id="job_1",
        project_id="proj_1",
        mode=JobMode.full,
        status=JobStatus.queued,
        input_url="https://example.com/search",
        fields=[FieldInfo(name="title", kind="text", confidence=0.9)],
    )
    store.upsert_job(seed)

    message = RunJobMessage(
        job_id="job_1",
        project_id="proj_1",
        url="https://example.com/search",
        prompt="Extract title",
        max_pages=1,
        max_rows=10,
    )
    process_run_job(message=message, store=store, scrape_timeout_seconds=2)

    status = store.job_status("job_1")
    assert status is not None
    assert status.status in {JobStatus.success, JobStatus.partial_success}

    rows = store.job_rows("job_1", offset=0, limit=20)
    assert rows is not None
    assert rows.total_rows > 0


def test_embedded_worker_processes_queue() -> None:
    store = InMemoryStore()
    queue = InMemoryJobQueue()

    seed = JobRecord(
        job_id="job_2",
        project_id="proj_1",
        mode=JobMode.full,
        status=JobStatus.queued,
        input_url="https://example.com/search",
        fields=[FieldInfo(name="title", kind="text", confidence=0.9)],
    )
    store.upsert_job(seed)

    handle = start_embedded_worker(store=store, queue=queue, scrape_timeout_seconds=2)
    try:
        queue.enqueue(
            RunJobMessage(
                job_id="job_2",
                project_id="proj_1",
                url="https://example.com/search",
                prompt="Extract title",
                max_pages=1,
                max_rows=5,
            )
        )

        for _ in range(40):
            status = store.job_status("job_2")
            if status and status.status in {JobStatus.success, JobStatus.partial_success, JobStatus.failed}:
                break
            time.sleep(0.05)

        final_status = store.job_status("job_2")
        assert final_status is not None
        assert final_status.status in {JobStatus.success, JobStatus.partial_success}
    finally:
        stop_embedded_worker(handle)


def test_process_run_job_idempotent_for_terminal_job(monkeypatch) -> None:
    store = InMemoryStore()
    store.upsert_job(
        JobRecord(
            job_id="job_terminal",
            project_id="proj_1",
            mode=JobMode.full,
            status=JobStatus.success,
            input_url="https://example.com/search",
            rows=[{"title": "done"}],
        )
    )

    def _should_not_run(*args, **kwargs):
        raise AssertionError("scrape_full should not run for terminal jobs")

    monkeypatch.setattr("app.worker.scrape_full", _should_not_run)

    result = process_run_job(
        message=RunJobMessage(
            job_id="job_terminal",
            project_id="proj_1",
            url="https://example.com/search",
            prompt="Extract title",
            max_pages=1,
            max_rows=10,
        ),
        store=store,
        scrape_timeout_seconds=2,
    )
    assert result is True
    status = store.job_status("job_terminal")
    assert status is not None
    assert status.status == JobStatus.success


def test_worker_retries_then_dead_letters(monkeypatch) -> None:
    store = InMemoryStore()
    queue = InMemoryJobQueue()
    store.upsert_job(
        JobRecord(
            job_id="job_retry_dlq",
            project_id="proj_1",
            mode=JobMode.full,
            status=JobStatus.queued,
            input_url="https://example.com/search",
        )
    )

    def _boom(*args, **kwargs):
        raise RuntimeError("forced_failure")

    monkeypatch.setattr("app.worker.scrape_full", _boom)

    handle = start_embedded_worker(
        store=store,
        queue=queue,
        scrape_timeout_seconds=2,
        worker_max_retries=1,
        worker_retry_backoff_initial_seconds=0,
        worker_retry_backoff_max_seconds=0,
    )
    try:
        queue.enqueue(
            RunJobMessage(
                job_id="job_retry_dlq",
                project_id="proj_1",
                url="https://example.com/search",
                prompt="Extract title",
                max_pages=1,
                max_rows=10,
                max_attempts=1,
            )
        )

        dead_letters = []
        for _ in range(80):
            dead_letters = queue.list_dead_letters(limit=5)
            if dead_letters:
                break
            time.sleep(0.05)

        assert dead_letters
        assert dead_letters[0]["reason"] == "max_retries_exceeded"

        final_status = store.job_status("job_retry_dlq")
        assert final_status is not None
        assert final_status.status == JobStatus.failed
    finally:
        stop_embedded_worker(handle)


def test_redis_queue_dequeue_timeout_does_not_raise() -> None:
    if queue_module.redis is None:
        pytest.skip("redis package not installed")

    class _TimeoutClient:
        def blpop(self, key: str, timeout: int):
            del key, timeout
            raise queue_module.redis.exceptions.TimeoutError("Timeout reading from socket")

    queue = object.__new__(RedisJobQueue)
    queue._client = _TimeoutClient()  # type: ignore[attr-defined]
    queue._queue_key = "queue:test"
    queue._dead_letter_key = "queue:test:dead"
    queue._lock_prefix = "queue:test:lock:"

    assert queue.dequeue(timeout_seconds=1) is None
