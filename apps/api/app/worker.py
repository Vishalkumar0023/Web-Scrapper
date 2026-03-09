from __future__ import annotations

from dataclasses import dataclass
from math import pow
from threading import Event, Thread
from uuid import uuid4

from app.models import JobMode, JobRecord, JobStatus, UsageEventRecord
from app.queue import JobQueue, RunJobMessage
from app.services.ai_planner import AIPlannerConfig, apply_ai_field_labels
from app.services.extractor import (
    FullScrapeResult,
    compute_page_signature,
    filter_duplicate_rows,
    find_next_page_url,
    infer_fields,
    scrape_full,
    transform_rows_for_prompt_schema,
)
from app.services.fetcher import PageFetchError, fetch_page_html
from app.services.template_engine import (
    apply_template_extract_rows,
    compute_page_fingerprint,
    fields_from_template,
    find_next_page_url_with_template,
    normalize_domain,
)
from app.store import Store


@dataclass
class WorkerHandle:
    threads: list[Thread]
    stop_event: Event

    @property
    def thread(self) -> Thread:
        # Backward-compatible alias for callers that expect a single thread.
        return self.threads[0]

    def is_alive(self) -> bool:
        return any(thread.is_alive() for thread in self.threads)


def _scrape_full_with_template(
    store: Store,
    template_id: str,
    url: str,
    max_pages: int,
    max_rows: int,
    scrape_timeout_seconds: int,
    playwright_fallback_enabled: bool,
    playwright_timeout_seconds: int | None,
    duplicate_row_cutoff_ratio: float,
    max_consecutive_low_yield_pages: int,
) -> FullScrapeResult | None:
    template = store.get_template(template_id)
    if template is None or template.invalidated:
        return None

    rows: list[dict[str, object]] = []
    warnings: list[str] = [f"template_matched:{template.template_id}", f"template_version:{template.version}"]
    seen_page_signatures: set[str] = set()
    seen_row_signatures: set[tuple[tuple[str, str], ...]] = set()
    pages_processed = 0
    low_yield_streak = 0
    partial = False
    current_url = url
    visited: set[str] = set()

    while current_url and pages_processed < max_pages and len(rows) < max_rows:
        if current_url in visited:
            warnings.append("repeated_page_detected")
            partial = pages_processed > 0
            break
        visited.add(current_url)

        try:
            fetch_result = fetch_page_html(
                url=current_url,
                timeout_seconds=scrape_timeout_seconds,
                allow_playwright_fallback=playwright_fallback_enabled,
                playwright_timeout_seconds=playwright_timeout_seconds,
            )
            html = fetch_result.html
            warnings.extend(fetch_result.warnings)
        except PageFetchError:
            warnings.append("page_load_failed")
            partial = pages_processed > 0
            break

        remaining = max_rows - len(rows)
        page_rows, template_warnings = apply_template_extract_rows(
            html=html,
            base_url=current_url,
            template=template,
            max_rows=remaining,
        )
        warnings.extend(template_warnings)

        page_signature = compute_page_signature(page_rows=page_rows, html=html)
        if page_signature in seen_page_signatures:
            warnings.append("repeated_page_signature_detected")
            partial = pages_processed > 0
            break
        seen_page_signatures.add(page_signature)

        unique_page_rows, duplicate_ratio = filter_duplicate_rows(
            page_rows=page_rows,
            seen_row_signatures=seen_row_signatures,
        )
        if duplicate_ratio >= duplicate_row_cutoff_ratio and pages_processed > 0:
            warnings.append("duplicate_row_cutoff_reached")
            partial = True
            break

        if not unique_page_rows:
            low_yield_streak += 1
            warnings.append("page_yield_low")
        else:
            low_yield_streak = 0
            rows.extend(unique_page_rows)

        pages_processed += 1
        if len(rows) >= max_rows:
            break

        if low_yield_streak > max_consecutive_low_yield_pages:
            warnings.append("consecutive_low_yield_cutoff_reached")
            partial = True
            break

        next_url, template_pagination_warnings = find_next_page_url_with_template(
            html=html,
            base_url=current_url,
            template=template,
        )
        warnings.extend(template_pagination_warnings)
        if not next_url:
            fallback_next, fallback_warnings = find_next_page_url(html=html, base_url=current_url, current_url=current_url)
            if fallback_next:
                warnings.append("template_pagination_fallback_used")
                warnings.extend(fallback_warnings)
                next_url = fallback_next

        if not next_url:
            break
        current_url = next_url

    if not rows:
        return FullScrapeResult(
            fields=fields_from_template(template) or infer_fields(None),
            page_type=template.page_type,
            rows=[],
            warnings=warnings + ["template_rows_empty"],
            pages_processed=max(1, pages_processed),
            partial=partial,
        )

    return FullScrapeResult(
        fields=fields_from_template(template) or infer_fields(None),
        page_type=template.page_type,
        rows=rows[:max_rows],
        warnings=warnings,
        pages_processed=pages_processed,
        partial=partial,
    )


def process_run_job(
    message: RunJobMessage,
    store: Store,
    scrape_timeout_seconds: int,
    playwright_fallback_enabled: bool = True,
    playwright_timeout_seconds: int | None = None,
    duplicate_row_cutoff_ratio: float = 0.8,
    max_consecutive_low_yield_pages: int = 1,
    ai_planner_config: AIPlannerConfig | None = None,
) -> bool:
    existing = store.get_job(message.job_id)
    if existing is not None and existing.status in {JobStatus.success, JobStatus.partial_success, JobStatus.cancelled} and not message.force:
        # Idempotency guard: repeated delivery for terminal jobs is ignored.
        return True

    if existing is None:
        existing = JobRecord(
            job_id=message.job_id,
            project_id=message.project_id,
            mode=JobMode.full,
            status=JobStatus.queued,
            input_url=message.url,
            prompt=message.prompt,
            max_pages=message.max_pages,
            max_rows=message.max_rows,
            fields=infer_fields(message.prompt),
        )

    if existing.status == JobStatus.cancelled and not message.force:
        return True

    existing.status = JobStatus.extraction_running
    store.upsert_job(existing)

    matched_template_id: str | None = message.template_id
    if not matched_template_id:
        try:
            first_page = fetch_page_html(
                url=message.url,
                timeout_seconds=scrape_timeout_seconds,
                allow_playwright_fallback=playwright_fallback_enabled,
                playwright_timeout_seconds=playwright_timeout_seconds,
            )
            fingerprint = compute_page_fingerprint(first_page.html)
            matched = store.match_template(
                domain=normalize_domain(message.url),
                page_type=None,
                page_fingerprint=fingerprint,
                template_id=None,
            )
            if matched:
                matched_template_id = matched.template_id
        except PageFetchError:
            matched_template_id = None

    try:
        result: FullScrapeResult | None = None
        if matched_template_id:
            result = _scrape_full_with_template(
                store=store,
                template_id=matched_template_id,
                url=message.url,
                max_pages=message.max_pages,
                max_rows=message.max_rows,
                scrape_timeout_seconds=scrape_timeout_seconds,
                playwright_fallback_enabled=playwright_fallback_enabled,
                playwright_timeout_seconds=playwright_timeout_seconds,
                duplicate_row_cutoff_ratio=duplicate_row_cutoff_ratio,
                max_consecutive_low_yield_pages=max_consecutive_low_yield_pages,
            )
            if result and result.rows:
                store.update_template_metrics(template_id=matched_template_id, success=True)
            else:
                store.update_template_metrics(
                    template_id=matched_template_id,
                    success=False,
                    invalidation_reason="template_rows_empty",
                )

        if not result or not result.rows:
            heuristic_result = scrape_full(
                url=message.url,
                prompt=message.prompt,
                max_pages=message.max_pages,
                max_rows=message.max_rows,
                timeout_seconds=scrape_timeout_seconds,
                playwright_fallback_enabled=playwright_fallback_enabled,
                playwright_timeout_seconds=playwright_timeout_seconds,
                duplicate_row_cutoff_ratio=duplicate_row_cutoff_ratio,
                max_consecutive_low_yield_pages=max_consecutive_low_yield_pages,
            )
            if result and result.warnings:
                heuristic_result.warnings = list(result.warnings) + list(heuristic_result.warnings)
            result = heuristic_result

        if ai_planner_config:
            ai_result = apply_ai_field_labels(
                config=ai_planner_config,
                prompt=message.prompt,
                page_url=message.url,
                page_type=result.page_type,
                fields=result.fields,
                rows=result.rows,
            )
            result.fields = ai_result.fields
            result.rows = ai_result.rows
            result.warnings = list(result.warnings) + list(ai_result.warnings)

        result.fields, result.rows, schema_warnings = transform_rows_for_prompt_schema(
            fields=result.fields,
            rows=result.rows,
            prompt=message.prompt,
            page_url=message.url,
        )
        result.warnings = list(result.warnings) + list(schema_warnings)

        existing.rows = result.rows
        existing.fields = result.fields
        existing.page_type = result.page_type
        existing.warnings = result.warnings
        existing.progress.pages_processed = result.pages_processed
        existing.progress.rows_extracted = len(result.rows)
        latest = store.get_job(existing.job_id)
        if latest is not None and latest.status == JobStatus.cancelled and not message.force:
            return True

        existing.status = JobStatus.partial_success if result.partial else JobStatus.success
        store.upsert_job(existing)
        store.record_usage_event(
            event=UsageEventRecord(
                workspace_id="ws_default",
                user_id="user_system",
                event_type="run.completed",
                event_json={
                    "job_id": existing.job_id,
                    "project_id": existing.project_id,
                    "status": existing.status.value,
                    "rows": len(existing.rows),
                    "pages_processed": existing.progress.pages_processed,
                },
            )
        )
        return True
    except Exception:
        existing.status = JobStatus.failed
        existing.warnings = list(existing.warnings) + ["worker_processing_failed"]
        store.upsert_job(existing)
        return False


def _retry_delay_seconds(initial_seconds: int, max_seconds: int, attempt: int) -> int:
    bounded_initial = max(0, initial_seconds)
    bounded_max = max(bounded_initial, max(0, max_seconds))
    if bounded_initial == 0:
        return 0
    delay = int(bounded_initial * pow(2, max(0, attempt)))
    return min(bounded_max, delay)


def worker_loop(
    store: Store,
    queue: JobQueue,
    scrape_timeout_seconds: int,
    playwright_fallback_enabled: bool,
    playwright_timeout_seconds: int | None,
    duplicate_row_cutoff_ratio: float,
    max_consecutive_low_yield_pages: int,
    ai_planner_config: AIPlannerConfig | None,
    stop_event: Event,
    worker_id: str,
    worker_max_retries: int,
    worker_retry_backoff_initial_seconds: int,
    worker_retry_backoff_max_seconds: int,
    worker_job_lock_ttl_seconds: int,
) -> None:
    while not stop_event.is_set():
        message = queue.dequeue(timeout_seconds=1)
        if message is None:
            continue

        lock_owner = f"{worker_id}:{uuid4().hex[:8]}"
        if not queue.acquire_job_lock(
            job_id=message.job_id,
            owner_id=lock_owner,
            ttl_seconds=worker_job_lock_ttl_seconds,
        ):
            # Duplicate in-flight delivery for same job_id; keep idempotent and skip.
            continue

        try:
            succeeded = process_run_job(
                message=message,
                store=store,
                scrape_timeout_seconds=scrape_timeout_seconds,
                playwright_fallback_enabled=playwright_fallback_enabled,
                playwright_timeout_seconds=playwright_timeout_seconds,
                duplicate_row_cutoff_ratio=duplicate_row_cutoff_ratio,
                max_consecutive_low_yield_pages=max_consecutive_low_yield_pages,
                ai_planner_config=ai_planner_config,
            )
            if succeeded:
                continue

            max_retries = max(0, min(worker_max_retries, message.max_attempts))
            if message.attempt < max_retries:
                retry_delay = _retry_delay_seconds(
                    initial_seconds=worker_retry_backoff_initial_seconds,
                    max_seconds=worker_retry_backoff_max_seconds,
                    attempt=message.attempt,
                )
                retry_message = message.with_retry(delay_seconds=retry_delay)
                queue.enqueue(retry_message)

                job = store.get_job(message.job_id)
                if job is not None and job.status != JobStatus.cancelled:
                    job.status = JobStatus.retrying
                    job.warnings = list(job.warnings) + [f"retry_scheduled_attempt_{retry_message.attempt}"]
                    store.upsert_job(job)

                store.record_usage_event(
                    event=UsageEventRecord(
                        workspace_id="ws_default",
                        user_id="user_system",
                        event_type="run.retry_scheduled",
                        event_json={
                            "job_id": message.job_id,
                            "project_id": message.project_id,
                            "attempt": retry_message.attempt,
                            "delay_seconds": retry_delay,
                        },
                    )
                )
                continue

            queue.enqueue_dead_letter(message=message, reason="max_retries_exceeded")
            store.record_usage_event(
                event=UsageEventRecord(
                    workspace_id="ws_default",
                    user_id="user_system",
                    event_type="run.failed",
                    event_json={
                        "job_id": message.job_id,
                        "project_id": message.project_id,
                        "status": JobStatus.failed.value,
                        "final_attempt": message.attempt,
                    },
                )
            )
            store.record_usage_event(
                event=UsageEventRecord(
                    workspace_id="ws_default",
                    user_id="user_system",
                    event_type="run.dead_lettered",
                    event_json={
                        "job_id": message.job_id,
                        "project_id": message.project_id,
                        "attempt": message.attempt,
                    },
                )
            )
        finally:
            queue.release_job_lock(job_id=message.job_id, owner_id=lock_owner)


def start_embedded_worker(
    store: Store,
    queue: JobQueue,
    scrape_timeout_seconds: int,
    playwright_fallback_enabled: bool = True,
    playwright_timeout_seconds: int | None = None,
    duplicate_row_cutoff_ratio: float = 0.8,
    max_consecutive_low_yield_pages: int = 1,
    ai_planner_config: AIPlannerConfig | None = None,
    worker_concurrency: int = 1,
    worker_max_retries: int = 3,
    worker_retry_backoff_initial_seconds: int = 2,
    worker_retry_backoff_max_seconds: int = 30,
    worker_job_lock_ttl_seconds: int = 300,
) -> WorkerHandle:
    stop_event = Event()
    concurrency = max(1, int(worker_concurrency))
    threads: list[Thread] = []

    for index in range(concurrency):
        thread = Thread(
            target=worker_loop,
            kwargs={
                "store": store,
                "queue": queue,
                "scrape_timeout_seconds": scrape_timeout_seconds,
                "playwright_fallback_enabled": playwright_fallback_enabled,
                "playwright_timeout_seconds": playwright_timeout_seconds,
                "duplicate_row_cutoff_ratio": duplicate_row_cutoff_ratio,
                "max_consecutive_low_yield_pages": max_consecutive_low_yield_pages,
                "ai_planner_config": ai_planner_config,
                "stop_event": stop_event,
                "worker_id": f"embedded-{index + 1}",
                "worker_max_retries": worker_max_retries,
                "worker_retry_backoff_initial_seconds": worker_retry_backoff_initial_seconds,
                "worker_retry_backoff_max_seconds": worker_retry_backoff_max_seconds,
                "worker_job_lock_ttl_seconds": worker_job_lock_ttl_seconds,
            },
            daemon=True,
            name=f"webscrapper-worker-{index + 1}",
        )
        thread.start()
        threads.append(thread)

    return WorkerHandle(threads=threads, stop_event=stop_event)


def stop_embedded_worker(handle: WorkerHandle) -> None:
    handle.stop_event.set()
    for thread in handle.threads:
        thread.join(timeout=2)
