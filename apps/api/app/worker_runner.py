import time

from app.config import settings
from app.queue import create_queue
from app.services.ai_planner import AIPlannerConfig
from app.store import create_store
from app.worker import start_embedded_worker, stop_embedded_worker


def main() -> None:
    store = create_store(settings.store_backend, settings.database_url)
    queue = create_queue(settings.queue_backend, settings.redis_url, settings.queue_key)

    print(f"[worker] starting with store={store.backend} queue={queue.backend}")
    handle = start_embedded_worker(
        store=store,
        queue=queue,
        scrape_timeout_seconds=settings.scrape_timeout_seconds,
        playwright_fallback_enabled=settings.playwright_fallback_enabled,
        playwright_timeout_seconds=settings.playwright_timeout_seconds,
        duplicate_row_cutoff_ratio=settings.duplicate_row_cutoff_ratio,
        max_consecutive_low_yield_pages=settings.max_consecutive_low_yield_pages,
        ai_planner_config=AIPlannerConfig(
            enabled=settings.ai_planner_enabled,
            provider=settings.ai_provider,
            api_key=settings.ai_api_key,
            model=settings.ai_model,
            timeout_seconds=settings.ai_timeout_seconds,
            max_sample_rows=settings.ai_max_sample_rows,
            max_chars_per_value=settings.ai_max_chars_per_value,
            max_input_chars=settings.ai_max_input_chars,
            max_estimated_input_tokens=settings.ai_max_estimated_input_tokens,
            max_output_tokens=settings.ai_max_output_tokens,
            labeling_prompt=settings.ai_labeling_prompt,
        ),
        worker_concurrency=settings.embedded_worker_concurrency,
        worker_max_retries=settings.worker_max_retries,
        worker_retry_backoff_initial_seconds=settings.worker_retry_backoff_initial_seconds,
        worker_retry_backoff_max_seconds=settings.worker_retry_backoff_max_seconds,
        worker_job_lock_ttl_seconds=settings.worker_job_lock_ttl_seconds,
    )
    try:
        while handle.is_alive():
            time.sleep(0.2)
    except KeyboardInterrupt:
        print("[worker] stopping...")
    finally:
        stop_embedded_worker(handle)
        print("[worker] stopped")


if __name__ == "__main__":
    main()
