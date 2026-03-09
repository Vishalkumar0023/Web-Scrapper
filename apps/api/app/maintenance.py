from __future__ import annotations

import argparse

from app.config import settings
from app.store import create_store


def run_cleanup(retention_days: int) -> None:
    store = create_store(settings.store_backend, settings.database_url)
    result = store.cleanup_old_data(retention_days=retention_days)
    print(
        "cleanup_result",
        {
            "deleted_jobs": result.deleted_jobs,
            "deleted_rows": result.deleted_rows,
            "deleted_exports": result.deleted_exports,
            "deleted_usage_events": result.deleted_usage_events,
            "deleted_invalidated_templates": result.deleted_invalidated_templates,
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run data retention cleanup")
    parser.add_argument("--retention-days", type=int, default=settings.data_retention_days)
    args = parser.parse_args()
    run_cleanup(retention_days=max(0, args.retention_days))


if __name__ == "__main__":
    main()
