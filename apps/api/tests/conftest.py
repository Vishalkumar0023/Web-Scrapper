from __future__ import annotations

import os

import pytest


@pytest.fixture
def integration_stack_config() -> tuple[str, str]:
    enabled = os.getenv("RUN_INFRA_INTEGRATION_TESTS", "").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        pytest.skip("Set RUN_INFRA_INTEGRATION_TESTS=1 to run Postgres+Redis integration tests")

    database_url = os.getenv("INTEGRATION_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/webscrapper")
    redis_url = os.getenv("INTEGRATION_REDIS_URL", "redis://localhost:6379/0")
    return database_url, redis_url
