import os
from dataclasses import dataclass
from pathlib import Path


def _load_env_file() -> None:
    # Lightweight .env loader to avoid extra runtime dependency.
    candidates: list[Path] = []

    def _push(path: Path) -> None:
        if path not in candidates:
            candidates.append(path)

    cwd = Path.cwd().resolve()
    _push(cwd / ".env")
    _push(cwd.parent / ".env")

    # Also resolve from source location so running from apps/api still picks repo-root .env.
    source = Path(__file__).resolve()
    for parent in source.parents[:5]:
        _push(parent / ".env")

    for env_file in candidates:
        if not env_file.exists():
            continue

        for line in env_file.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


_load_env_file()


@dataclass(frozen=True)
class Settings:
    app_env: str
    cors_origins: str
    cors_origin_regex: str
    database_url: str
    store_backend: str
    redis_url: str
    queue_backend: str
    queue_key: str
    embedded_worker_enabled: bool
    embedded_worker_concurrency: int
    worker_max_retries: int
    worker_retry_backoff_initial_seconds: int
    worker_retry_backoff_max_seconds: int
    worker_job_lock_ttl_seconds: int
    auth_enabled: bool
    auth_tokens_json: str
    auth_dev_token: str
    auth_signing_secret: str
    auth_token_ttl_seconds: int
    auth_default_workspace_id: str
    auth_default_user_id: str
    rate_limit_enabled: bool
    rate_limit_backend: str
    rate_limit_key_prefix: str
    rate_limit_requests_per_window: int
    rate_limit_window_seconds: int
    export_storage_backend: str
    export_local_dir: str
    export_signed_url_ttl_seconds: int
    export_signing_secret: str
    app_base_url: str
    s3_bucket: str
    s3_region: str
    s3_endpoint_url: str
    s3_access_key_id: str
    s3_secret_access_key: str
    scrape_timeout_seconds: int
    playwright_fallback_enabled: bool
    playwright_timeout_seconds: int
    duplicate_row_cutoff_ratio: float
    max_consecutive_low_yield_pages: int
    ai_planner_enabled: bool
    ai_provider: str
    ai_api_key: str
    ai_model: str
    ai_timeout_seconds: int
    ai_max_sample_rows: int
    ai_max_chars_per_value: int
    ai_max_input_chars: int
    ai_max_estimated_input_tokens: int
    ai_max_output_tokens: int
    ai_labeling_prompt: str
    data_retention_days: int
    startup_cleanup_enabled: bool


def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


settings = Settings(
    app_env=os.getenv("APP_ENV", "local"),
    cors_origins=os.getenv("CORS_ORIGINS", "http://localhost:3000"),
    cors_origin_regex=os.getenv(
        "CORS_ORIGIN_REGEX",
        r"^https?://(localhost|127\.0\.0\.1|0\.0\.0\.0|192\.168\.\d{1,3}\.\d{1,3})(:\d+)?$",
    )
    if os.getenv("APP_ENV", "local") == "local"
    else os.getenv("CORS_ORIGIN_REGEX", ""),
    database_url=os.getenv("DATABASE_URL", ""),
    store_backend=os.getenv("APP_STORE_BACKEND", "auto").lower(),
    redis_url=os.getenv("REDIS_URL", ""),
    queue_backend=os.getenv("APP_QUEUE_BACKEND", "auto").lower(),
    queue_key=os.getenv("APP_QUEUE_KEY", "queue:scrape:jobs"),
    embedded_worker_enabled=_as_bool(os.getenv("APP_EMBEDDED_WORKER"), default=True),
    embedded_worker_concurrency=int(os.getenv("APP_EMBEDDED_WORKER_CONCURRENCY", "1")),
    worker_max_retries=int(os.getenv("APP_WORKER_MAX_RETRIES", "3")),
    worker_retry_backoff_initial_seconds=int(os.getenv("APP_WORKER_RETRY_BACKOFF_INITIAL_SECONDS", "2")),
    worker_retry_backoff_max_seconds=int(os.getenv("APP_WORKER_RETRY_BACKOFF_MAX_SECONDS", "30")),
    worker_job_lock_ttl_seconds=int(os.getenv("APP_WORKER_JOB_LOCK_TTL_SECONDS", "300")),
    auth_enabled=_as_bool(os.getenv("APP_AUTH_ENABLED"), default=False),
    auth_tokens_json=os.getenv("APP_AUTH_TOKENS_JSON", "").strip(),
    auth_dev_token=os.getenv("APP_AUTH_DEV_TOKEN", "dev-token").strip(),
    auth_signing_secret=os.getenv("APP_AUTH_SIGNING_SECRET", "change-me-local-auth-secret").strip(),
    auth_token_ttl_seconds=int(os.getenv("APP_AUTH_TOKEN_TTL_SECONDS", "86400")),
    auth_default_workspace_id=os.getenv("APP_AUTH_DEFAULT_WORKSPACE_ID", "ws_default").strip(),
    auth_default_user_id=os.getenv("APP_AUTH_DEFAULT_USER_ID", "user_local").strip(),
    rate_limit_enabled=_as_bool(os.getenv("APP_RATE_LIMIT_ENABLED"), default=True),
    rate_limit_backend=os.getenv("APP_RATE_LIMIT_BACKEND", "auto").strip().lower(),
    rate_limit_key_prefix=os.getenv("APP_RATE_LIMIT_KEY_PREFIX", "ratelimit:workspace").strip(),
    rate_limit_requests_per_window=int(os.getenv("APP_RATE_LIMIT_REQUESTS_PER_WINDOW", "120")),
    rate_limit_window_seconds=int(os.getenv("APP_RATE_LIMIT_WINDOW_SECONDS", "60")),
    export_storage_backend=os.getenv("APP_EXPORT_STORAGE_BACKEND", "local").lower(),
    export_local_dir=os.getenv("APP_EXPORT_LOCAL_DIR", "./exports"),
    export_signed_url_ttl_seconds=int(os.getenv("APP_EXPORT_SIGNED_URL_TTL_SECONDS", "900")),
    export_signing_secret=os.getenv("APP_EXPORT_SIGNING_SECRET", "change-me-local-signing-secret"),
    app_base_url=os.getenv("APP_BASE_URL", "http://localhost:8000"),
    s3_bucket=os.getenv("APP_S3_BUCKET", "").strip(),
    s3_region=os.getenv("APP_S3_REGION", "").strip(),
    s3_endpoint_url=os.getenv("APP_S3_ENDPOINT_URL", "").strip(),
    s3_access_key_id=os.getenv("APP_S3_ACCESS_KEY_ID", "").strip(),
    s3_secret_access_key=os.getenv("APP_S3_SECRET_ACCESS_KEY", "").strip(),
    scrape_timeout_seconds=int(os.getenv("SCRAPE_TIMEOUT_SECONDS", "12")),
    playwright_fallback_enabled=_as_bool(os.getenv("APP_PLAYWRIGHT_FALLBACK"), default=True),
    playwright_timeout_seconds=int(os.getenv("APP_PLAYWRIGHT_TIMEOUT_SECONDS", "20")),
    duplicate_row_cutoff_ratio=float(os.getenv("APP_DUPLICATE_ROW_CUTOFF_RATIO", "0.8")),
    max_consecutive_low_yield_pages=int(os.getenv("APP_MAX_CONSECUTIVE_LOW_YIELD_PAGES", "1")),
    ai_planner_enabled=_as_bool(os.getenv("APP_AI_PLANNER_ENABLED"), default=True),
    ai_provider=os.getenv("APP_AI_PROVIDER", "gemini").lower(),
    ai_api_key=os.getenv("AI_API_KEY", "").strip(),
    ai_model=os.getenv("APP_AI_MODEL", "gemini-2.0-flash"),
    ai_timeout_seconds=int(os.getenv("APP_AI_TIMEOUT_SECONDS", "12")),
    ai_max_sample_rows=int(os.getenv("APP_AI_MAX_SAMPLE_ROWS", "8")),
    ai_max_chars_per_value=int(os.getenv("APP_AI_MAX_CHARS_PER_VALUE", "120")),
    ai_max_input_chars=int(os.getenv("APP_AI_MAX_INPUT_CHARS", "4000")),
    ai_max_estimated_input_tokens=int(os.getenv("APP_AI_MAX_ESTIMATED_INPUT_TOKENS", "1200")),
    ai_max_output_tokens=int(os.getenv("APP_AI_MAX_OUTPUT_TOKENS", "350")),
    ai_labeling_prompt=os.getenv("APP_AI_LABELING_PROMPT", ""),
    data_retention_days=int(os.getenv("APP_DATA_RETENTION_DAYS", "30")),
    startup_cleanup_enabled=_as_bool(os.getenv("APP_STARTUP_CLEANUP_ENABLED"), default=True),
)
