from contextlib import asynccontextmanager
from pathlib import Path
from threading import Lock
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, RedirectResponse

from app.config import settings
from app.models import (
    AuthLoginRequest,
    AuthSignupRequest,
    AuthTokenResponse,
    AuthUserProfile,
    ExportRecord,
    ExportRequest,
    ExportResponse,
    JobMode,
    JobInsightsResponse,
    JobRecord,
    RowClassification,
    JobStatus,
    PreviewRequest,
    PreviewResponse,
    RunRequest,
    RunResponse,
    TemplateCreateRequest,
    TemplateCreateResponse,
    TemplateRecord,
    UsageEventRecord,
    now_utc,
)
from app.observability import MetricsCollector, RequestObservation, log_request_event
from app.queue import JobQueue, RunJobMessage, create_queue
from app.rate_limiter import RateLimitResult, RateLimiter, create_rate_limiter
from app.security import (
    AuthIdentity,
    bind_identity,
    ensure_admin,
    ensure_project_access,
    hash_password,
    issue_user_token,
    parse_auth_tokens,
    request_identity,
    resolve_identity,
    verify_password,
)
from app.services.ai_planner import AIPlannerConfig, apply_ai_field_labels
from app.services.ai_planner import generate_ai_insights
from app.services.exporter import ExportStorage, ExportStorageSettings, create_export_storage, render_export
from app.services.fetcher import PageFetchError, fetch_page_html
from app.services.extractor import infer_fields, scrape_preview, transform_rows_for_prompt_schema
from app.services.template_engine import (
    apply_template_extract_rows,
    compute_page_fingerprint,
    fields_from_template,
    normalize_domain,
)
from app.store import Store, create_store, normalize_email
from app.worker import WorkerHandle, start_embedded_worker, stop_embedded_worker


_worker_lock = Lock()
_worker_handle: WorkerHandle | None = None
store: Store = create_store(settings.store_backend, settings.database_url)
job_queue: JobQueue = create_queue(settings.queue_backend, settings.redis_url, settings.queue_key)
rate_limiter: RateLimiter = create_rate_limiter(
    rate_limit_backend=settings.rate_limit_backend,
    redis_url=settings.redis_url,
    key_prefix=settings.rate_limit_key_prefix,
)
metrics = MetricsCollector()
auth_registry = parse_auth_tokens(settings.auth_tokens_json)
export_storage: ExportStorage = create_export_storage(
    ExportStorageSettings(
        backend=settings.export_storage_backend,
        app_base_url=settings.app_base_url,
        local_dir=settings.export_local_dir,
        signing_secret=settings.export_signing_secret,
        signed_url_ttl_seconds=settings.export_signed_url_ttl_seconds,
        s3_bucket=settings.s3_bucket,
        s3_region=settings.s3_region,
        s3_endpoint_url=settings.s3_endpoint_url,
        s3_access_key_id=settings.s3_access_key_id,
        s3_secret_access_key=settings.s3_secret_access_key,
    )
)


def ensure_worker_started() -> None:
    global _worker_handle
    if not settings.embedded_worker_enabled:
        return

    if _worker_handle is not None and _worker_handle.is_alive():
        return

    with _worker_lock:
        if _worker_handle is None or not _worker_handle.is_alive():
            _worker_handle = start_embedded_worker(
                store=store,
                queue=job_queue,
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


def _extract_extension_html(extension_dom_payload: dict[str, object] | None) -> str | None:
    if not extension_dom_payload:
        return None

    html = extension_dom_payload.get("html")
    if isinstance(html, str) and html.strip():
        return html
    return None


def _dedupe_strings(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _identity(request: Request) -> AuthIdentity:
    return request_identity(
        request=request,
        default_user_id=settings.auth_default_user_id,
        default_workspace_id=settings.auth_default_workspace_id,
    )


def _is_public_path(path: str) -> bool:
    if path == "/health":
        return True
    if path in {"/api/v1/auth/signup", "/api/v1/auth/login"}:
        return True
    if path.startswith("/api/v1/exports/") and path.endswith("/download"):
        return True
    return False


def _is_valid_email(email: str) -> bool:
    if "@" not in email:
        return False
    local, _, domain = email.partition("@")
    return bool(local and "." in domain)


def _profile_from_identity(identity: AuthIdentity) -> AuthUserProfile:
    account = store.get_user_account_by_user_id(identity.user_id)
    project_ids = list(identity.project_ids) if identity.project_ids else ["*"]
    default_project_id = ""
    if project_ids and project_ids[0] != "*":
        default_project_id = project_ids[0]
    elif account is not None:
        default_project_id = account.project_id

    if account is None:
        return AuthUserProfile(
            user_id=identity.user_id,
            email="",
            name=identity.user_id,
            workspace_id=identity.workspace_id,
            project_ids=project_ids,
            default_project_id=default_project_id,
            role=identity.role,
        )

    return AuthUserProfile(
        user_id=account.user_id,
        email=account.email,
        name=account.name,
        workspace_id=account.workspace_id,
        project_ids=project_ids,
        default_project_id=default_project_id or account.project_id,
        role=identity.role,
    )


def _rate_limit_scope(identity: AuthIdentity, method: str, path: str) -> str:
    return f"{identity.workspace_id}:{method}:{path}"


def _authorize_project(request: Request, project_id: str) -> None:
    ensure_project_access(_identity(request), project_id)


def _serialize_job_summary(job: JobRecord) -> dict[str, object]:
    return {
        "job_id": job.job_id,
        "project_id": job.project_id,
        "mode": job.mode,
        "status": job.status,
        "input_url": job.input_url,
        "page_type": job.page_type,
        "progress": job.progress,
        "created_at": job.created_at,
    }


def _serialize_job_detail(job: JobRecord) -> dict[str, object]:
    return {
        "job_id": job.job_id,
        "project_id": job.project_id,
        "mode": job.mode,
        "status": job.status,
        "input_url": job.input_url,
        "prompt": job.prompt,
        "max_pages": job.max_pages,
        "max_rows": job.max_rows,
        "page_type": job.page_type,
        "fields": job.fields,
        "warnings": job.warnings,
        "progress": job.progress,
        "created_at": job.created_at,
    }


def _record_usage(
    event_type: str,
    event_json: dict[str, object],
    workspace_id: str = "ws_default",
    user_id: str = "user_system",
    request: Request | None = None,
) -> None:
    if request is not None:
        identity = _identity(request)
        workspace_id = identity.workspace_id
        user_id = identity.user_id
    try:
        store.record_usage_event(
            UsageEventRecord(
                workspace_id=workspace_id,
                user_id=user_id,
                event_type=event_type,
                event_json=event_json,
            )
        )
    except Exception:
        # Usage telemetry should never block core flows.
        pass


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_worker_started()
    if settings.startup_cleanup_enabled:
        try:
            store.cleanup_old_data(retention_days=settings.data_retention_days)
        except Exception:
            pass
    global _worker_handle
    try:
        yield
    finally:
        if _worker_handle is not None:
            stop_embedded_worker(_worker_handle)
            _worker_handle = None


app = FastAPI(title="WebScrapper API", version="0.3.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in settings.cors_origins.split(",") if origin.strip()],
    allow_origin_regex=settings.cors_origin_regex or None,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def auth_rate_limit_and_observability(request: Request, call_next):
    path = request.url.path
    method = request.method
    request_id = request.headers.get("X-Request-Id", "").strip() or uuid4().hex
    trace_id = request.headers.get("X-Trace-Id", "").strip() or uuid4().hex
    request.state.request_id = request_id
    request.state.trace_id = trace_id

    # Let CORS preflight flow through without auth/rate-limit checks.
    if method.upper() == "OPTIONS":
        response = await call_next(request)
        response.headers["X-Request-Id"] = request_id
        response.headers["X-Trace-Id"] = trace_id
        return response

    identity = _identity(request)
    rate_limit_result: RateLimitResult | None = None
    start = perf_counter()

    if not _is_public_path(path):
        try:
            identity = resolve_identity(
                request=request,
                auth_enabled=settings.auth_enabled,
                auth_registry=auth_registry,
                auth_dev_token=settings.auth_dev_token,
                auth_signing_secret=settings.auth_signing_secret,
                default_user_id=settings.auth_default_user_id,
                default_workspace_id=settings.auth_default_workspace_id,
            )
            bind_identity(request, identity)
        except HTTPException as exc:
            metrics.observe_auth_failed(path)
            response = JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
            duration_ms = (perf_counter() - start) * 1000
            response.headers["X-Request-Id"] = request_id
            response.headers["X-Trace-Id"] = trace_id
            log_request_event(
                request_id=request_id,
                method=method,
                path=path,
                status_code=exc.status_code,
                duration_ms=duration_ms,
                trace_id=trace_id,
                workspace_id=identity.workspace_id,
                user_id=identity.user_id,
            )
            metrics.observe_request(
                RequestObservation(method=method, path=path, status_code=exc.status_code, duration_ms=duration_ms)
            )
            return response

        if settings.rate_limit_enabled and path.startswith("/api/v1"):
            rate_limit_result = rate_limiter.check(
                key=_rate_limit_scope(identity=identity, method=method, path=path),
                limit=settings.rate_limit_requests_per_window,
                window_seconds=settings.rate_limit_window_seconds,
            )
            if not rate_limit_result.allowed:
                metrics.observe_rate_limit_block(path)
                response = JSONResponse(
                    status_code=429,
                    content={
                        "detail": {
                            "code": "rate_limited",
                            "message": "Too many requests in current window",
                        }
                    },
                )
                response.headers["X-RateLimit-Limit"] = str(settings.rate_limit_requests_per_window)
                response.headers["X-RateLimit-Remaining"] = str(rate_limit_result.remaining)
                response.headers["X-RateLimit-Reset"] = str(rate_limit_result.reset_seconds)
                response.headers["Retry-After"] = str(rate_limit_result.reset_seconds)
                duration_ms = (perf_counter() - start) * 1000
                response.headers["X-Request-Id"] = request_id
                response.headers["X-Trace-Id"] = trace_id
                log_request_event(
                    request_id=request_id,
                    method=method,
                    path=path,
                    status_code=429,
                    duration_ms=duration_ms,
                    trace_id=trace_id,
                    workspace_id=identity.workspace_id,
                    user_id=identity.user_id,
                )
                metrics.observe_request(
                    RequestObservation(method=method, path=path, status_code=429, duration_ms=duration_ms)
                )
                return response
    else:
        bind_identity(request, identity)

    response = await call_next(request)
    duration_ms = (perf_counter() - start) * 1000

    if rate_limit_result is not None:
        response.headers["X-RateLimit-Limit"] = str(settings.rate_limit_requests_per_window)
        response.headers["X-RateLimit-Remaining"] = str(rate_limit_result.remaining)
        response.headers["X-RateLimit-Reset"] = str(rate_limit_result.reset_seconds)

    response.headers["X-Request-Id"] = request_id
    response.headers["X-Trace-Id"] = trace_id
    log_request_event(
        request_id=request_id,
        method=method,
        path=path,
        status_code=response.status_code,
        duration_ms=duration_ms,
        trace_id=trace_id,
        workspace_id=identity.workspace_id,
        user_id=identity.user_id,
    )
    metrics.observe_request(
        RequestObservation(method=method, path=path, status_code=response.status_code, duration_ms=duration_ms)
    )
    return response


@app.get("/health")
def health() -> dict[str, str]:
    worker_state = "running" if _worker_handle and _worker_handle.is_alive() else "stopped"
    return {
        "status": "ok",
        "env": settings.app_env,
        "store": store.backend,
        "queue": job_queue.backend,
        "export_storage": export_storage.backend,
        "embedded_worker": worker_state,
        "embedded_worker_concurrency": str(settings.embedded_worker_concurrency),
        "worker_max_retries": str(settings.worker_max_retries),
        "playwright_fallback": "enabled" if settings.playwright_fallback_enabled else "disabled",
        "duplicate_row_cutoff_ratio": f"{settings.duplicate_row_cutoff_ratio:.2f}",
        "ai_planner": "enabled" if settings.ai_planner_enabled else "disabled",
        "data_retention_days": str(settings.data_retention_days),
    }


@app.get("/metrics")
def metrics_endpoint(request: Request):
    ensure_admin(_identity(request))
    return PlainTextResponse(metrics.render_prometheus(), media_type="text/plain; version=0.0.4")


@app.post("/api/v1/auth/signup", response_model=AuthTokenResponse)
def auth_signup(payload: AuthSignupRequest) -> AuthTokenResponse:
    email = normalize_email(payload.email)
    if not _is_valid_email(email):
        raise HTTPException(status_code=400, detail={"code": "auth_invalid_email", "message": "Invalid email address"})

    safe_name = payload.name.strip() or email.split("@", 1)[0]
    try:
        password_hash = hash_password(payload.password)
        account = store.create_user_account(email=email, name=safe_name, password_hash=password_hash)
    except ValueError as exc:
        reason = str(exc)
        if reason == "email_exists":
            raise HTTPException(
                status_code=409,
                detail={"code": "auth_email_exists", "message": "An account with this email already exists"},
            ) from exc
        if reason == "signup_failed":
            raise HTTPException(
                status_code=500,
                detail={"code": "auth_signup_failed", "message": "Unable to create account. Please try again."},
            ) from exc
        raise HTTPException(status_code=400, detail={"code": "auth_signup_failed", "message": reason}) from exc

    identity = AuthIdentity(
        user_id=account.user_id,
        workspace_id=account.workspace_id,
        project_ids=(account.project_id,),
        role="member",
        token_id=f"signup_{account.user_id}",
    )
    access_token = issue_user_token(
        identity=identity,
        signing_secret=settings.auth_signing_secret,
        ttl_seconds=settings.auth_token_ttl_seconds,
    )
    return AuthTokenResponse(
        access_token=access_token,
        token_type="bearer",
        expires_in=settings.auth_token_ttl_seconds,
        user=AuthUserProfile(
            user_id=account.user_id,
            email=account.email,
            name=account.name,
            workspace_id=account.workspace_id,
            project_ids=[account.project_id],
            default_project_id=account.project_id,
            role="member",
        ),
    )


@app.post("/api/v1/auth/login", response_model=AuthTokenResponse)
def auth_login(payload: AuthLoginRequest) -> AuthTokenResponse:
    email = normalize_email(payload.email)
    if not _is_valid_email(email):
        raise HTTPException(status_code=400, detail={"code": "auth_invalid_email", "message": "Invalid email address"})

    account = store.get_user_account_by_email(email)
    if account is None or not verify_password(payload.password, account.password_hash):
        raise HTTPException(
            status_code=401,
            detail={"code": "auth_invalid_credentials", "message": "Invalid email or password"},
        )

    identity = AuthIdentity(
        user_id=account.user_id,
        workspace_id=account.workspace_id,
        project_ids=(account.project_id,),
        role="member",
        token_id=f"login_{account.user_id}",
    )
    access_token = issue_user_token(
        identity=identity,
        signing_secret=settings.auth_signing_secret,
        ttl_seconds=settings.auth_token_ttl_seconds,
    )
    return AuthTokenResponse(
        access_token=access_token,
        token_type="bearer",
        expires_in=settings.auth_token_ttl_seconds,
        user=AuthUserProfile(
            user_id=account.user_id,
            email=account.email,
            name=account.name,
            workspace_id=account.workspace_id,
            project_ids=[account.project_id],
            default_project_id=account.project_id,
            role="member",
        ),
    )


@app.get("/api/v1/auth/me", response_model=AuthUserProfile)
def auth_me(request: Request) -> AuthUserProfile:
    return _profile_from_identity(_identity(request))


@app.post("/api/v1/scrape/preview", response_model=PreviewResponse)
def scrape_preview_endpoint(payload: PreviewRequest, request: Request) -> PreviewResponse:
    _authorize_project(request, payload.project_id)
    job_id = f"job_prev_{uuid4().hex[:8]}"
    url = str(payload.url)
    pre_warnings: list[str] = []
    html = _extract_extension_html(payload.extension_dom_payload)

    if html is None:
        try:
            fetched = fetch_page_html(
                url=url,
                timeout_seconds=settings.scrape_timeout_seconds,
                allow_playwright_fallback=settings.playwright_fallback_enabled,
                playwright_timeout_seconds=settings.playwright_timeout_seconds,
            )
            html = fetched.html
            pre_warnings.extend(fetched.warnings)
            pre_warnings.append(f"source_{fetched.source}")
        except PageFetchError:
            pre_warnings.append("page_load_failed")
    else:
        pre_warnings.append("source_extension_dom")

    matched_template = None
    if html:
        page_fingerprint = compute_page_fingerprint(html)
        matched_template = store.match_template(
            domain=normalize_domain(url),
            page_type=None,
            page_fingerprint=page_fingerprint,
            template_id=payload.template_id,
        )

    if html and matched_template:
        template_rows, template_warnings = apply_template_extract_rows(
            html=html,
            base_url=url,
            template=matched_template,
            max_rows=min(payload.max_rows, 100),
        )
        if template_rows:
            store.update_template_metrics(template_id=matched_template.template_id, success=True)
            template_fields = fields_from_template(matched_template)
            if not template_fields:
                template_fields = infer_fields(payload.prompt)

            warnings = _dedupe_strings(
                pre_warnings
                + template_warnings
                + [
                    f"template_matched:{matched_template.template_id}",
                    f"template_version:{matched_template.version}",
                ]
            )

            ai_result = apply_ai_field_labels(
                config=AIPlannerConfig(
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
                prompt=payload.prompt,
                page_url=url,
                page_type=matched_template.page_type,
                fields=template_fields,
                rows=template_rows,
            )

            template_fields = ai_result.fields
            template_rows = ai_result.rows
            warnings = _dedupe_strings(warnings + ai_result.warnings)
            template_fields, template_rows, schema_warnings = transform_rows_for_prompt_schema(
                fields=template_fields,
                rows=template_rows,
                prompt=payload.prompt,
                page_url=url,
            )
            warnings = _dedupe_strings(warnings + schema_warnings)
            job = JobRecord(
                job_id=job_id,
                project_id=payload.project_id,
                mode=JobMode.preview,
                status=JobStatus.success,
                input_url=url,
                prompt=payload.prompt,
                max_pages=1,
                max_rows=payload.max_rows,
                rows=template_rows,
                fields=template_fields,
                page_type=matched_template.page_type,
                warnings=warnings,
            )
            job.progress.pages_processed = 1
            job.progress.rows_extracted = len(template_rows)
            store.upsert_job(job)
            _record_usage(
                event_type="preview.completed",
                event_json={
                    "job_id": job.job_id,
                    "project_id": job.project_id,
                    "rows": len(job.rows),
                    "template_id": matched_template.template_id,
                    "used_ai_planner": ai_result.used,
                },
                request=request,
            )

            return PreviewResponse(
                job_id=job.job_id,
                status=job.status,
                page_type=job.page_type,
                fields=job.fields,
                rows=job.rows,
                warnings=job.warnings,
            )

        store.update_template_metrics(
            template_id=matched_template.template_id,
            success=False,
            invalidation_reason="template_rows_empty",
        )
        pre_warnings.extend(template_warnings)
        pre_warnings.append("template_match_failed")

    scrape_result = scrape_preview(
        url=url,
        prompt=payload.prompt,
        max_rows=min(payload.max_rows, 100),
        timeout_seconds=settings.scrape_timeout_seconds,
        playwright_fallback_enabled=settings.playwright_fallback_enabled,
        playwright_timeout_seconds=settings.playwright_timeout_seconds,
        extension_dom_payload={"html": html} if html else payload.extension_dom_payload,
    )
    ai_result = apply_ai_field_labels(
        config=AIPlannerConfig(
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
        prompt=payload.prompt,
        page_url=url,
        page_type=scrape_result.page_type,
        fields=scrape_result.fields,
        rows=scrape_result.rows,
    )

    scrape_result.fields = ai_result.fields
    scrape_result.rows = ai_result.rows
    scrape_result.fields, scrape_result.rows, schema_warnings = transform_rows_for_prompt_schema(
        fields=scrape_result.fields,
        rows=scrape_result.rows,
        prompt=payload.prompt,
        page_url=url,
    )
    merged_warnings = _dedupe_strings(pre_warnings + scrape_result.warnings + ai_result.warnings + schema_warnings)

    job = JobRecord(
        job_id=job_id,
        project_id=payload.project_id,
        mode=JobMode.preview,
        status=JobStatus.success,
        input_url=url,
        prompt=payload.prompt,
        max_pages=1,
        max_rows=payload.max_rows,
        rows=scrape_result.rows,
        fields=scrape_result.fields,
        page_type=scrape_result.page_type,
        warnings=merged_warnings,
    )
    job.progress.pages_processed = 1
    job.progress.rows_extracted = len(scrape_result.rows)
    store.upsert_job(job)
    _record_usage(
        event_type="preview.completed",
        event_json={
            "job_id": job.job_id,
            "project_id": job.project_id,
            "rows": len(job.rows),
            "template_id": payload.template_id or "",
            "used_ai_planner": ai_result.used,
        },
        request=request,
    )

    return PreviewResponse(
        job_id=job.job_id,
        status=job.status,
        page_type=job.page_type,
        fields=job.fields,
        rows=job.rows,
        warnings=job.warnings,
    )


@app.post("/api/v1/scrape/run", response_model=RunResponse)
def scrape_run(payload: RunRequest, request: Request) -> RunResponse:
    ensure_worker_started()
    _authorize_project(request, payload.project_id)

    job_id = f"job_{uuid4().hex[:8]}"
    job = JobRecord(
        job_id=job_id,
        project_id=payload.project_id,
        mode=JobMode.full,
        status=JobStatus.queued,
        input_url=str(payload.url),
        prompt=payload.prompt,
        max_pages=payload.max_pages,
        max_rows=payload.max_rows,
        fields=infer_fields(payload.prompt),
    )
    store.upsert_job(job)

    message = RunJobMessage(
        job_id=job_id,
        project_id=payload.project_id,
        url=str(payload.url),
        prompt=payload.prompt,
        max_pages=payload.max_pages,
        max_rows=payload.max_rows,
        template_id=payload.template_id,
        max_attempts=settings.worker_max_retries,
        idempotency_key=job_id,
    )

    try:
        job_queue.enqueue(message)
    except Exception as exc:
        job.status = JobStatus.failed
        job.warnings.append("queue_enqueue_failed")
        store.upsert_job(job)
        raise HTTPException(
            status_code=500,
            detail={"code": "queue_enqueue_failed", "message": f"Failed to enqueue job: {exc}"},
        ) from exc
    _record_usage(
        event_type="run.queued",
        event_json={
            "job_id": job_id,
            "project_id": payload.project_id,
            "max_pages": payload.max_pages,
            "max_rows": payload.max_rows,
            "template_id": payload.template_id or "",
        },
        request=request,
    )

    return RunResponse(job_id=job_id, status=JobStatus.queued)


@app.get("/api/v1/jobs/{job_id}")
def get_job(job_id: str, request: Request):
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail={"code": "job_not_found", "message": "Job not found"})
    _authorize_project(request, job.project_id)
    status = store.job_status(job_id)
    if status is None:
        raise HTTPException(status_code=404, detail={"code": "job_not_found", "message": "Job not found"})
    return status


@app.get("/api/v1/jobs")
def list_jobs(
    request: Request,
    project_id: str | None = None,
    status: JobStatus | None = None,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=500),
):
    identity = _identity(request)
    scoped_project_ids: tuple[str, ...] | None = None
    if project_id:
        ensure_project_access(identity, project_id)
    elif not identity.can_access_project("*"):
        scoped_project_ids = identity.project_ids

    total, jobs = store.list_jobs(
        project_id=project_id,
        status=status,
        offset=offset,
        limit=limit,
        project_ids=scoped_project_ids,
    )
    return {
        "total_jobs": total,
        "jobs": [_serialize_job_summary(item) for item in jobs],
    }


@app.get("/api/v1/jobs/{job_id}/rows")
def get_job_rows(
    job_id: str,
    request: Request,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
):
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail={"code": "job_not_found", "message": "Job not found"})
    _authorize_project(request, job.project_id)
    rows = store.job_rows(job_id, offset=offset, limit=limit)
    if rows is None:
        raise HTTPException(status_code=404, detail={"code": "job_not_found", "message": "Job not found"})
    return rows


@app.get("/api/v1/jobs/{job_id}/detail")
def get_job_detail(job_id: str, request: Request):
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail={"code": "job_not_found", "message": "Job not found"})
    _authorize_project(request, job.project_id)
    return _serialize_job_detail(job)


@app.get("/api/v1/jobs/{job_id}/insights", response_model=JobInsightsResponse)
def get_job_insights(
    job_id: str,
    request: Request,
    max_rows: int = Query(default=30, ge=1, le=300),
) -> JobInsightsResponse:
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail={"code": "job_not_found", "message": "Job not found"})
    _authorize_project(request, job.project_id)
    if not job.rows:
        return JobInsightsResponse(
            job_id=job_id,
            summary="No rows available for summarization/classification.",
            row_classifications=[],
            label_counts={},
            warnings=[],
            used_ai=False,
        )

    result = generate_ai_insights(
        config=AIPlannerConfig(
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
        prompt=job.prompt,
        page_url=job.input_url,
        page_type=job.page_type,
        rows=job.rows,
        max_rows=max_rows,
    )

    label_counts: dict[str, int] = {}
    classifications: list[RowClassification] = []
    for item in result.row_classifications:
        label = str(item.get("label", "")).strip() or "unclassified"
        label_counts[label] = label_counts.get(label, 0) + 1
        classifications.append(
            RowClassification(
                row_index=int(item.get("row_index", 0)),
                label=label,
                confidence=float(item.get("confidence", 0.0)),
            )
        )

    _record_usage(
        event_type="ai.insights.generated",
        event_json={
            "job_id": job.job_id,
            "project_id": job.project_id,
            "rows_considered": min(len(job.rows), max_rows),
            "used_ai": result.used,
            "labels": list(label_counts.keys()),
        },
        request=request,
    )

    return JobInsightsResponse(
        job_id=job_id,
        summary=result.summary,
        row_classifications=classifications,
        label_counts=label_counts,
        warnings=result.warnings,
        used_ai=result.used,
    )


@app.post("/api/v1/jobs/{job_id}/cancel")
def cancel_job(job_id: str, request: Request):
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail={"code": "job_not_found", "message": "Job not found"})
    _authorize_project(request, job.project_id)

    if job.status in {JobStatus.success, JobStatus.partial_success, JobStatus.failed}:
        raise HTTPException(
            status_code=409,
            detail={"code": "job_not_cancellable", "message": f"Job cannot be cancelled in status '{job.status.value}'"},
        )

    if job.status == JobStatus.cancelled:
        return {"job_id": job.job_id, "status": job.status, "detail": "already_cancelled"}

    previous_status = job.status.value
    job.status = JobStatus.cancelled
    job.warnings = list(job.warnings) + ["cancelled_by_user"]
    store.upsert_job(job)
    _record_usage(
        event_type="run.cancelled",
        event_json={
            "job_id": job.job_id,
            "project_id": job.project_id,
            "previous_status": previous_status,
        },
        request=request,
    )
    return {"job_id": job.job_id, "status": job.status}


@app.post("/api/v1/jobs/{job_id}/retry", response_model=RunResponse)
def retry_job(job_id: str, request: Request) -> RunResponse:
    ensure_worker_started()
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail={"code": "job_not_found", "message": "Job not found"})
    _authorize_project(request, job.project_id)

    if job.status not in {JobStatus.failed, JobStatus.cancelled}:
        raise HTTPException(
            status_code=409,
            detail={"code": "job_not_retryable", "message": f"Job cannot be retried in status '{job.status.value}'"},
        )

    job.status = JobStatus.queued
    job.warnings = list(job.warnings) + ["retry_requested"]
    store.upsert_job(job)

    retry_message = RunJobMessage(
        job_id=job.job_id,
        project_id=job.project_id,
        url=job.input_url,
        prompt=job.prompt,
        max_pages=job.max_pages,
        max_rows=job.max_rows,
        attempt=0,
        max_attempts=settings.worker_max_retries,
        force=True,
        idempotency_key=job.job_id,
    )
    try:
        job_queue.enqueue(retry_message)
    except Exception as exc:
        job.status = JobStatus.failed
        job.warnings = list(job.warnings) + ["queue_enqueue_failed"]
        store.upsert_job(job)
        raise HTTPException(
            status_code=500,
            detail={"code": "queue_enqueue_failed", "message": f"Failed to enqueue retry job: {exc}"},
        ) from exc

    _record_usage(
        event_type="run.retry_requested",
        event_json={
            "job_id": job.job_id,
            "project_id": job.project_id,
        },
        request=request,
    )
    return RunResponse(job_id=job.job_id, status=JobStatus.queued)


@app.post("/api/v1/templates", response_model=TemplateCreateResponse)
def create_template(payload: TemplateCreateRequest, request: Request) -> TemplateCreateResponse:
    _authorize_project(request, payload.project_id)
    template_id = f"tpl_{uuid4().hex[:8]}"
    domain = payload.domain.strip().lower()
    if domain.startswith("http://") or domain.startswith("https://"):
        domain = normalize_domain(domain)
    if domain.startswith("www."):
        domain = domain[4:]

    template = TemplateRecord(
        template_id=template_id,
        domain=domain,
        page_type=payload.page_type,
        page_fingerprint=payload.page_fingerprint,
        template=payload.template,
        parent_template_id=payload.parent_template_id,
    )
    store.save_template(template)
    _record_usage(
        event_type="template.saved",
        event_json={
            "template_id": template_id,
            "project_id": payload.project_id,
            "domain": domain,
            "page_type": payload.page_type,
            "fingerprint_present": bool(payload.page_fingerprint),
        },
        request=request,
    )
    return TemplateCreateResponse(template_id=template_id, status="saved")


@app.get("/api/v1/templates")
def list_templates(domain: str | None = None, page_type: str | None = None):
    templates = list(store.list_templates(domain=domain, page_type=page_type))
    return {"templates": templates}


@app.post("/api/v1/export/{job_id}", response_model=ExportResponse)
def export_job(job_id: str, payload: ExportRequest, request: Request) -> ExportResponse:
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail={"code": "job_not_found", "message": "Job not found"})
    _authorize_project(request, job.project_id)
    if job.status not in {JobStatus.success, JobStatus.partial_success}:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "job_not_exportable",
                "message": f"Job cannot be exported in status '{job.status.value}'",
            },
        )
    if not job.rows:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "export_no_rows",
                "message": "Job has no extracted rows to export",
            },
        )

    rendered = render_export(rows=job.rows, export_format=payload.format, selected_columns=payload.selected_columns)
    export_id = f"exp_{uuid4().hex[:8]}"
    try:
        stored = export_storage.store(
            export_id=export_id,
            job_id=job_id,
            export_format=payload.format,
            content=rendered.content,
            content_type=rendered.content_type,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={"code": "export_storage_failed", "message": f"Failed to store export artifact: {exc}"},
        ) from exc

    store.save_export(
        ExportRecord(
            export_id=export_id,
            job_id=job_id,
            format=payload.format,
            file_url=stored.storage_ref,
            status="ready",
            created_at=now_utc(),
            completed_at=now_utc(),
        )
    )
    _record_usage(
        event_type="export.created",
        event_json={
            "export_id": export_id,
            "job_id": job_id,
            "format": payload.format,
            "selected_columns": payload.selected_columns,
            "rows": rendered.rows_count,
            "bytes_written": stored.bytes_written,
            "storage_backend": export_storage.backend,
        },
        request=request,
    )
    return ExportResponse(export_id=export_id, status="ready", file_url=stored.signed_url)


@app.get("/api/v1/exports")
def list_exports(
    request: Request,
    job_id: str | None = None,
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
):
    identity = _identity(request)

    if job_id:
        target_job = store.get_job(job_id)
        if target_job is not None:
            ensure_project_access(identity, target_job.project_id)

    exports = list(store.list_exports(job_id=job_id))
    if job_id is None and not identity.can_access_project("*"):
        allowed_exports: list[ExportRecord] = []
        for item in exports:
            job = store.get_job(item.job_id)
            if job is not None and identity.can_access_project(job.project_id):
                allowed_exports.append(item)
        exports = allowed_exports

    paged = exports[offset : offset + limit]
    serialized = []
    for item in paged:
        payload = item.model_dump(mode="json")
        try:
            payload["file_url"] = export_storage.resolve_signed_url(export_id=item.export_id, storage_ref=item.file_url)
        except Exception:
            payload["file_url"] = item.file_url
        serialized.append(payload)
    return {
        "total_exports": len(exports),
        "exports": serialized,
    }


@app.get("/api/v1/exports/{export_id}/download")
def download_export(export_id: str, expires: int | None = None, token: str | None = None):
    export = store.get_export(export_id)
    if export is None:
        raise HTTPException(status_code=404, detail={"code": "export_not_found", "message": "Export not found"})

    if export_storage.backend == "local":
        if expires is None or not token or not export_storage.verify_download_token(export_id=export_id, expires=expires, token=token):
            raise HTTPException(status_code=403, detail={"code": "download_token_invalid", "message": "Invalid or expired token"})

        local_path = export_storage.local_path_from_ref(export.file_url)
        if local_path is None:
            raise HTTPException(status_code=404, detail={"code": "export_artifact_missing", "message": "Export artifact not found"})
        file_path = Path(local_path)
        if not file_path.exists():
            raise HTTPException(status_code=404, detail={"code": "export_artifact_missing", "message": "Export artifact not found"})

        _record_usage(
            event_type="export.downloaded",
            event_json={
                "export_id": export_id,
                "job_id": export.job_id,
                "format": export.format,
            },
        )
        return FileResponse(
            path=str(file_path),
            media_type="text/csv; charset=utf-8" if export.format == "csv" else "application/json",
            filename=f"{export.export_id}.{export.format}",
        )

    try:
        signed_url = export_storage.resolve_signed_url(export_id=export.export_id, storage_ref=export.file_url)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail={"code": "export_signed_url_failed", "message": f"Failed to generate signed URL: {exc}"},
        ) from exc

    _record_usage(
        event_type="export.download_redirected",
        event_json={
            "export_id": export_id,
            "job_id": export.job_id,
            "format": export.format,
            "storage_backend": export_storage.backend,
        },
    )
    return RedirectResponse(url=signed_url, status_code=307)


@app.post("/api/v1/maintenance/cleanup")
def run_cleanup(request: Request, retention_days: int = Query(default=settings.data_retention_days, ge=0, le=3650)):
    ensure_admin(_identity(request))
    result = store.cleanup_old_data(retention_days=retention_days)
    _record_usage(
        event_type="maintenance.cleanup",
        event_json={
            "retention_days": retention_days,
            "deleted_jobs": result.deleted_jobs,
            "deleted_rows": result.deleted_rows,
            "deleted_exports": result.deleted_exports,
            "deleted_usage_events": result.deleted_usage_events,
            "deleted_invalidated_templates": result.deleted_invalidated_templates,
        },
        request=request,
    )
    return {
        "retention_days": retention_days,
        "result": result,
    }
