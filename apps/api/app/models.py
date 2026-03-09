from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, HttpUrl


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


class JobMode(str, Enum):
    preview = "preview"
    full = "full"


class JobStatus(str, Enum):
    queued = "queued"
    retrying = "retrying"
    request_received = "request_received"
    page_loaded = "page_loaded"
    structure_detected = "structure_detected"
    fields_planned = "fields_planned"
    extraction_running = "extraction_running"
    pagination_running = "pagination_running"
    normalization_complete = "normalization_complete"
    export_ready = "export_ready"
    success = "success"
    partial_success = "partial_success"
    cancelled = "cancelled"
    failed = "failed"


class FieldInfo(BaseModel):
    name: str
    kind: str
    confidence: float = Field(ge=0.0, le=1.0)


class PreviewRequest(BaseModel):
    project_id: str
    url: HttpUrl
    prompt: str | None = None
    max_rows: int = Field(default=20, ge=1, le=100)
    template_id: str | None = None
    extension_dom_payload: dict[str, Any] | None = None


class PreviewResponse(BaseModel):
    job_id: str
    status: JobStatus
    page_type: str
    fields: list[FieldInfo]
    rows: list[dict[str, Any]]
    warnings: list[str] = Field(default_factory=list)


class RunRequest(BaseModel):
    project_id: str
    url: HttpUrl
    prompt: str | None = None
    max_pages: int = Field(default=10, ge=1, le=100)
    max_rows: int = Field(default=500, ge=1, le=5000)
    template_id: str | None = None


class RunResponse(BaseModel):
    job_id: str
    status: JobStatus


class AuthSignupRequest(BaseModel):
    email: str
    name: str
    password: str = Field(min_length=8, max_length=256)


class AuthLoginRequest(BaseModel):
    email: str
    password: str = Field(min_length=1, max_length=256)


class AuthUserProfile(BaseModel):
    user_id: str
    email: str
    name: str
    workspace_id: str
    project_ids: list[str]
    default_project_id: str
    role: str = "member"


class AuthTokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    user: AuthUserProfile


class RowClassification(BaseModel):
    row_index: int
    label: str
    confidence: float = Field(ge=0.0, le=1.0)


class JobInsightsResponse(BaseModel):
    job_id: str
    summary: str
    row_classifications: list[RowClassification] = Field(default_factory=list)
    label_counts: dict[str, int] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    used_ai: bool = False


class UserAccountRecord(BaseModel):
    user_id: str
    email: str
    name: str
    workspace_id: str
    project_id: str
    password_hash: str
    created_at: datetime = Field(default_factory=now_utc)


class JobProgress(BaseModel):
    pages_processed: int = 0
    rows_extracted: int = 0


class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    progress: JobProgress
    error: dict[str, str] | None = None


class RowsResponse(BaseModel):
    job_id: str
    total_rows: int
    rows: list[dict[str, Any]]


class TemplateCreateRequest(BaseModel):
    project_id: str
    domain: str
    page_type: str
    template: dict[str, Any]
    page_fingerprint: str | None = None
    parent_template_id: str | None = None


class TemplateRecord(BaseModel):
    template_id: str
    domain: str
    page_type: str
    page_fingerprint: str | None = None
    template: dict[str, Any]
    version: int = 1
    parent_template_id: str | None = None
    success_count: int = 0
    failure_count: int = 0
    success_rate: float = 0.0
    last_verified_at: datetime | None = None
    invalidated: bool = False
    invalidation_reason: str | None = None
    created_at: datetime = Field(default_factory=now_utc)
    updated_at: datetime = Field(default_factory=now_utc)


class TemplateCreateResponse(BaseModel):
    template_id: str
    status: str


class ExportRequest(BaseModel):
    format: str = Field(pattern="^(csv|json)$")
    selected_columns: list[str] = Field(default_factory=list)


class ExportResponse(BaseModel):
    export_id: str
    status: str
    file_url: str


class ExportRecord(BaseModel):
    export_id: str
    job_id: str
    format: str
    file_url: str
    status: str
    created_at: datetime = Field(default_factory=now_utc)
    completed_at: datetime | None = None


class UsageEventRecord(BaseModel):
    event_id: int | None = None
    workspace_id: str
    user_id: str
    event_type: str
    event_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=now_utc)


class CleanupResult(BaseModel):
    deleted_jobs: int = 0
    deleted_rows: int = 0
    deleted_exports: int = 0
    deleted_usage_events: int = 0
    deleted_invalidated_templates: int = 0


class JobRecord(BaseModel):
    job_id: str
    project_id: str
    mode: JobMode
    status: JobStatus
    input_url: str
    prompt: str | None = None
    max_pages: int = 1
    max_rows: int = 20
    rows: list[dict[str, Any]] = Field(default_factory=list)
    fields: list[FieldInfo] = Field(default_factory=list)
    page_type: str = "listing"
    warnings: list[str] = Field(default_factory=list)
    progress: JobProgress = Field(default_factory=JobProgress)
    created_at: datetime = Field(default_factory=now_utc)
