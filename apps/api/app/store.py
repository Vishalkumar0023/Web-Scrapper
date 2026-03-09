from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Protocol
from uuid import uuid4

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    create_engine,
    delete,
    func,
    select,
)
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from app.models import (
    CleanupResult,
    ExportRecord,
    FieldInfo,
    JobProgress,
    JobRecord,
    JobStatus,
    JobStatusResponse,
    RowsResponse,
    TemplateRecord,
    UserAccountRecord,
    UsageEventRecord,
)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def normalize_email(email: str) -> str:
    return email.strip().lower()


class Store(Protocol):
    backend: str

    def upsert_job(self, job: JobRecord) -> None: ...

    def get_job(self, job_id: str) -> JobRecord | None: ...

    def list_jobs(
        self,
        project_id: str | None = None,
        status: JobStatus | None = None,
        offset: int = 0,
        limit: int = 100,
        project_ids: tuple[str, ...] | None = None,
    ) -> tuple[int, list[JobRecord]]: ...

    def job_status(self, job_id: str) -> JobStatusResponse | None: ...

    def job_rows(self, job_id: str, offset: int, limit: int) -> RowsResponse | None: ...

    def save_template(self, template: TemplateRecord) -> None: ...

    def list_templates(self, domain: str | None = None, page_type: str | None = None) -> Iterable[TemplateRecord]: ...

    def get_template(self, template_id: str) -> TemplateRecord | None: ...

    def match_template(
        self,
        domain: str,
        page_type: str | None,
        page_fingerprint: str | None,
        template_id: str | None = None,
    ) -> TemplateRecord | None: ...

    def update_template_metrics(
        self,
        template_id: str,
        success: bool,
        invalidation_reason: str | None = None,
    ) -> TemplateRecord | None: ...

    def save_export(self, export: ExportRecord) -> None: ...

    def list_exports(self, job_id: str | None = None) -> Iterable[ExportRecord]: ...

    def get_export(self, export_id: str) -> ExportRecord | None: ...

    def record_usage_event(self, event: UsageEventRecord) -> UsageEventRecord: ...

    def cleanup_old_data(self, retention_days: int) -> CleanupResult: ...

    def create_user_account(self, email: str, name: str, password_hash: str) -> UserAccountRecord: ...

    def get_user_account_by_email(self, email: str) -> UserAccountRecord | None: ...

    def get_user_account_by_user_id(self, user_id: str) -> UserAccountRecord | None: ...


class InMemoryStore:
    backend = "memory"

    def __init__(self) -> None:
        self._jobs: dict[str, JobRecord] = {}
        self._templates: dict[str, TemplateRecord] = {}
        self._exports: dict[str, ExportRecord] = {}
        self._usage_events: list[UsageEventRecord] = []
        self._accounts_by_user_id: dict[str, UserAccountRecord] = {}
        self._account_user_by_email: dict[str, str] = {}
        self._event_counter = 0
        self._lock = Lock()

    def upsert_job(self, job: JobRecord) -> None:
        with self._lock:
            self._jobs[job.job_id] = job

    def get_job(self, job_id: str) -> JobRecord | None:
        return self._jobs.get(job_id)

    def list_jobs(
        self,
        project_id: str | None = None,
        status: JobStatus | None = None,
        offset: int = 0,
        limit: int = 100,
        project_ids: tuple[str, ...] | None = None,
    ) -> tuple[int, list[JobRecord]]:
        values = list(self._jobs.values())
        if project_id:
            values = [item for item in values if item.project_id == project_id]
        elif project_ids:
            allowed = set(project_ids)
            values = [item for item in values if item.project_id in allowed]
        if status:
            values = [item for item in values if item.status == status]

        values.sort(key=lambda item: item.created_at, reverse=True)
        total = len(values)
        start = max(0, offset)
        end = start + max(1, limit)
        paged = values[start:end]
        return total, paged

    def job_status(self, job_id: str) -> JobStatusResponse | None:
        job = self.get_job(job_id)
        if not job:
            return None

        error = None
        if job.status == JobStatus.failed:
            error = {
                "code": "internal_error",
                "message": "Job failed during processing",
            }

        return JobStatusResponse(
            job_id=job.job_id,
            status=job.status,
            progress=job.progress,
            error=error,
        )

    def job_rows(self, job_id: str, offset: int, limit: int) -> RowsResponse | None:
        job = self.get_job(job_id)
        if not job:
            return None

        sliced = job.rows[offset : offset + limit]
        return RowsResponse(job_id=job.job_id, total_rows=len(job.rows), rows=sliced)

    def save_template(self, template: TemplateRecord) -> None:
        with self._lock:
            existing = self._templates.get(template.template_id)
            lineage_templates = [
                item
                for item in self._templates.values()
                if item.domain == template.domain
                and item.page_type == template.page_type
                and item.page_fingerprint == template.page_fingerprint
            ]
            next_version = max((item.version for item in lineage_templates), default=0) + 1
            template.version = max(template.version, next_version)

            if template.parent_template_id is None and lineage_templates:
                template.parent_template_id = max(lineage_templates, key=lambda item: item.version).template_id

            if existing is not None:
                template.updated_at = now_utc()
            self._templates[template.template_id] = template

    def list_templates(self, domain: str | None = None, page_type: str | None = None) -> Iterable[TemplateRecord]:
        values: Iterable[TemplateRecord] = self._templates.values()

        if domain:
            values = [t for t in values if t.domain == domain]
        if page_type:
            values = [t for t in values if t.page_type == page_type]

        ordered = sorted(values, key=lambda item: (item.success_rate, item.version, item.updated_at), reverse=True)
        return ordered

    def get_template(self, template_id: str) -> TemplateRecord | None:
        return self._templates.get(template_id)

    def match_template(
        self,
        domain: str,
        page_type: str | None,
        page_fingerprint: str | None,
        template_id: str | None = None,
    ) -> TemplateRecord | None:
        if template_id:
            template = self.get_template(template_id)
            if template and not template.invalidated:
                return template
            return None

        candidates = [item for item in self._templates.values() if item.domain == domain and not item.invalidated]
        if page_type:
            candidates = [item for item in candidates if item.page_type == page_type]

        if page_fingerprint:
            exact = [item for item in candidates if item.page_fingerprint == page_fingerprint]
            if exact:
                return max(exact, key=lambda item: (item.success_rate, item.version, item.updated_at))

        if candidates:
            return max(candidates, key=lambda item: (item.success_rate, item.version, item.updated_at))
        return None

    def update_template_metrics(
        self,
        template_id: str,
        success: bool,
        invalidation_reason: str | None = None,
    ) -> TemplateRecord | None:
        with self._lock:
            template = self._templates.get(template_id)
            if template is None:
                return None

            if success:
                template.success_count += 1
                template.last_verified_at = now_utc()
                template.invalidated = False
                template.invalidation_reason = None
            else:
                template.failure_count += 1
                if invalidation_reason:
                    template.invalidation_reason = invalidation_reason
                if template.failure_count >= 3:
                    template.invalidated = True
                    template.invalidation_reason = template.invalidation_reason or "repeated_template_failures"

            attempts = template.success_count + template.failure_count
            template.success_rate = (template.success_count / attempts) if attempts else 0.0
            template.updated_at = now_utc()
            self._templates[template_id] = template
            return template

    def save_export(self, export: ExportRecord) -> None:
        with self._lock:
            self._exports[export.export_id] = export

    def list_exports(self, job_id: str | None = None) -> Iterable[ExportRecord]:
        exports = list(self._exports.values())
        if job_id:
            exports = [item for item in exports if item.job_id == job_id]
        exports.sort(key=lambda item: item.created_at, reverse=True)
        return exports

    def get_export(self, export_id: str) -> ExportRecord | None:
        return self._exports.get(export_id)

    def record_usage_event(self, event: UsageEventRecord) -> UsageEventRecord:
        with self._lock:
            self._event_counter += 1
            event.event_id = self._event_counter
            self._usage_events.append(event)
            return event

    def cleanup_old_data(self, retention_days: int) -> CleanupResult:
        cutoff = now_utc() - timedelta(days=max(0, retention_days))
        result = CleanupResult()

        with self._lock:
            old_job_ids = [job_id for job_id, job in self._jobs.items() if job.created_at < cutoff]
            for job_id in old_job_ids:
                del self._jobs[job_id]
            result.deleted_jobs = len(old_job_ids)
            result.deleted_rows = 0

            old_export_ids = [
                export_id
                for export_id, export in self._exports.items()
                if export.created_at < cutoff
            ]
            for export_id in old_export_ids:
                del self._exports[export_id]
            result.deleted_exports = len(old_export_ids)

            old_events = [item for item in self._usage_events if item.created_at < cutoff]
            self._usage_events = [item for item in self._usage_events if item.created_at >= cutoff]
            result.deleted_usage_events = len(old_events)

            old_invalidated_templates = [
                template_id
                for template_id, template in self._templates.items()
                if template.invalidated and template.updated_at < cutoff
            ]
            for template_id in old_invalidated_templates:
                del self._templates[template_id]
            result.deleted_invalidated_templates = len(old_invalidated_templates)

        return result

    def create_user_account(self, email: str, name: str, password_hash: str) -> UserAccountRecord:
        normalized_email = normalize_email(email)
        safe_name = name.strip() or "New User"
        with self._lock:
            if normalized_email in self._account_user_by_email:
                raise ValueError("email_exists")

            user_id = f"user_{uuid4().hex[:10]}"
            workspace_id = f"ws_{uuid4().hex[:10]}"
            project_id = f"proj_{uuid4().hex[:10]}"
            account = UserAccountRecord(
                user_id=user_id,
                email=normalized_email,
                name=safe_name,
                workspace_id=workspace_id,
                project_id=project_id,
                password_hash=password_hash,
                created_at=now_utc(),
            )
            self._accounts_by_user_id[user_id] = account
            self._account_user_by_email[normalized_email] = user_id
            return account

    def get_user_account_by_email(self, email: str) -> UserAccountRecord | None:
        normalized_email = normalize_email(email)
        user_id = self._account_user_by_email.get(normalized_email)
        if not user_id:
            return None
        return self._accounts_by_user_id.get(user_id)

    def get_user_account_by_user_id(self, user_id: str) -> UserAccountRecord | None:
        return self._accounts_by_user_id.get(user_id)


class Base(DeclarativeBase):
    pass


class UserTable(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class AuthCredentialTable(Base):
    __tablename__ = "auth_credentials"

    user_id: Mapped[str] = mapped_column(String(64), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class WorkspaceTable(Base):
    __tablename__ = "workspaces"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    owner_user_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("users.id"), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class ProjectTable(Base):
    __tablename__ = "projects"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    workspace_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("workspaces.id"), nullable=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_by: Mapped[str | None] = mapped_column(String(64), ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class JobTable(Base):
    __tablename__ = "scrape_jobs"
    __table_args__ = (
        Index("ix_scrape_jobs_project_created", "project_id", "created_at"),
        Index("ix_scrape_jobs_status_created", "status", "created_at"),
    )

    job_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    project_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    input_url: Mapped[str] = mapped_column(Text, nullable=False)
    prompt: Mapped[str | None] = mapped_column(Text, nullable=True)
    max_pages: Mapped[int] = mapped_column(Integer, default=1)
    max_rows: Mapped[int] = mapped_column(Integer, default=20)
    fields_json: Mapped[list[dict[str, object]]] = mapped_column(JSON, default=list)
    page_type: Mapped[str] = mapped_column(String(32), default="listing")
    warnings_json: Mapped[list[str]] = mapped_column(JSON, default=list)
    pages_processed: Mapped[int] = mapped_column(Integer, default=0)
    rows_extracted: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class JobRowTable(Base):
    __tablename__ = "result_rows"
    __table_args__ = (
        Index("ix_result_rows_job_rowindex", "job_id", "row_index"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(64), ForeignKey("scrape_jobs.job_id", ondelete="CASCADE"), index=True)
    row_index: Mapped[int] = mapped_column(Integer, nullable=False)
    row_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)


class TemplateTable(Base):
    __tablename__ = "templates"
    __table_args__ = (
        Index("ix_templates_domain_fingerprint", "domain", "page_fingerprint"),
        Index("ix_templates_workspace_like", "domain", "page_type", "invalidated"),
    )

    template_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    domain: Mapped[str] = mapped_column(String(255), index=True)
    page_type: Mapped[str] = mapped_column(String(64), index=True)
    page_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    template_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1)
    parent_template_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    success_count: Mapped[int] = mapped_column(Integer, default=0)
    failure_count: Mapped[int] = mapped_column(Integer, default=0)
    success_rate: Mapped[float] = mapped_column(default=0.0)
    last_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    invalidated: Mapped[bool] = mapped_column(default=False)
    invalidation_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class ExportTable(Base):
    __tablename__ = "exports"
    __table_args__ = (
        Index("ix_exports_job_created", "job_id", "created_at"),
    )

    export_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(64), ForeignKey("scrape_jobs.job_id", ondelete="CASCADE"), index=True)
    format: Mapped[str] = mapped_column(String(16), nullable=False)
    file_url: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class UsageEventTable(Base):
    __tablename__ = "usage_events"
    __table_args__ = (
        Index("ix_usage_events_workspace_created", "workspace_id", "created_at"),
        Index("ix_usage_events_user_created", "user_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_json: Mapped[dict[str, object]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class PostgresStore:
    backend = "postgres"

    def __init__(self, database_url: str) -> None:
        self._engine = create_engine(database_url, pool_pre_ping=True)
        self._session_factory = sessionmaker(bind=self._engine, autoflush=False, autocommit=False)
        Base.metadata.create_all(self._engine)

    def _session(self) -> Session:
        return self._session_factory()

    @staticmethod
    def _is_email_conflict(exc: IntegrityError) -> bool:
        original = getattr(exc, "orig", None)
        pgcode = getattr(original, "pgcode", "")
        constraint_name = getattr(getattr(original, "diag", None), "constraint_name", "") or ""
        text = f"{original or exc}".lower()

        if pgcode == "23505":
            if "email" in constraint_name.lower():
                return True
            if "users_email_key" in text or "users.email" in text:
                return True

        return "users_email_key" in text and "duplicate key" in text

    def upsert_job(self, job: JobRecord) -> None:
        fields_payload = [field.model_dump() for field in job.fields]
        warnings_payload = list(job.warnings)

        with self._session() as session:
            db_job = session.get(JobTable, job.job_id)
            if db_job is None:
                db_job = JobTable(job_id=job.job_id)
                session.add(db_job)

            db_job.project_id = job.project_id
            db_job.mode = job.mode.value
            db_job.status = job.status.value
            db_job.input_url = job.input_url
            db_job.prompt = job.prompt
            db_job.max_pages = job.max_pages
            db_job.max_rows = job.max_rows
            db_job.fields_json = fields_payload
            db_job.page_type = job.page_type
            db_job.warnings_json = warnings_payload
            db_job.pages_processed = job.progress.pages_processed
            db_job.rows_extracted = job.progress.rows_extracted
            db_job.created_at = job.created_at

            # Ensure parent scrape_jobs row exists before child result_rows writes.
            session.flush()
            session.execute(delete(JobRowTable).where(JobRowTable.job_id == job.job_id))
            for row_index, row in enumerate(job.rows, start=1):
                session.add(JobRowTable(job_id=job.job_id, row_index=row_index, row_json=row))

            session.commit()

    def get_job(self, job_id: str) -> JobRecord | None:
        with self._session() as session:
            db_job = session.get(JobTable, job_id)
            if db_job is None:
                return None

            db_rows = session.scalars(
                select(JobRowTable).where(JobRowTable.job_id == job_id).order_by(JobRowTable.row_index)
            ).all()

            fields = [FieldInfo.model_validate(item) for item in db_job.fields_json]
            rows = [row.row_json for row in db_rows]

            return JobRecord(
                job_id=db_job.job_id,
                project_id=db_job.project_id,
                mode=db_job.mode,
                status=db_job.status,
                input_url=db_job.input_url,
                prompt=db_job.prompt,
                max_pages=db_job.max_pages,
                max_rows=db_job.max_rows,
                rows=rows,
                fields=fields,
                page_type=db_job.page_type,
                warnings=db_job.warnings_json,
                progress=JobProgress(
                    pages_processed=db_job.pages_processed,
                    rows_extracted=db_job.rows_extracted,
                ),
                created_at=db_job.created_at,
            )

    def list_jobs(
        self,
        project_id: str | None = None,
        status: JobStatus | None = None,
        offset: int = 0,
        limit: int = 100,
        project_ids: tuple[str, ...] | None = None,
    ) -> tuple[int, list[JobRecord]]:
        with self._session() as session:
            count_query = select(func.count()).select_from(JobTable)
            list_query = select(JobTable)

            if project_id:
                count_query = count_query.where(JobTable.project_id == project_id)
                list_query = list_query.where(JobTable.project_id == project_id)
            elif project_ids:
                count_query = count_query.where(JobTable.project_id.in_(project_ids))
                list_query = list_query.where(JobTable.project_id.in_(project_ids))
            if status:
                count_query = count_query.where(JobTable.status == status.value)
                list_query = list_query.where(JobTable.status == status.value)

            total = int(session.scalar(count_query) or 0)
            rows = session.scalars(
                list_query.order_by(JobTable.created_at.desc()).offset(max(0, offset)).limit(max(1, limit))
            ).all()

            jobs = [
                JobRecord(
                    job_id=row.job_id,
                    project_id=row.project_id,
                    mode=row.mode,
                    status=row.status,
                    input_url=row.input_url,
                    prompt=row.prompt,
                    max_pages=row.max_pages,
                    max_rows=row.max_rows,
                    rows=[],
                    fields=[FieldInfo.model_validate(item) for item in row.fields_json],
                    page_type=row.page_type,
                    warnings=row.warnings_json,
                    progress=JobProgress(
                        pages_processed=row.pages_processed,
                        rows_extracted=row.rows_extracted,
                    ),
                    created_at=row.created_at,
                )
                for row in rows
            ]
            return total, jobs

    def job_status(self, job_id: str) -> JobStatusResponse | None:
        with self._session() as session:
            db_job = session.get(JobTable, job_id)
            if db_job is None:
                return None

            error = None
            if db_job.status == JobStatus.failed.value:
                error = {
                    "code": "internal_error",
                    "message": "Job failed during processing",
                }

            return JobStatusResponse(
                job_id=db_job.job_id,
                status=db_job.status,
                progress=JobProgress(
                    pages_processed=db_job.pages_processed,
                    rows_extracted=db_job.rows_extracted,
                ),
                error=error,
            )

    def job_rows(self, job_id: str, offset: int, limit: int) -> RowsResponse | None:
        with self._session() as session:
            db_job = session.get(JobTable, job_id)
            if db_job is None:
                return None

            total = session.scalar(select(func.count()).select_from(JobRowTable).where(JobRowTable.job_id == job_id))
            if total is None:
                total = 0

            db_rows = session.scalars(
                select(JobRowTable)
                .where(JobRowTable.job_id == job_id)
                .order_by(JobRowTable.row_index)
                .offset(offset)
                .limit(limit)
            ).all()
            rows = [item.row_json for item in db_rows]
            return RowsResponse(job_id=job_id, total_rows=int(total), rows=rows)

    def save_template(self, template: TemplateRecord) -> None:
        with self._session() as session:
            lineage_query = select(TemplateTable).where(
                TemplateTable.domain == template.domain,
                TemplateTable.page_type == template.page_type,
                TemplateTable.page_fingerprint == template.page_fingerprint,
            )
            lineage = session.scalars(lineage_query).all()
            next_version = max((item.version for item in lineage), default=0) + 1

            db_template = session.get(TemplateTable, template.template_id)
            is_new = db_template is None
            if db_template is None:
                db_template = TemplateTable(template_id=template.template_id)
                session.add(db_template)

            if template.parent_template_id is None and lineage:
                template.parent_template_id = max(lineage, key=lambda item: item.version).template_id

            db_template.version = max(template.version, next_version)
            db_template.parent_template_id = template.parent_template_id
            db_template.domain = template.domain
            db_template.page_type = template.page_type
            db_template.page_fingerprint = template.page_fingerprint
            db_template.template_json = template.template
            db_template.success_count = template.success_count
            db_template.failure_count = template.failure_count
            db_template.success_rate = template.success_rate
            db_template.last_verified_at = template.last_verified_at
            db_template.invalidated = template.invalidated
            db_template.invalidation_reason = template.invalidation_reason
            db_template.created_at = template.created_at
            db_template.updated_at = template.updated_at if is_new else now_utc()
            session.commit()

    def list_templates(self, domain: str | None = None, page_type: str | None = None) -> Iterable[TemplateRecord]:
        with self._session() as session:
            query = select(TemplateTable)
            if domain:
                query = query.where(TemplateTable.domain == domain)
            if page_type:
                query = query.where(TemplateTable.page_type == page_type)

            rows = session.scalars(
                query.order_by(TemplateTable.success_rate.desc(), TemplateTable.version.desc(), TemplateTable.updated_at.desc())
            ).all()
            return [self._template_from_row(item) for item in rows]

    def get_template(self, template_id: str) -> TemplateRecord | None:
        with self._session() as session:
            row = session.get(TemplateTable, template_id)
            if row is None:
                return None
            return self._template_from_row(row)

    def match_template(
        self,
        domain: str,
        page_type: str | None,
        page_fingerprint: str | None,
        template_id: str | None = None,
    ) -> TemplateRecord | None:
        with self._session() as session:
            if template_id:
                row = session.get(TemplateTable, template_id)
                if row and not row.invalidated:
                    return self._template_from_row(row)
                return None

            query = select(TemplateTable).where(TemplateTable.domain == domain, TemplateTable.invalidated.is_(False))
            if page_type:
                query = query.where(TemplateTable.page_type == page_type)

            if page_fingerprint:
                exact_query = query.where(TemplateTable.page_fingerprint == page_fingerprint).order_by(
                    TemplateTable.success_rate.desc(),
                    TemplateTable.version.desc(),
                    TemplateTable.updated_at.desc(),
                )
                exact = session.scalars(exact_query).first()
                if exact:
                    return self._template_from_row(exact)

            fallback = session.scalars(
                query.order_by(TemplateTable.success_rate.desc(), TemplateTable.version.desc(), TemplateTable.updated_at.desc())
            ).first()
            if fallback is None:
                return None
            return self._template_from_row(fallback)

    def update_template_metrics(
        self,
        template_id: str,
        success: bool,
        invalidation_reason: str | None = None,
    ) -> TemplateRecord | None:
        with self._session() as session:
            row = session.get(TemplateTable, template_id)
            if row is None:
                return None

            if success:
                row.success_count += 1
                row.last_verified_at = now_utc()
                row.invalidated = False
                row.invalidation_reason = None
            else:
                row.failure_count += 1
                if invalidation_reason:
                    row.invalidation_reason = invalidation_reason
                if row.failure_count >= 3:
                    row.invalidated = True
                    row.invalidation_reason = row.invalidation_reason or "repeated_template_failures"

            attempts = row.success_count + row.failure_count
            row.success_rate = (row.success_count / attempts) if attempts else 0.0
            row.updated_at = now_utc()
            session.commit()
            session.refresh(row)
            return self._template_from_row(row)

    def save_export(self, export: ExportRecord) -> None:
        with self._session() as session:
            db_export = session.get(ExportTable, export.export_id)
            if db_export is None:
                db_export = ExportTable(export_id=export.export_id)
                session.add(db_export)

            db_export.job_id = export.job_id
            db_export.format = export.format
            db_export.file_url = export.file_url
            db_export.status = export.status
            db_export.created_at = export.created_at
            db_export.completed_at = export.completed_at
            session.commit()

    def list_exports(self, job_id: str | None = None) -> Iterable[ExportRecord]:
        with self._session() as session:
            query = select(ExportTable)
            if job_id:
                query = query.where(ExportTable.job_id == job_id)

            rows = session.scalars(query.order_by(ExportTable.created_at.desc())).all()
            return [
                ExportRecord(
                    export_id=row.export_id,
                    job_id=row.job_id,
                    format=row.format,
                    file_url=row.file_url,
                    status=row.status,
                    created_at=row.created_at,
                    completed_at=row.completed_at,
                )
                for row in rows
            ]

    def get_export(self, export_id: str) -> ExportRecord | None:
        with self._session() as session:
            row = session.get(ExportTable, export_id)
            if row is None:
                return None
            return ExportRecord(
                export_id=row.export_id,
                job_id=row.job_id,
                format=row.format,
                file_url=row.file_url,
                status=row.status,
                created_at=row.created_at,
                completed_at=row.completed_at,
            )

    def record_usage_event(self, event: UsageEventRecord) -> UsageEventRecord:
        with self._session() as session:
            row = UsageEventTable(
                workspace_id=event.workspace_id,
                user_id=event.user_id,
                event_type=event.event_type,
                event_json=event.event_json,
                created_at=event.created_at,
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            return UsageEventRecord(
                event_id=row.id,
                workspace_id=row.workspace_id,
                user_id=row.user_id,
                event_type=row.event_type,
                event_json=row.event_json,
                created_at=row.created_at,
            )

    def cleanup_old_data(self, retention_days: int) -> CleanupResult:
        cutoff = now_utc() - timedelta(days=max(0, retention_days))
        result = CleanupResult()

        with self._session() as session:
            old_job_ids = session.scalars(select(JobTable.job_id).where(JobTable.created_at < cutoff)).all()
            if old_job_ids:
                deleted_rows = session.execute(delete(JobRowTable).where(JobRowTable.job_id.in_(old_job_ids)))
                deleted_exports = session.execute(delete(ExportTable).where(ExportTable.job_id.in_(old_job_ids)))
                deleted_jobs = session.execute(delete(JobTable).where(JobTable.job_id.in_(old_job_ids)))
                result.deleted_rows = int(deleted_rows.rowcount or 0)
                result.deleted_exports = int(deleted_exports.rowcount or 0)
                result.deleted_jobs = int(deleted_jobs.rowcount or 0)

            deleted_usage_events = session.execute(delete(UsageEventTable).where(UsageEventTable.created_at < cutoff))
            result.deleted_usage_events = int(deleted_usage_events.rowcount or 0)

            deleted_templates = session.execute(
                delete(TemplateTable).where(TemplateTable.invalidated.is_(True), TemplateTable.updated_at < cutoff)
            )
            result.deleted_invalidated_templates = int(deleted_templates.rowcount or 0)

            session.commit()

        return result

    def create_user_account(self, email: str, name: str, password_hash: str) -> UserAccountRecord:
        normalized_email = normalize_email(email)
        safe_name = name.strip() or "New User"
        created_at = now_utc()

        with self._session() as session:
            existing = session.scalar(select(UserTable.id).where(func.lower(UserTable.email) == normalized_email))
            if existing is not None:
                raise ValueError("email_exists")

            user_id = f"user_{uuid4().hex[:10]}"
            workspace_id = f"ws_{uuid4().hex[:10]}"
            project_id = f"proj_{uuid4().hex[:10]}"

            try:
                session.add(UserTable(id=user_id, email=normalized_email, name=safe_name, created_at=created_at))
                # Ensure parent user row exists before child FK rows are inserted.
                session.flush()
                session.add(
                    AuthCredentialTable(
                        user_id=user_id,
                        password_hash=password_hash,
                        created_at=created_at,
                        updated_at=created_at,
                    )
                )
                session.add(
                    WorkspaceTable(
                        id=workspace_id,
                        name=f"{safe_name} Workspace",
                        owner_user_id=user_id,
                        created_at=created_at,
                    )
                )
                # Ensure workspace row exists before project FK insert.
                session.flush()
                session.add(
                    ProjectTable(
                        id=project_id,
                        workspace_id=workspace_id,
                        name="Default Project",
                        created_by=user_id,
                        created_at=created_at,
                        updated_at=created_at,
                    )
                )
                session.commit()
            except IntegrityError as exc:
                session.rollback()
                if self._is_email_conflict(exc):
                    raise ValueError("email_exists") from exc
                raise ValueError("signup_failed") from exc

            return UserAccountRecord(
                user_id=user_id,
                email=normalized_email,
                name=safe_name,
                workspace_id=workspace_id,
                project_id=project_id,
                password_hash=password_hash,
                created_at=created_at,
            )

    def get_user_account_by_email(self, email: str) -> UserAccountRecord | None:
        normalized_email = normalize_email(email)
        with self._session() as session:
            user = session.scalars(select(UserTable).where(func.lower(UserTable.email) == normalized_email)).first()
            if user is None:
                return None
            return self._user_account_from_user(session=session, user=user)

    def get_user_account_by_user_id(self, user_id: str) -> UserAccountRecord | None:
        with self._session() as session:
            user = session.get(UserTable, user_id)
            if user is None:
                return None
            return self._user_account_from_user(session=session, user=user)

    def _template_from_row(self, row: TemplateTable) -> TemplateRecord:
        return TemplateRecord(
            template_id=row.template_id,
            domain=row.domain,
            page_type=row.page_type,
            page_fingerprint=row.page_fingerprint,
            template=row.template_json,
            version=row.version,
            parent_template_id=row.parent_template_id,
            success_count=row.success_count,
            failure_count=row.failure_count,
            success_rate=float(row.success_rate),
            last_verified_at=row.last_verified_at,
            invalidated=row.invalidated,
            invalidation_reason=row.invalidation_reason,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )

    def _user_account_from_user(self, session: Session, user: UserTable) -> UserAccountRecord | None:
        credential = session.get(AuthCredentialTable, user.id)
        if credential is None:
            return None

        workspace = session.scalars(
            select(WorkspaceTable).where(WorkspaceTable.owner_user_id == user.id).order_by(WorkspaceTable.created_at.asc())
        ).first()

        project = session.scalars(
            select(ProjectTable).where(ProjectTable.created_by == user.id).order_by(ProjectTable.created_at.asc())
        ).first()

        if project is None and workspace is not None:
            project = session.scalars(
                select(ProjectTable)
                .where(ProjectTable.workspace_id == workspace.id)
                .order_by(ProjectTable.created_at.asc())
            ).first()

        if workspace is None or project is None:
            return None

        return UserAccountRecord(
            user_id=user.id,
            email=user.email,
            name=user.name,
            workspace_id=workspace.id,
            project_id=project.id,
            password_hash=credential.password_hash,
            created_at=user.created_at,
        )


def create_store(store_backend: str, database_url: str) -> Store:
    backend = store_backend.strip().lower()
    if backend == "memory":
        return InMemoryStore()

    if backend == "postgres" and not database_url:
        raise RuntimeError("APP_STORE_BACKEND is set to 'postgres' but DATABASE_URL is empty")

    if database_url:
        try:
            return PostgresStore(database_url)
        except SQLAlchemyError:
            if backend == "postgres":
                raise

    return InMemoryStore()
