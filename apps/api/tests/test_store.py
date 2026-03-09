from datetime import timedelta

from app.models import ExportRecord, FieldInfo, JobMode, JobRecord, JobStatus, TemplateRecord, UsageEventRecord, now_utc
from app.store import InMemoryStore, create_store


def test_create_store_memory() -> None:
    store = create_store("memory", "")
    assert isinstance(store, InMemoryStore)


def test_sqlalchemy_store_roundtrip() -> None:
    store = create_store("postgres", "sqlite+pysqlite:///:memory:")

    job = JobRecord(
        job_id="job_1",
        project_id="proj_1",
        mode=JobMode.preview,
        status=JobStatus.success,
        input_url="https://example.com",
        rows=[{"title": "A"}, {"title": "B"}],
        fields=[FieldInfo(name="title", kind="text", confidence=0.9)],
    )
    store.upsert_job(job)

    status = store.job_status("job_1")
    assert status is not None
    assert status.status == JobStatus.success

    rows = store.job_rows("job_1", offset=0, limit=10)
    assert rows is not None
    assert rows.total_rows == 2

    template = TemplateRecord(
        template_id="tpl_1",
        domain="example.com",
        page_type="listing",
        template={"container_selector": ".item-card"},
    )
    store.save_template(template)
    listed = list(store.list_templates(domain="example.com"))
    assert len(listed) == 1

    export = ExportRecord(
        export_id="exp_1",
        job_id="job_1",
        format="csv",
        file_url="https://storage.local/exports/job_1/result.csv",
        status="ready",
        created_at=now_utc(),
    )
    store.save_export(export)
    exports = list(store.list_exports(job_id="job_1"))
    assert len(exports) == 1
    loaded_export = store.get_export("exp_1")
    assert loaded_export is not None
    assert loaded_export.file_url == "https://storage.local/exports/job_1/result.csv"

    event = store.record_usage_event(
        UsageEventRecord(
            workspace_id="ws_1",
            user_id="user_1",
            event_type="preview.completed",
            event_json={"job_id": "job_1"},
        )
    )
    assert event.event_id is not None


def test_template_matching_versioning_and_metrics() -> None:
    store = InMemoryStore()

    template_v1 = TemplateRecord(
        template_id="tpl_a",
        domain="example.com",
        page_type="listing",
        page_fingerprint="abc123",
        template={"container_selector": ".item", "fields": {"title": "h2"}},
    )
    template_v2 = TemplateRecord(
        template_id="tpl_b",
        domain="example.com",
        page_type="listing",
        page_fingerprint="abc123",
        template={"container_selector": ".item", "fields": {"title": "h2", "price": ".price"}},
    )
    store.save_template(template_v1)
    store.save_template(template_v2)

    listed = list(store.list_templates(domain="example.com", page_type="listing"))
    assert len(listed) == 2
    assert listed[0].version >= listed[1].version

    matched = store.match_template(domain="example.com", page_type="listing", page_fingerprint="abc123")
    assert matched is not None
    assert matched.template_id == "tpl_b"

    store.update_template_metrics(template_id="tpl_b", success=True)
    store.update_template_metrics(template_id="tpl_b", success=False, invalidation_reason="selector_not_found")
    updated = store.get_template("tpl_b")
    assert updated is not None
    assert updated.success_count == 1
    assert updated.failure_count == 1
    assert updated.success_rate > 0

    store.update_template_metrics(template_id="tpl_b", success=False, invalidation_reason="selector_not_found")
    store.update_template_metrics(template_id="tpl_b", success=False, invalidation_reason="selector_not_found")
    invalidated = store.get_template("tpl_b")
    assert invalidated is not None
    assert invalidated.invalidated is True


def test_list_jobs_filters_and_paging() -> None:
    store = InMemoryStore()
    first = JobRecord(
        job_id="job_list_1",
        project_id="proj_a",
        mode=JobMode.full,
        status=JobStatus.success,
        input_url="https://example.com/1",
    )
    second = JobRecord(
        job_id="job_list_2",
        project_id="proj_a",
        mode=JobMode.full,
        status=JobStatus.failed,
        input_url="https://example.com/2",
    )
    third = JobRecord(
        job_id="job_list_3",
        project_id="proj_b",
        mode=JobMode.preview,
        status=JobStatus.queued,
        input_url="https://example.com/3",
    )
    store.upsert_job(first)
    store.upsert_job(second)
    store.upsert_job(third)

    total, jobs = store.list_jobs(project_id="proj_a", status=None, offset=0, limit=10)
    assert total == 2
    assert len(jobs) == 2

    total_failed, failed = store.list_jobs(project_id="proj_a", status=JobStatus.failed, offset=0, limit=10)
    assert total_failed == 1
    assert len(failed) == 1
    assert failed[0].job_id == "job_list_2"


def test_inmemory_cleanup_policy() -> None:
    store = InMemoryStore()

    old_job = JobRecord(
        job_id="job_old",
        project_id="proj_1",
        mode=JobMode.preview,
        status=JobStatus.success,
        input_url="https://example.com",
        created_at=now_utc() - timedelta(days=45),
    )
    store.upsert_job(old_job)

    old_export = ExportRecord(
        export_id="exp_old",
        job_id="job_old",
        format="csv",
        file_url="https://storage.local/exports/job_old/result.csv",
        status="ready",
        created_at=now_utc() - timedelta(days=45),
    )
    store.save_export(old_export)

    old_template = TemplateRecord(
        template_id="tpl_old",
        domain="example.com",
        page_type="listing",
        template={"container_selector": ".item"},
        invalidated=True,
        updated_at=now_utc() - timedelta(days=45),
    )
    store.save_template(old_template)

    old_event = UsageEventRecord(
        workspace_id="ws_1",
        user_id="user_1",
        event_type="run.completed",
        event_json={"job_id": "job_old"},
        created_at=now_utc() - timedelta(days=45),
    )
    store.record_usage_event(old_event)

    cleanup = store.cleanup_old_data(retention_days=30)
    assert cleanup.deleted_jobs >= 1
    assert cleanup.deleted_exports >= 1
    assert cleanup.deleted_usage_events >= 1
    assert cleanup.deleted_invalidated_templates >= 1


def test_user_account_roundtrip_memory_and_sqlite() -> None:
    memory = InMemoryStore()
    created = memory.create_user_account(email="a@example.com", name="Alice", password_hash="hash-1")
    loaded_by_email = memory.get_user_account_by_email("a@example.com")
    loaded_by_user_id = memory.get_user_account_by_user_id(created.user_id)
    assert loaded_by_email is not None
    assert loaded_by_user_id is not None
    assert loaded_by_email.user_id == created.user_id
    assert loaded_by_user_id.project_id == created.project_id

    sqlite_store = create_store("postgres", "sqlite+pysqlite:///:memory:")
    sql_created = sqlite_store.create_user_account(email="b@example.com", name="Bob", password_hash="hash-2")
    sql_loaded = sqlite_store.get_user_account_by_email("b@example.com")
    assert sql_loaded is not None
    assert sql_loaded.user_id == sql_created.user_id
    assert sql_loaded.workspace_id.startswith("ws_")
