import time
from datetime import timedelta
from urllib.parse import urlparse
from types import SimpleNamespace

import app.main as main_module
from fastapi.testclient import TestClient

from app.main import app, export_storage, job_queue, store
from app.models import ExportRecord, JobMode, JobRecord, JobStatus, TemplateRecord, UsageEventRecord, now_utc
from app.services.template_engine import compute_page_fingerprint


client = TestClient(app)


SAMPLE_LISTING_HTML = """
<html>
  <body>
    <div class="results">
      <div class="item-card">
        <a href="/item/1"><h2>Running Shoes Alpha</h2></a>
        <p>Price: $49.99</p>
        <p>4.4 out of 5 stars</p>
      </div>
      <div class="item-card">
        <a href="/item/2"><h2>Running Shoes Beta</h2></a>
        <p>Price: $59.99</p>
        <p>4.1 out of 5 stars</p>
      </div>
      <div class="item-card">
        <a href="/item/3"><h2>Running Shoes Gamma</h2></a>
        <p>Price: $69.99</p>
        <p>4.8 out of 5 stars</p>
      </div>
    </div>
  </body>
</html>
"""


def test_health() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["store"] in {"memory", "postgres"}
    assert payload["queue"] in {"memory", "redis"}


def test_preview_flow_with_extension_dom_payload() -> None:
    payload = {
        "project_id": "proj_1",
        "url": "https://example.com/search?q=shoes",
        "prompt": "Extract title, price, rating, product URL",
        "extension_dom_payload": {
            "html": SAMPLE_LISTING_HTML,
        },
    }
    response = client.post("/api/v1/scrape/preview", json=payload)
    assert response.status_code == 200

    data = response.json()
    assert data["job_id"].startswith("job_prev_")
    assert len(data["rows"]) >= 3
    assert len(data["fields"]) >= 3
    assert data["rows"][0]["title"] == "Running Shoes Alpha"


def test_run_and_get_job_async_queue() -> None:
    run_payload = {
        "project_id": "proj_1",
        "url": "https://example.com/search?q=shoes",
        "prompt": "Extract title and price",
        "max_pages": 2,
        "max_rows": 25,
    }
    run_response = client.post("/api/v1/scrape/run", json=run_payload)
    assert run_response.status_code == 200
    run_data = run_response.json()
    assert run_data["status"] == "queued"
    job_id = run_data["job_id"]

    final_status = None
    for _ in range(60):
        status_response = client.get(f"/api/v1/jobs/{job_id}")
        assert status_response.status_code == 200
        final_status = status_response.json()["status"]
        if final_status in {"success", "partial_success", "failed"}:
            break
        time.sleep(0.05)

    assert final_status in {"success", "partial_success"}

    rows_response = client.get(f"/api/v1/jobs/{job_id}/rows")
    assert rows_response.status_code == 200
    assert rows_response.json()["total_rows"] > 0


def test_list_jobs_and_detail_endpoints() -> None:
    run_payload = {
        "project_id": "proj_list",
        "url": "https://example.com/search?q=shoes",
        "prompt": "Extract title and price",
        "max_pages": 1,
        "max_rows": 10,
    }
    run_response = client.post("/api/v1/scrape/run", json=run_payload)
    assert run_response.status_code == 200
    job_id = run_response.json()["job_id"]

    for _ in range(60):
        status_response = client.get(f"/api/v1/jobs/{job_id}")
        assert status_response.status_code == 200
        if status_response.json()["status"] in {"success", "partial_success", "failed"}:
            break
        time.sleep(0.05)

    jobs_response = client.get("/api/v1/jobs?project_id=proj_list&offset=0&limit=10")
    assert jobs_response.status_code == 200
    jobs_payload = jobs_response.json()
    assert jobs_payload["total_jobs"] >= 1
    assert any(item["job_id"] == job_id for item in jobs_payload["jobs"])

    detail_response = client.get(f"/api/v1/jobs/{job_id}/detail")
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["job_id"] == job_id
    assert "warnings" in detail_payload
    assert "fields" in detail_payload


def test_job_insights_endpoint(monkeypatch) -> None:
    preview_response = client.post(
        "/api/v1/scrape/preview",
        json={
            "project_id": "proj_insights",
            "url": "https://example.com/search?q=shoes",
            "prompt": "Extract title and price",
            "extension_dom_payload": {"html": SAMPLE_LISTING_HTML},
        },
    )
    assert preview_response.status_code == 200
    job_id = preview_response.json()["job_id"]

    def _fake_insights(**_kwargs):
        return SimpleNamespace(
            summary="3 products extracted.",
            row_classifications=[
                {"row_index": 0, "label": "budget", "confidence": 0.82},
                {"row_index": 1, "label": "budget", "confidence": 0.8},
                {"row_index": 2, "label": "premium", "confidence": 0.9},
            ],
            warnings=["ai_insights_applied"],
            used=True,
        )

    monkeypatch.setattr(main_module, "generate_ai_insights", _fake_insights)
    response = client.get(f"/api/v1/jobs/{job_id}/insights?max_rows=20")
    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"] == "3 products extracted."
    assert payload["used_ai"] is True
    assert payload["label_counts"]["budget"] == 2
    assert payload["label_counts"]["premium"] == 1
    assert len(payload["row_classifications"]) == 3


def test_templates_roundtrip() -> None:
    create_response = client.post(
        "/api/v1/templates",
        json={
            "project_id": "proj_1",
            "domain": "example.com",
            "page_type": "listing",
            "template": {
                "container_selector": ".item-card",
                "fields": {
                    "title": "h2",
                },
            },
        },
    )
    assert create_response.status_code == 200

    list_response = client.get("/api/v1/templates?domain=example.com")
    assert list_response.status_code == 200
    payload = list_response.json()
    assert len(payload["templates"]) >= 1


def test_preview_with_explicit_template_id() -> None:
    create_response = client.post(
        "/api/v1/templates",
        json={
            "project_id": "proj_1",
            "domain": "example.com",
            "page_type": "listing",
            "template": {
                "container_selector": ".item-card",
                "fields": {
                    "title": "h2",
                    "price": ".price",
                    "rating": ".rating",
                    "product_url": "a@href",
                },
            },
        },
    )
    assert create_response.status_code == 200
    template_id = create_response.json()["template_id"]

    preview_response = client.post(
        "/api/v1/scrape/preview",
        json={
            "project_id": "proj_1",
            "url": "https://example.com/search?q=shoes",
            "template_id": template_id,
            "extension_dom_payload": {"html": SAMPLE_LISTING_HTML},
        },
    )
    assert preview_response.status_code == 200
    payload = preview_response.json()
    assert payload["rows"][0]["title"] == "Running Shoes Alpha"
    assert any("template_matched" in warning for warning in payload["warnings"])


def test_preview_auto_matches_template_by_fingerprint() -> None:
    fingerprint = compute_page_fingerprint(SAMPLE_LISTING_HTML)
    create_response = client.post(
        "/api/v1/templates",
        json={
            "project_id": "proj_1",
            "domain": "example.com",
            "page_type": "listing",
            "page_fingerprint": fingerprint,
            "template": {
                "container_selector": ".item-card",
                "fields": {
                    "title": "h2",
                    "price": "p",
                    "product_url": "a@href",
                },
            },
        },
    )
    assert create_response.status_code == 200

    preview_response = client.post(
        "/api/v1/scrape/preview",
        json={
            "project_id": "proj_1",
            "url": "https://example.com/search?q=shoes",
            "extension_dom_payload": {"html": SAMPLE_LISTING_HTML},
        },
    )
    assert preview_response.status_code == 200
    payload = preview_response.json()
    assert len(payload["rows"]) >= 1
    assert any("template_matched" in warning for warning in payload["warnings"])


def test_export_persists_record() -> None:
    preview_response = client.post(
        "/api/v1/scrape/preview",
        json={
            "project_id": "proj_1",
            "url": "https://example.com/search?q=shoes",
            "extension_dom_payload": {"html": SAMPLE_LISTING_HTML},
        },
    )
    assert preview_response.status_code == 200
    job_id = preview_response.json()["job_id"]

    export_response = client.post(
        f"/api/v1/export/{job_id}",
        json={"format": "csv", "selected_columns": ["title", "price"]},
    )
    assert export_response.status_code == 200
    export_payload = export_response.json()
    export_id = export_payload["export_id"]
    if export_storage.backend == "local":
        assert "/api/v1/exports/" in export_payload["file_url"]
        assert "token=" in export_payload["file_url"]

    exports = list(store.list_exports(job_id=job_id))
    assert any(item.export_id == export_id for item in exports)


def test_list_exports_endpoint() -> None:
    preview_response = client.post(
        "/api/v1/scrape/preview",
        json={
            "project_id": "proj_1",
            "url": "https://example.com/search?q=shoes",
            "extension_dom_payload": {"html": SAMPLE_LISTING_HTML},
        },
    )
    assert preview_response.status_code == 200
    job_id = preview_response.json()["job_id"]

    first_export = client.post(
        f"/api/v1/export/{job_id}",
        json={"format": "csv", "selected_columns": ["title", "price"]},
    )
    assert first_export.status_code == 200
    first_export_id = first_export.json()["export_id"]

    second_export = client.post(
        f"/api/v1/export/{job_id}",
        json={"format": "json", "selected_columns": ["title"]},
    )
    assert second_export.status_code == 200
    second_export_id = second_export.json()["export_id"]

    list_response = client.get(f"/api/v1/exports?job_id={job_id}")
    assert list_response.status_code == 200
    payload = list_response.json()
    assert payload["total_exports"] >= 2
    export_ids = {item["export_id"] for item in payload["exports"]}
    assert first_export_id in export_ids
    assert second_export_id in export_ids
    if export_storage.backend == "local":
        assert all("/api/v1/exports/" in item["file_url"] for item in payload["exports"])

    paged_response = client.get(f"/api/v1/exports?job_id={job_id}&offset=0&limit=1")
    assert paged_response.status_code == 200
    assert len(paged_response.json()["exports"]) == 1


def test_download_export_with_signed_url() -> None:
    if export_storage.backend != "local":
        return

    preview_response = client.post(
        "/api/v1/scrape/preview",
        json={
            "project_id": "proj_1",
            "url": "https://example.com/search?q=shoes",
            "extension_dom_payload": {"html": SAMPLE_LISTING_HTML},
        },
    )
    assert preview_response.status_code == 200
    job_id = preview_response.json()["job_id"]

    export_response = client.post(
        f"/api/v1/export/{job_id}",
        json={"format": "csv", "selected_columns": ["title", "price"]},
    )
    assert export_response.status_code == 200
    file_url = export_response.json()["file_url"]
    parsed = urlparse(file_url)
    signed_path = f"{parsed.path}?{parsed.query}"

    download_response = client.get(signed_path)
    assert download_response.status_code == 200
    body = download_response.content.decode("utf-8")
    assert "title,price" in body


def test_export_rejects_non_terminal_job() -> None:
    job_id = f"job_export_pending_{time.time_ns()}"
    store.upsert_job(
        JobRecord(
            job_id=job_id,
            project_id="proj_export_guard",
            mode=JobMode.full,
            status=JobStatus.queued,
            input_url="https://example.com/search",
        )
    )

    response = client.post(
        f"/api/v1/export/{job_id}",
        json={"format": "json", "selected_columns": ["title"]},
    )
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "job_not_exportable"


def test_cleanup_endpoint() -> None:
    suffix = str(time.time_ns())
    old_job_id = f"job_cleanup_api_old_{suffix}"
    old_export_id = f"exp_cleanup_api_old_{suffix}"
    old_template_id = f"tpl_cleanup_api_old_{suffix}"
    old_job = JobRecord(
        job_id=old_job_id,
        project_id="proj_cleanup",
        mode=JobMode.preview,
        status=JobStatus.success,
        input_url="https://example.com",
        created_at=now_utc() - timedelta(days=45),
    )
    store.upsert_job(old_job)

    store.save_export(
        ExportRecord(
            export_id=old_export_id,
            job_id=old_job_id,
            format="csv",
            file_url=f"https://storage.local/exports/{old_job_id}/result.csv",
            status="ready",
            created_at=now_utc() - timedelta(days=45),
        )
    )
    store.save_template(
        TemplateRecord(
            template_id=old_template_id,
            domain="example.com",
            page_type="listing",
            template={"container_selector": ".item-card"},
            invalidated=True,
            updated_at=now_utc() - timedelta(days=45),
        )
    )
    store.record_usage_event(
        UsageEventRecord(
            workspace_id="ws_cleanup",
            user_id="user_cleanup",
            event_type="cleanup.seed",
            event_json={"seed": True},
            created_at=now_utc() - timedelta(days=45),
        )
    )

    cleanup_response = client.post("/api/v1/maintenance/cleanup?retention_days=30")
    assert cleanup_response.status_code == 200
    payload = cleanup_response.json()

    assert payload["retention_days"] == 30
    assert payload["result"]["deleted_jobs"] >= 1
    assert payload["result"]["deleted_exports"] >= 1
    assert payload["result"]["deleted_usage_events"] >= 1
    assert payload["result"]["deleted_invalidated_templates"] >= 1


def test_cancel_endpoint() -> None:
    job_id = f"job_cancel_api_{time.time_ns()}"
    store.upsert_job(
        JobRecord(
            job_id=job_id,
            project_id="proj_cancel",
            mode=JobMode.full,
            status=JobStatus.queued,
            input_url="https://example.com/search",
        )
    )

    cancel_response = client.post(f"/api/v1/jobs/{job_id}/cancel")
    assert cancel_response.status_code == 200
    payload = cancel_response.json()
    assert payload["job_id"] == job_id
    assert payload["status"] == JobStatus.cancelled.value

    status = store.job_status(job_id)
    assert status is not None
    assert status.status == JobStatus.cancelled


def test_retry_endpoint_enqueues_force_message(monkeypatch) -> None:
    captured_messages = []
    monkeypatch.setattr("app.main.ensure_worker_started", lambda: None)
    monkeypatch.setattr(job_queue, "enqueue", lambda message: captured_messages.append(message))

    job_id = f"job_retry_api_{time.time_ns()}"
    store.upsert_job(
        JobRecord(
            job_id=job_id,
            project_id="proj_retry",
            mode=JobMode.full,
            status=JobStatus.failed,
            input_url="https://example.com/search",
            prompt="Extract title",
            max_pages=2,
            max_rows=25,
        )
    )

    retry_response = client.post(f"/api/v1/jobs/{job_id}/retry")
    assert retry_response.status_code == 200
    payload = retry_response.json()
    assert payload["job_id"] == job_id
    assert payload["status"] == JobStatus.queued.value

    assert len(captured_messages) == 1
    message = captured_messages[0]
    assert message.job_id == job_id
    assert message.force is True
    assert message.attempt == 0
