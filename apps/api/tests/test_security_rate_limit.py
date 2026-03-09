import json
import time
from uuid import uuid4

import app.main as main_module
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.main import app
from app.rate_limiter import InMemoryRateLimiter
from app.security import AuthIdentity, parse_auth_tokens


client = TestClient(app)

SAMPLE_HTML = """
<html>
  <body>
    <div class="item-card"><h2>One</h2><p>$10</p></div>
    <div class="item-card"><h2>Two</h2><p>$20</p></div>
  </body>
</html>
"""


def test_parse_auth_tokens_builds_identity_registry() -> None:
    raw = json.dumps(
        {
            "token_1": {
                "user_id": "user_1",
                "workspace_id": "ws_1",
                "project_ids": ["proj_a", "proj_b"],
                "role": "admin",
            }
        }
    )
    registry = parse_auth_tokens(raw)

    assert "token_1" in registry
    identity = registry["token_1"]
    assert identity.user_id == "user_1"
    assert identity.workspace_id == "ws_1"
    assert identity.project_ids == ("proj_a", "proj_b")
    assert identity.role == "admin"


def test_parse_auth_tokens_invalid_payload_returns_empty_registry() -> None:
    assert parse_auth_tokens("not-json") == {}
    assert parse_auth_tokens("[]") == {}


def test_inmemory_rate_limiter_blocks_after_limit() -> None:
    limiter = InMemoryRateLimiter()
    first = limiter.check(key="ws_1:GET:/api/v1/jobs", limit=2, window_seconds=60)
    second = limiter.check(key="ws_1:GET:/api/v1/jobs", limit=2, window_seconds=60)
    blocked = limiter.check(key="ws_1:GET:/api/v1/jobs", limit=2, window_seconds=60)

    assert first.allowed is True
    assert second.allowed is True
    assert blocked.allowed is False
    assert blocked.remaining == 0
    assert blocked.reset_seconds >= 1


def test_inmemory_rate_limiter_window_resets() -> None:
    limiter = InMemoryRateLimiter()
    blocked = limiter.check(key="ws_2:GET:/api/v1/jobs", limit=1, window_seconds=1)
    blocked = limiter.check(key="ws_2:GET:/api/v1/jobs", limit=1, window_seconds=1)
    assert blocked.allowed is False

    time.sleep(1.1)
    allowed = limiter.check(key="ws_2:GET:/api/v1/jobs", limit=1, window_seconds=1)
    assert allowed.allowed is True


def test_auth_middleware_returns_401_when_identity_resolution_fails(monkeypatch) -> None:
    def _raise_unauthorized(**_kwargs):
        raise HTTPException(
            status_code=401,
            detail={"code": "auth_required", "message": "Bearer token is required"},
        )

    monkeypatch.setattr(main_module, "resolve_identity", _raise_unauthorized)
    response = client.get("/api/v1/jobs?project_id=proj_1")
    assert response.status_code == 401
    assert response.headers.get("X-Request-Id")
    assert response.headers.get("X-Trace-Id")
    assert response.json()["detail"]["code"] == "auth_required"


def test_trace_header_is_returned_and_respects_input_header() -> None:
    response = client.get("/health", headers={"X-Trace-Id": "trace-from-test"})
    assert response.status_code == 200
    assert response.headers.get("X-Trace-Id") == "trace-from-test"
    assert response.headers.get("X-Request-Id")


def test_preview_forbidden_for_non_member_project(monkeypatch) -> None:
    def _restricted_identity(**_kwargs):
        return AuthIdentity(
            user_id="user_member",
            workspace_id="ws_member",
            project_ids=("proj_allowed",),
            role="member",
            token_id="token_member",
        )

    monkeypatch.setattr(main_module, "resolve_identity", _restricted_identity)
    denied = client.post(
        "/api/v1/scrape/preview",
        json={
            "project_id": "proj_denied",
            "url": "https://example.com/catalog",
            "prompt": "Extract title and price",
            "extension_dom_payload": {"html": SAMPLE_HTML},
        },
    )
    assert denied.status_code == 403
    assert denied.json()["detail"]["code"] == "forbidden_project"

    allowed = client.post(
        "/api/v1/scrape/preview",
        json={
            "project_id": "proj_allowed",
            "url": "https://example.com/catalog",
            "prompt": "Extract title and price",
            "extension_dom_payload": {"html": SAMPLE_HTML},
        },
    )
    assert allowed.status_code == 200


def test_list_jobs_auto_scopes_for_restricted_identity(monkeypatch) -> None:
    unique = uuid4().hex[:8]
    project_allowed = f"proj_scope_allowed_{unique}"
    project_blocked = f"proj_scope_blocked_{unique}"

    preview_allowed = client.post(
        "/api/v1/scrape/preview",
        json={
            "project_id": project_allowed,
            "url": "https://example.com/catalog",
            "prompt": "Extract title and price",
            "extension_dom_payload": {"html": SAMPLE_HTML},
        },
    )
    assert preview_allowed.status_code == 200

    preview_blocked = client.post(
        "/api/v1/scrape/preview",
        json={
            "project_id": project_blocked,
            "url": "https://example.com/catalog",
            "prompt": "Extract title and price",
            "extension_dom_payload": {"html": SAMPLE_HTML},
        },
    )
    assert preview_blocked.status_code == 200

    def _restricted_identity(**_kwargs):
        return AuthIdentity(
            user_id="user_member",
            workspace_id="ws_member",
            project_ids=(project_allowed,),
            role="member",
            token_id="token_scope",
        )

    monkeypatch.setattr(main_module, "resolve_identity", _restricted_identity)
    no_scope = client.get("/api/v1/jobs")
    assert no_scope.status_code == 200
    assert all(item["project_id"] == project_allowed for item in no_scope.json()["jobs"])

    scoped = client.get(f"/api/v1/jobs?project_id={project_allowed}")
    assert scoped.status_code == 200


def test_metrics_requires_admin_identity(monkeypatch) -> None:
    def _member_identity(**_kwargs):
        return AuthIdentity(
            user_id="user_member",
            workspace_id="ws_member",
            project_ids=("*",),
            role="member",
            token_id="token_member",
        )

    monkeypatch.setattr(main_module, "resolve_identity", _member_identity)
    response = client.get("/metrics")
    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "forbidden_admin"


def test_cleanup_requires_admin_identity(monkeypatch) -> None:
    def _member_identity(**_kwargs):
        return AuthIdentity(
            user_id="user_member",
            workspace_id="ws_member",
            project_ids=("*",),
            role="member",
            token_id="token_member",
        )

    monkeypatch.setattr(main_module, "resolve_identity", _member_identity)
    response = client.post("/api/v1/maintenance/cleanup?retention_days=30")
    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "forbidden_admin"


def test_list_exports_filters_by_accessible_projects(monkeypatch) -> None:
    unique = uuid4().hex[:8]
    project_allowed = f"proj_allowed_{unique}"
    project_blocked = f"proj_blocked_{unique}"

    preview_allowed = client.post(
        "/api/v1/scrape/preview",
        json={
            "project_id": project_allowed,
            "url": "https://example.com/catalog",
            "prompt": "Extract title and price",
            "extension_dom_payload": {"html": SAMPLE_HTML},
        },
    )
    assert preview_allowed.status_code == 200
    allowed_job_id = preview_allowed.json()["job_id"]

    preview_blocked = client.post(
        "/api/v1/scrape/preview",
        json={
            "project_id": project_blocked,
            "url": "https://example.com/catalog",
            "prompt": "Extract title and price",
            "extension_dom_payload": {"html": SAMPLE_HTML},
        },
    )
    assert preview_blocked.status_code == 200
    blocked_job_id = preview_blocked.json()["job_id"]

    create_allowed_export = client.post(
        f"/api/v1/export/{allowed_job_id}",
        json={"format": "csv", "selected_columns": ["title"]},
    )
    assert create_allowed_export.status_code == 200
    allowed_export_id = create_allowed_export.json()["export_id"]

    create_blocked_export = client.post(
        f"/api/v1/export/{blocked_job_id}",
        json={"format": "csv", "selected_columns": ["title"]},
    )
    assert create_blocked_export.status_code == 200
    blocked_export_id = create_blocked_export.json()["export_id"]

    def _restricted_identity(**_kwargs):
        return AuthIdentity(
            user_id="user_member",
            workspace_id="ws_member",
            project_ids=(project_allowed,),
            role="member",
            token_id="token_member",
        )

    monkeypatch.setattr(main_module, "resolve_identity", _restricted_identity)
    response = client.get("/api/v1/exports")
    assert response.status_code == 200
    payload = response.json()
    export_ids = {item["export_id"] for item in payload["exports"]}
    assert allowed_export_id in export_ids
    assert blocked_export_id not in export_ids
