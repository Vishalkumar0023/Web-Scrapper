from dataclasses import replace
from uuid import uuid4

import app.main as main_module
from fastapi.testclient import TestClient

from app.main import app
from app.security import hash_password, parse_signed_identity_token, verify_password


client = TestClient(app)


def test_password_hash_roundtrip() -> None:
    password_hash = hash_password("very-strong-password")
    assert verify_password("very-strong-password", password_hash) is True
    assert verify_password("wrong-password", password_hash) is False


def test_auth_signup_and_login_and_me(monkeypatch) -> None:
    monkeypatch.setattr(
        main_module,
        "settings",
        replace(
            main_module.settings,
            auth_enabled=True,
            auth_signing_secret="test-auth-secret",
            auth_token_ttl_seconds=3600,
            auth_dev_token="",
        ),
    )

    email = f"user_{uuid4().hex[:8]}@example.com"
    signup = client.post(
        "/api/v1/auth/signup",
        json={
            "email": email,
            "name": "Test User",
            "password": "password123",
        },
    )
    assert signup.status_code == 200
    signup_payload = signup.json()
    assert signup_payload["token_type"] == "bearer"
    assert signup_payload["user"]["email"] == email
    assert signup_payload["user"]["default_project_id"].startswith("proj_")
    assert len(signup_payload["user"]["project_ids"]) == 1

    token = signup_payload["access_token"]
    token_identity = parse_signed_identity_token(token=token, signing_secret="test-auth-secret")
    assert token_identity is not None
    assert token_identity.user_id == signup_payload["user"]["user_id"]

    me = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me.status_code == 200
    me_payload = me.json()
    assert me_payload["user_id"] == signup_payload["user"]["user_id"]
    assert me_payload["workspace_id"] == signup_payload["user"]["workspace_id"]
    assert me_payload["default_project_id"] == signup_payload["user"]["default_project_id"]

    login = client.post(
        "/api/v1/auth/login",
        json={
            "email": email,
            "password": "password123",
        },
    )
    assert login.status_code == 200
    assert login.json()["user"]["user_id"] == signup_payload["user"]["user_id"]


def test_auth_signup_duplicate_email_rejected() -> None:
    email = f"dupe_{uuid4().hex[:8]}@example.com"
    first = client.post(
        "/api/v1/auth/signup",
        json={
            "email": email,
            "name": "User One",
            "password": "password123",
        },
    )
    assert first.status_code == 200

    second = client.post(
        "/api/v1/auth/signup",
        json={
            "email": email,
            "name": "User Two",
            "password": "password123",
        },
    )
    assert second.status_code == 409
    assert second.json()["detail"]["code"] == "auth_email_exists"


def test_auth_login_invalid_credentials() -> None:
    email = f"login_{uuid4().hex[:8]}@example.com"
    signup = client.post(
        "/api/v1/auth/signup",
        json={
            "email": email,
            "name": "Login User",
            "password": "password123",
        },
    )
    assert signup.status_code == 200

    invalid = client.post(
        "/api/v1/auth/login",
        json={
            "email": email,
            "password": "invalid-password",
        },
    )
    assert invalid.status_code == 401
    assert invalid.json()["detail"]["code"] == "auth_invalid_credentials"
