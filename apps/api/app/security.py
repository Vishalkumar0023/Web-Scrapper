from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass

from fastapi import HTTPException, Request, status


PASSWORD_SCHEME = "pbkdf2_sha256"
PASSWORD_ITERATIONS = 240_000
TOKEN_SCHEME = "wsu1"


@dataclass(frozen=True)
class AuthIdentity:
    user_id: str
    workspace_id: str
    project_ids: tuple[str, ...]
    role: str = "member"
    token_id: str = "anonymous"

    def can_access_project(self, project_id: str) -> bool:
        return self.role == "admin" or "*" in self.project_ids or project_id in self.project_ids


def default_identity(default_user_id: str, default_workspace_id: str) -> AuthIdentity:
    return AuthIdentity(
        user_id=default_user_id or "user_local",
        workspace_id=default_workspace_id or "ws_default",
        project_ids=("*",),
        role="admin",
        token_id="local-bypass",
    )


def parse_auth_tokens(raw_json: str) -> dict[str, AuthIdentity]:
    if not raw_json.strip():
        return {}
    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}

    registry: dict[str, AuthIdentity] = {}
    for token, value in payload.items():
        if not isinstance(token, str) or not token:
            continue
        if not isinstance(value, dict):
            continue

        projects = value.get("project_ids", ["*"])
        if not isinstance(projects, list) or not projects:
            projects = ["*"]
        normalized_projects = tuple(str(item) for item in projects if isinstance(item, str) and item)
        if not normalized_projects:
            normalized_projects = ("*",)

        identity = AuthIdentity(
            user_id=str(value.get("user_id", "user_unknown")),
            workspace_id=str(value.get("workspace_id", "ws_default")),
            project_ids=normalized_projects,
            role=str(value.get("role", "member")),
            token_id=token,
        )
        registry[token] = identity

    return registry


def _b64url_encode(payload: bytes) -> str:
    return base64.urlsafe_b64encode(payload).decode("utf-8").rstrip("=")


def _b64url_decode(payload: str) -> bytes:
    padding = "=" * (-len(payload) % 4)
    return base64.urlsafe_b64decode(f"{payload}{padding}")


def hash_password(password: str) -> str:
    if not password:
        raise ValueError("password_required")
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PASSWORD_ITERATIONS)
    return f"{PASSWORD_SCHEME}${PASSWORD_ITERATIONS}${_b64url_encode(salt)}${_b64url_encode(digest)}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        scheme, iterations_raw, salt_b64, expected_b64 = password_hash.split("$", 3)
        iterations = int(iterations_raw)
        if scheme != PASSWORD_SCHEME or iterations <= 0:
            return False
        salt = _b64url_decode(salt_b64)
        expected = _b64url_decode(expected_b64)
    except Exception:
        return False

    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(actual, expected)


def issue_user_token(identity: AuthIdentity, signing_secret: str, ttl_seconds: int) -> str:
    if not signing_secret:
        raise ValueError("signing_secret_required")

    expires_at = int(time.time()) + max(60, int(ttl_seconds))
    payload = {
        "sub": identity.user_id,
        "ws": identity.workspace_id,
        "projects": list(identity.project_ids) or ["*"],
        "role": identity.role,
        "token_id": identity.token_id,
        "exp": expires_at,
    }
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signature = hmac.new(signing_secret.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    return f"{TOKEN_SCHEME}.{payload_b64}.{_b64url_encode(signature)}"


def parse_signed_identity_token(token: str, signing_secret: str) -> AuthIdentity | None:
    if not signing_secret:
        return None

    parts = token.split(".")
    if len(parts) != 3 or parts[0] != TOKEN_SCHEME:
        return None

    payload_b64 = parts[1]
    signature_b64 = parts[2]

    expected_sig = hmac.new(signing_secret.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    try:
        provided_sig = _b64url_decode(signature_b64)
    except Exception:
        return None

    if not hmac.compare_digest(expected_sig, provided_sig):
        return None

    try:
        payload = json.loads(_b64url_decode(payload_b64))
    except Exception:
        return None

    if not isinstance(payload, dict):
        return None

    exp = payload.get("exp")
    if not isinstance(exp, int) or exp < int(time.time()):
        return None

    projects = payload.get("projects", ["*"])
    if not isinstance(projects, list) or not projects:
        projects = ["*"]
    normalized_projects = tuple(str(item) for item in projects if isinstance(item, str) and item)
    if not normalized_projects:
        normalized_projects = ("*",)

    return AuthIdentity(
        user_id=str(payload.get("sub", "user_unknown")),
        workspace_id=str(payload.get("ws", "ws_default")),
        project_ids=normalized_projects,
        role=str(payload.get("role", "member")),
        token_id=str(payload.get("token_id", "signed-token")),
    )


def resolve_identity(
    request: Request,
    auth_enabled: bool,
    auth_registry: dict[str, AuthIdentity],
    auth_dev_token: str,
    auth_signing_secret: str,
    default_user_id: str,
    default_workspace_id: str,
) -> AuthIdentity:
    if not auth_enabled:
        return default_identity(default_user_id=default_user_id, default_workspace_id=default_workspace_id)

    authorization = request.headers.get("Authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "auth_required", "message": "Bearer token is required"},
        )

    identity = auth_registry.get(token)
    if identity is not None:
        return identity

    if auth_dev_token and token == auth_dev_token:
        return default_identity(default_user_id=default_user_id, default_workspace_id=default_workspace_id)

    signed_identity = parse_signed_identity_token(token=token, signing_secret=auth_signing_secret)
    if signed_identity is not None:
        return signed_identity

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={"code": "auth_invalid_token", "message": "Invalid bearer token"},
    )


def bind_identity(request: Request, identity: AuthIdentity) -> None:
    request.state.identity = identity


def request_identity(request: Request, default_user_id: str, default_workspace_id: str) -> AuthIdentity:
    identity = getattr(request.state, "identity", None)
    if isinstance(identity, AuthIdentity):
        return identity
    return default_identity(default_user_id=default_user_id, default_workspace_id=default_workspace_id)


def ensure_project_access(identity: AuthIdentity, project_id: str) -> None:
    if identity.can_access_project(project_id):
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail={"code": "forbidden_project", "message": f"No access to project '{project_id}'"},
    )


def ensure_admin(identity: AuthIdentity) -> None:
    if identity.role == "admin":
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail={"code": "forbidden_admin", "message": "Admin role is required for this action"},
    )
