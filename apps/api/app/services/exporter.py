from __future__ import annotations

import csv
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote

try:
    import boto3
except Exception:  # pragma: no cover - optional dependency fallback
    boto3 = None


SUPPORTED_EXPORT_FORMATS = {"csv", "json"}


@dataclass(frozen=True)
class ExportStorageSettings:
    backend: str
    app_base_url: str
    local_dir: str
    signing_secret: str
    signed_url_ttl_seconds: int
    s3_bucket: str
    s3_region: str
    s3_endpoint_url: str
    s3_access_key_id: str
    s3_secret_access_key: str


@dataclass(frozen=True)
class RenderedExport:
    content: bytes
    content_type: str
    columns: list[str]
    rows_count: int


@dataclass(frozen=True)
class StoredExport:
    storage_ref: str
    signed_url: str
    bytes_written: int


class ExportStorage(Protocol):
    backend: str

    def store(self, export_id: str, job_id: str, export_format: str, content: bytes, content_type: str) -> StoredExport: ...

    def resolve_signed_url(self, export_id: str, storage_ref: str) -> str: ...

    def verify_download_token(self, export_id: str, expires: int, token: str) -> bool: ...

    def local_path_from_ref(self, storage_ref: str) -> str | None: ...


def _normalize_row_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, ensure_ascii=False)


def canonicalize_rows(rows: list[dict[str, Any]], selected_columns: list[str]) -> tuple[list[str], list[dict[str, Any]]]:
    if selected_columns:
        columns = [item for item in selected_columns if item]
    else:
        columns = []
        seen: set[str] = set()
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    columns.append(key)
                    seen.add(key)

    canonical_rows: list[dict[str, Any]] = []
    for row in rows:
        canonical_rows.append({column: _normalize_row_value(row.get(column)) for column in columns})

    return columns, canonical_rows


def render_export(rows: list[dict[str, Any]], export_format: str, selected_columns: list[str]) -> RenderedExport:
    fmt = export_format.strip().lower()
    if fmt not in SUPPORTED_EXPORT_FORMATS:
        raise ValueError(f"Unsupported export format: {export_format}")

    columns, canonical_rows = canonicalize_rows(rows=rows, selected_columns=selected_columns)
    if fmt == "csv":
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in canonical_rows:
            writer.writerow({key: "" if value is None else value for key, value in row.items()})
        payload = output.getvalue().encode("utf-8")
        return RenderedExport(
            content=payload,
            content_type="text/csv; charset=utf-8",
            columns=columns,
            rows_count=len(canonical_rows),
        )

    payload = json.dumps(canonical_rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return RenderedExport(
        content=payload,
        content_type="application/json",
        columns=columns,
        rows_count=len(canonical_rows),
    )


class LocalExportStorage:
    backend = "local"

    def __init__(self, settings: ExportStorageSettings) -> None:
        self._settings = settings
        self._root = Path(settings.local_dir).expanduser().resolve()
        self._root.mkdir(parents=True, exist_ok=True)
        self._secret = settings.signing_secret or secrets.token_hex(32)

    def _sign(self, export_id: str, expires: int) -> str:
        payload = f"{export_id}:{expires}".encode("utf-8")
        digest = hmac.new(self._secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
        return digest

    def _build_signed_url(self, export_id: str, expires: int, token: str) -> str:
        base = self._settings.app_base_url.rstrip("/")
        return f"{base}/api/v1/exports/{quote(export_id)}/download?expires={expires}&token={token}"

    def store(self, export_id: str, job_id: str, export_format: str, content: bytes, content_type: str) -> StoredExport:
        fmt = export_format.strip().lower()
        if fmt not in SUPPORTED_EXPORT_FORMATS:
            raise ValueError(f"Unsupported export format: {fmt}")

        job_dir = self._root / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        path = job_dir / f"{export_id}.{fmt}"
        path.write_bytes(content)

        expires = int(time.time()) + max(60, int(self._settings.signed_url_ttl_seconds))
        token = self._sign(export_id=export_id, expires=expires)
        return StoredExport(
            storage_ref=str(path),
            signed_url=self._build_signed_url(export_id=export_id, expires=expires, token=token),
            bytes_written=len(content),
        )

    def resolve_signed_url(self, export_id: str, storage_ref: str) -> str:
        del storage_ref
        expires = int(time.time()) + max(60, int(self._settings.signed_url_ttl_seconds))
        token = self._sign(export_id=export_id, expires=expires)
        return self._build_signed_url(export_id=export_id, expires=expires, token=token)

    def verify_download_token(self, export_id: str, expires: int, token: str) -> bool:
        if expires < int(time.time()):
            return False
        expected = self._sign(export_id=export_id, expires=expires)
        return hmac.compare_digest(expected, token)

    def local_path_from_ref(self, storage_ref: str) -> str | None:
        if not storage_ref:
            return None
        return storage_ref


class S3ExportStorage:
    backend = "s3"

    def __init__(self, settings: ExportStorageSettings) -> None:
        if boto3 is None:
            raise RuntimeError("boto3 dependency is not installed")
        if not settings.s3_bucket:
            raise RuntimeError("S3 storage backend requires APP_S3_BUCKET")

        self._settings = settings
        session = boto3.session.Session(
            aws_access_key_id=settings.s3_access_key_id or None,
            aws_secret_access_key=settings.s3_secret_access_key or None,
            region_name=settings.s3_region or None,
        )
        self._client = session.client("s3", endpoint_url=settings.s3_endpoint_url or None)

    def _key(self, export_id: str, job_id: str, export_format: str) -> str:
        return f"exports/{job_id}/{export_id}.{export_format}"

    def _presign(self, key: str) -> str:
        expires = max(60, int(self._settings.signed_url_ttl_seconds))
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self._settings.s3_bucket, "Key": key},
            ExpiresIn=expires,
        )

    def store(self, export_id: str, job_id: str, export_format: str, content: bytes, content_type: str) -> StoredExport:
        fmt = export_format.strip().lower()
        if fmt not in SUPPORTED_EXPORT_FORMATS:
            raise ValueError(f"Unsupported export format: {fmt}")

        key = self._key(export_id=export_id, job_id=job_id, export_format=fmt)
        self._client.put_object(
            Bucket=self._settings.s3_bucket,
            Key=key,
            Body=content,
            ContentType=content_type,
        )
        storage_ref = f"s3://{self._settings.s3_bucket}/{key}"
        return StoredExport(
            storage_ref=storage_ref,
            signed_url=self._presign(key),
            bytes_written=len(content),
        )

    def resolve_signed_url(self, export_id: str, storage_ref: str) -> str:
        del export_id
        prefix = f"s3://{self._settings.s3_bucket}/"
        if not storage_ref.startswith(prefix):
            raise RuntimeError("Invalid S3 storage_ref")
        key = storage_ref[len(prefix) :]
        return self._presign(key)

    def verify_download_token(self, export_id: str, expires: int, token: str) -> bool:
        del export_id, expires, token
        return False

    def local_path_from_ref(self, storage_ref: str) -> str | None:
        del storage_ref
        return None


def create_export_storage(settings: ExportStorageSettings) -> ExportStorage:
    backend = settings.backend.strip().lower()
    if backend == "s3":
        return S3ExportStorage(settings=settings)
    if backend == "local":
        return LocalExportStorage(settings=settings)

    if settings.s3_bucket:
        try:
            return S3ExportStorage(settings=settings)
        except Exception:
            pass
    return LocalExportStorage(settings=settings)
