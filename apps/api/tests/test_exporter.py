from __future__ import annotations

import time
from urllib.parse import parse_qs, urlparse

from app.services.exporter import (
    ExportStorageSettings,
    LocalExportStorage,
    canonicalize_rows,
    render_export,
)


def test_canonicalize_rows_with_selected_columns() -> None:
    columns, rows = canonicalize_rows(
        rows=[
            {"title": "A", "price": 10, "rating": 4.4},
            {"title": "B", "price": 12},
        ],
        selected_columns=["title", "price"],
    )
    assert columns == ["title", "price"]
    assert rows == [{"title": "A", "price": 10}, {"title": "B", "price": 12}]


def test_render_export_csv_and_json() -> None:
    sample_rows = [
        {"title": "A", "price": 10, "meta": {"currency": "USD"}},
        {"title": "B", "price": 12, "meta": {"currency": "USD"}},
    ]

    csv_rendered = render_export(rows=sample_rows, export_format="csv", selected_columns=["title", "price"])
    assert csv_rendered.content_type.startswith("text/csv")
    csv_text = csv_rendered.content.decode("utf-8")
    assert "title,price" in csv_text
    assert "A,10" in csv_text
    assert csv_rendered.rows_count == 2

    json_rendered = render_export(rows=sample_rows, export_format="json", selected_columns=["title", "meta"])
    assert json_rendered.content_type == "application/json"
    json_text = json_rendered.content.decode("utf-8")
    assert '"title":"A"' in json_text
    assert '"meta":"{\\"currency\\": \\"USD\\"}"' in json_text
    assert json_rendered.rows_count == 2


def test_local_storage_sign_and_verify(tmp_path) -> None:
    storage = LocalExportStorage(
        settings=ExportStorageSettings(
            backend="local",
            app_base_url="http://localhost:8000",
            local_dir=str(tmp_path),
            signing_secret="unit-test-secret",
            signed_url_ttl_seconds=300,
            s3_bucket="",
            s3_region="",
            s3_endpoint_url="",
            s3_access_key_id="",
            s3_secret_access_key="",
        )
    )

    stored = storage.store(
        export_id="exp_1",
        job_id="job_1",
        export_format="csv",
        content=b"title\nA\n",
        content_type="text/csv",
    )
    assert stored.bytes_written > 0

    parsed = urlparse(stored.signed_url)
    query = parse_qs(parsed.query)
    expires = int(query["expires"][0])
    token = query["token"][0]
    assert storage.verify_download_token(export_id="exp_1", expires=expires, token=token) is True
    assert storage.verify_download_token(export_id="exp_1", expires=int(time.time()) - 1, token=token) is False
