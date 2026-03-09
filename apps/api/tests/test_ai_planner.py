from types import SimpleNamespace

from app.models import FieldInfo
from app.services import ai_planner
from app.services.ai_planner import AIPlannerConfig, apply_ai_field_labels, generate_ai_insights


def _config(**overrides):
    base = {
        "enabled": True,
        "provider": "gemini",
        "api_key": "test-key",
        "model": "gemini-2.0-flash",
        "timeout_seconds": 5,
        "max_sample_rows": 5,
        "max_chars_per_value": 80,
        "max_input_chars": 4000,
        "max_estimated_input_tokens": 1200,
        "max_output_tokens": 200,
    }
    base.update(overrides)
    return AIPlannerConfig(**base)


def test_ai_planner_skips_without_key() -> None:
    result = apply_ai_field_labels(
        config=_config(api_key=""),
        prompt="Extract title and price",
        page_url="https://example.com",
        page_type="listing",
        fields=[FieldInfo(name="title", kind="text", confidence=0.8)],
        rows=[{"title": "A"}],
    )
    assert result.used is False
    assert "ai_planner_skipped_no_api_key" in result.warnings


def test_ai_planner_applies_labels(monkeypatch) -> None:
    def fake_post(url, json, timeout):
        _ = (url, json, timeout)
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"labels":['
                                        '{"source_field":"title","canonical_name":"product_name","kind":"text","confidence":0.92},'
                                        '{"source_field":"price","canonical_name":"price","kind":"money","confidence":0.95}'
                                        "]}"
                                    )
                                }
                            ]
                        }
                    }
                ]
            },
        )

    monkeypatch.setattr(ai_planner.httpx, "post", fake_post)

    fields = [
        FieldInfo(name="title", kind="text", confidence=0.8),
        FieldInfo(name="price", kind="money", confidence=0.75),
    ]
    rows = [{"title": "Wireless Mouse", "price": "$19.99"}]

    result = apply_ai_field_labels(
        config=_config(),
        prompt="Extract title and price",
        page_url="https://example.com",
        page_type="listing",
        fields=fields,
        rows=rows,
    )

    assert result.used is True
    assert any(field.name == "product_name" for field in result.fields)
    assert result.rows[0]["product_name"] == "Wireless Mouse"
    assert "price" in result.rows[0]
    assert "ai_planner_applied" in result.warnings


def test_ai_planner_respects_input_budget() -> None:
    big_rows = [{"title": "x" * 1000, "price": "$10"} for _ in range(6)]
    result = apply_ai_field_labels(
        config=_config(max_input_chars=120, max_estimated_input_tokens=10),
        prompt="Extract fields",
        page_url="https://example.com",
        page_type="listing",
        fields=[
            FieldInfo(name="title", kind="text", confidence=0.8),
            FieldInfo(name="price", kind="money", confidence=0.8),
        ],
        rows=big_rows,
    )

    assert result.used is False
    assert "ai_planner_skipped_input_budget" in result.warnings


def test_ai_insights_summary_and_classification(monkeypatch) -> None:
    def fake_post(url, json, timeout):
        _ = (url, json, timeout)
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"summary":"2 products extracted.","row_classifications":['
                                        '{"row_index":0,"label":"budget","confidence":0.91},'
                                        '{"row_index":1,"label":"premium","confidence":0.88}'
                                        "]}"
                                    )
                                }
                            ]
                        }
                    }
                ]
            },
        )

    monkeypatch.setattr(ai_planner.httpx, "post", fake_post)
    rows = [
        {"title": "A", "price": "₹69,900"},
        {"title": "B", "price": "₹1,99,900"},
    ]
    result = generate_ai_insights(
        config=_config(),
        prompt="Summarize and classify products",
        page_url="https://example.com",
        page_type="listing",
        rows=rows,
        max_rows=10,
    )

    assert result.used is True
    assert result.summary == "2 products extracted."
    assert len(result.row_classifications) == 2
    assert result.row_classifications[0]["label"] == "budget"
    assert "ai_insights_applied" in result.warnings
