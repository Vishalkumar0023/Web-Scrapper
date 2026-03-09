from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

import httpx

from app.models import FieldInfo


ALLOWED_KINDS = {"text", "money", "rating", "url", "number", "date", "email", "phone", "boolean"}
NON_LABEL_KEYS = {
    "job_id",
    "project_id",
    "request_id",
    "status",
    "created_at",
    "updated_at",
    "source_url",
}

DEFAULT_LABELING_PROMPT = (
    "You are a strict product listing field-labeling engine.\n"
    "You only label existing source fields and never change row values.\n"
    "Task: For each source field, return source_field, canonical_name (snake_case), kind, and confidence.\n"
    "Rules:\n"
    "1. Return valid JSON only.\n"
    "2. Do not include explanations.\n"
    "3. Include every source_field exactly once.\n"
    "4. Do not generate new values and do not scrape additional data.\n"
    "5. Prefer canonical ecommerce names when clear: title, price, rating, product_url.\n"
    "6. For rating, prefer decimal product rating semantics (for example: 4.6), not rating count/review count.\n"
    "7. kind must be one of: text|money|rating|url|number|date|email|phone|boolean.\n"
    "8. confidence must be between 0.0 and 1.0.\n"
    "9. If unsure, keep safe canonical_name close to source field in snake_case and kind=text.\n"
    "10. Output schema must be exactly:\n"
    "{\"labels\":[{\"source_field\":\"string\",\"canonical_name\":\"string\",\"kind\":\"text\",\"confidence\":0.0}]}"
)


@dataclass(frozen=True)
class AIPlannerConfig:
    enabled: bool
    provider: str
    api_key: str
    model: str
    timeout_seconds: int
    max_sample_rows: int
    max_chars_per_value: int
    max_input_chars: int
    max_estimated_input_tokens: int
    max_output_tokens: int
    labeling_prompt: str = DEFAULT_LABELING_PROMPT


@dataclass
class AIPlannerResult:
    fields: list[FieldInfo]
    rows: list[dict[str, object]]
    warnings: list[str] = field(default_factory=list)
    used: bool = False


@dataclass
class AIInsightsResult:
    summary: str
    row_classifications: list[dict[str, object]]
    warnings: list[str] = field(default_factory=list)
    used: bool = False


_AI_PLAN_CACHE: dict[str, list[dict[str, object]]] = {}
_AI_INSIGHTS_CACHE: dict[str, dict[str, object]] = {}


def apply_ai_field_labels(
    *,
    config: AIPlannerConfig,
    prompt: str | None,
    page_url: str,
    page_type: str,
    fields: list[FieldInfo],
    rows: list[dict[str, object]],
) -> AIPlannerResult:
    if not rows or not fields:
        return AIPlannerResult(fields=fields, rows=rows, warnings=[])

    if not config.enabled:
        return AIPlannerResult(fields=fields, rows=rows, warnings=[])

    if not config.api_key:
        return AIPlannerResult(fields=fields, rows=rows, warnings=["ai_planner_skipped_no_api_key"])

    if config.provider != "gemini":
        return AIPlannerResult(fields=fields, rows=rows, warnings=["ai_planner_provider_unsupported"])

    source_fields = list(_collect_source_fields(fields=fields, rows=rows))
    if not source_fields:
        return AIPlannerResult(fields=fields, rows=rows, warnings=[])

    sample_rows = _build_sample_rows(
        rows=rows,
        source_fields=source_fields,
        max_rows=config.max_sample_rows,
        max_chars_per_value=config.max_chars_per_value,
    )

    request_payload = {
        "task": "field_labeling_only",
        "instruction": "Label existing fields only. Do not generate, infer, or scrape new values.",
        "page_url": page_url,
        "page_type": page_type,
        "user_prompt": prompt or "",
        "source_fields": source_fields,
        "sample_rows": sample_rows,
    }

    context_text = json.dumps(request_payload, ensure_ascii=True, separators=(",", ":"))
    context_text = _fit_input_budget(
        context_text=context_text,
        sample_rows=sample_rows,
        request_payload=request_payload,
        max_input_chars=config.max_input_chars,
    )

    estimated_tokens = _estimate_tokens(context_text)
    if estimated_tokens > config.max_estimated_input_tokens:
        return AIPlannerResult(
            fields=fields,
            rows=rows,
            warnings=["ai_planner_skipped_input_budget"],
        )

    cache_key = hashlib.sha1(f"{config.model}|{context_text}".encode("utf-8")).hexdigest()
    labels = _AI_PLAN_CACHE.get(cache_key)
    warnings: list[str] = []

    if labels is None:
        labels, call_warnings = _call_gemini_for_labels(
            config=config,
            context_text=context_text,
            source_fields=source_fields,
        )
        warnings.extend(call_warnings)
        if labels:
            _AI_PLAN_CACHE[cache_key] = labels
    else:
        warnings.append("ai_planner_cache_hit")

    if not labels:
        return AIPlannerResult(fields=fields, rows=rows, warnings=warnings)

    mapped_fields, mapped_rows = _apply_labels(
        source_fields=source_fields,
        original_fields=fields,
        rows=rows,
        labels=labels,
    )

    warnings.append("ai_planner_applied")
    return AIPlannerResult(fields=mapped_fields, rows=mapped_rows, warnings=_dedupe_strings(warnings), used=True)


def generate_ai_insights(
    *,
    config: AIPlannerConfig,
    prompt: str | None,
    page_url: str,
    page_type: str,
    rows: list[dict[str, object]],
    max_rows: int = 30,
) -> AIInsightsResult:
    if not rows:
        return AIInsightsResult(
            summary="No rows available for summarization/classification.",
            row_classifications=[],
            warnings=[],
            used=False,
        )

    sampled_rows = rows[: max(1, max_rows)]
    fallback_summary = _heuristic_summary(rows=sampled_rows, page_type=page_type)

    if not config.enabled:
        return AIInsightsResult(summary=fallback_summary, row_classifications=[], warnings=[], used=False)

    if not config.api_key:
        return AIInsightsResult(
            summary=fallback_summary,
            row_classifications=[],
            warnings=["ai_insights_skipped_no_api_key"],
            used=False,
        )

    if config.provider != "gemini":
        return AIInsightsResult(
            summary=fallback_summary,
            row_classifications=[],
            warnings=["ai_insights_provider_unsupported"],
            used=False,
        )

    keys: list[str] = []
    seen_keys: set[str] = set()
    for row in sampled_rows:
        for key in row.keys():
            if key in seen_keys:
                continue
            seen_keys.add(key)
            keys.append(key)

    sample_rows = _build_sample_rows(
        rows=sampled_rows,
        source_fields=keys,
        max_rows=min(max_rows, len(sampled_rows)),
        max_chars_per_value=config.max_chars_per_value,
    )

    request_payload = {
        "task": "summarization_and_classification",
        "instruction": "Summarize and classify extracted rows only. Do not generate new rows.",
        "page_url": page_url,
        "page_type": page_type,
        "user_prompt": prompt or "",
        "source_fields": keys,
        "sample_rows": sample_rows,
    }

    context_text = json.dumps(request_payload, ensure_ascii=True, separators=(",", ":"))
    context_text = _fit_input_budget(
        context_text=context_text,
        sample_rows=sample_rows,
        request_payload=request_payload,
        max_input_chars=config.max_input_chars,
    )
    estimated_tokens = _estimate_tokens(context_text)
    if estimated_tokens > config.max_estimated_input_tokens:
        return AIInsightsResult(
            summary=fallback_summary,
            row_classifications=[],
            warnings=["ai_insights_skipped_input_budget"],
            used=False,
        )

    cache_key = hashlib.sha1(f"insights|{config.model}|{context_text}".encode("utf-8")).hexdigest()
    cached = _AI_INSIGHTS_CACHE.get(cache_key)
    warnings: list[str] = []

    if cached is None:
        parsed_payload, call_warnings = _call_gemini_for_insights(
            config=config,
            context_text=context_text,
            row_count=len(sample_rows),
        )
        warnings.extend(call_warnings)
        if parsed_payload:
            _AI_INSIGHTS_CACHE[cache_key] = parsed_payload
            cached = parsed_payload
    else:
        warnings.append("ai_insights_cache_hit")

    if not cached:
        return AIInsightsResult(
            summary=fallback_summary,
            row_classifications=[],
            warnings=_dedupe_strings(warnings),
            used=False,
        )

    summary = str(cached.get("summary", "")).strip() or fallback_summary
    row_classifications = _validate_row_classifications(
        value=cached.get("row_classifications"),
        row_count=len(sample_rows),
    )
    warnings.append("ai_insights_applied")
    return AIInsightsResult(
        summary=summary,
        row_classifications=row_classifications,
        warnings=_dedupe_strings(warnings),
        used=True,
    )


def _collect_source_fields(fields: list[FieldInfo], rows: list[dict[str, object]]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()

    for item in fields:
        name = item.name.strip()
        if not name or name in NON_LABEL_KEYS:
            continue
        if name in seen:
            continue
        seen.add(name)
        output.append(name)

    if rows:
        for key in rows[0].keys():
            if key in seen or key in NON_LABEL_KEYS:
                continue
            seen.add(key)
            output.append(key)

    return output


def _build_sample_rows(
    rows: list[dict[str, object]],
    source_fields: list[str],
    max_rows: int,
    max_chars_per_value: int,
) -> list[dict[str, str]]:
    sample_rows: list[dict[str, str]] = []

    for row in rows[:max_rows]:
        reduced: dict[str, str] = {}
        for field in source_fields:
            value = row.get(field, "")
            text = str(value)
            text = " ".join(text.split())
            reduced[field] = text[:max_chars_per_value]
        sample_rows.append(reduced)

    return sample_rows


def _fit_input_budget(
    *,
    context_text: str,
    sample_rows: list[dict[str, str]],
    request_payload: dict[str, Any],
    max_input_chars: int,
) -> str:
    if len(context_text) <= max_input_chars:
        return context_text

    mutable_rows = list(sample_rows)
    while mutable_rows and len(context_text) > max_input_chars:
        mutable_rows.pop()
        request_payload["sample_rows"] = mutable_rows
        context_text = json.dumps(request_payload, ensure_ascii=True, separators=(",", ":"))

    if len(context_text) > max_input_chars:
        request_payload["sample_rows"] = []
        context_text = json.dumps(request_payload, ensure_ascii=True, separators=(",", ":"))

    return context_text


def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def _call_gemini_for_labels(
    *,
    config: AIPlannerConfig,
    context_text: str,
    source_fields: list[str],
) -> tuple[list[dict[str, object]] | None, list[str]]:
    instruction = config.labeling_prompt.strip() if config.labeling_prompt.strip() else DEFAULT_LABELING_PROMPT
    prompt = f"{instruction}\nINPUT_JSON:\n{context_text}"

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{config.model}:generateContent?key={config.api_key}"
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0,
            "responseMimeType": "application/json",
            "maxOutputTokens": config.max_output_tokens,
        },
    }

    try:
        response = httpx.post(url, json=body, timeout=config.timeout_seconds)
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None, ["ai_planner_request_failed"]

    response_text = _extract_model_text(payload)
    parsed = _parse_model_json(response_text)
    if not parsed:
        return None, ["ai_planner_invalid_json"]

    labels = _validate_labels(parsed=parsed, source_fields=source_fields)
    if not labels:
        return None, ["ai_planner_invalid_labels"]

    return labels, []


def _call_gemini_for_insights(
    *,
    config: AIPlannerConfig,
    context_text: str,
    row_count: int,
) -> tuple[dict[str, object] | None, list[str]]:
    prompt = (
        "You are a strict data-insights assistant.\n"
        "Tasks:\n"
        "1) Summarize the extracted rows in 1-3 short sentences.\n"
        "2) Classify each row into a concise label.\n"
        "Return JSON only with this exact shape:\n"
        "{\"summary\":\"string\",\"row_classifications\":[{\"row_index\":0,\"label\":\"string\",\"confidence\":0.0}]}\n"
        f"row_index must be between 0 and {max(0, row_count - 1)} and each row index should appear at most once.\n"
        "Do not invent rows.\n"
        "INPUT_JSON:\n"
        f"{context_text}"
    )

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{config.model}:generateContent?key={config.api_key}"
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0,
            "responseMimeType": "application/json",
            "maxOutputTokens": config.max_output_tokens,
        },
    }

    try:
        response = httpx.post(url, json=body, timeout=config.timeout_seconds)
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None, ["ai_insights_request_failed"]

    response_text = _extract_model_text(payload)
    parsed = _parse_model_json(response_text)
    if not parsed:
        return None, ["ai_insights_invalid_json"]
    return parsed, []


def _extract_model_text(payload: dict[str, Any]) -> str:
    candidates = payload.get("candidates", [])
    if not isinstance(candidates, list):
        return ""

    for candidate in candidates:
        content = candidate.get("content", {}) if isinstance(candidate, dict) else {}
        parts = content.get("parts", []) if isinstance(content, dict) else []
        if not isinstance(parts, list):
            continue

        for part in parts:
            if isinstance(part, dict):
                text = part.get("text")
                if isinstance(text, str) and text.strip():
                    return text
    return ""


def _parse_model_json(text: str) -> dict[str, Any] | None:
    if not text:
        return None

    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None

    try:
        obj = json.loads(match.group(0))
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        return None


def _validate_row_classifications(value: object, row_count: int) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []

    output: list[dict[str, object]] = []
    seen: set[int] = set()
    for item in value:
        if not isinstance(item, dict):
            continue

        row_index = item.get("row_index", item.get("rowIndex"))
        label = item.get("label")
        confidence = item.get("confidence", 0.7)

        try:
            row_index_int = int(row_index)
        except Exception:
            continue
        if row_index_int < 0 or row_index_int >= row_count:
            continue
        if row_index_int in seen:
            continue
        if not isinstance(label, str) or not label.strip():
            continue

        try:
            confidence_float = float(confidence)
        except Exception:
            confidence_float = 0.7
        confidence_float = max(0.0, min(1.0, confidence_float))

        output.append(
            {
                "row_index": row_index_int,
                "label": _sanitize_field_name(label),
                "confidence": confidence_float,
            }
        )
        seen.add(row_index_int)
    return output


def _validate_labels(parsed: dict[str, Any], source_fields: list[str]) -> list[dict[str, object]] | None:
    labels = parsed.get("labels")
    if not isinstance(labels, list):
        return None

    source_set = set(source_fields)
    normalized_labels: list[dict[str, object]] = []
    seen_source: set[str] = set()

    for item in labels:
        if not isinstance(item, dict):
            continue

        source = item.get("source_field") or item.get("sourceField")
        canonical = item.get("canonical_name") or item.get("canonicalName")
        kind = item.get("kind")
        confidence = item.get("confidence", 0.75)

        if not isinstance(source, str) or source not in source_set:
            continue
        if source in seen_source:
            continue

        canonical_name = _sanitize_field_name(canonical if isinstance(canonical, str) else source)
        if not canonical_name:
            canonical_name = _sanitize_field_name(source)

        if not isinstance(kind, str):
            kind = _infer_kind_from_name(canonical_name)
        kind = kind.lower().strip()
        if kind not in ALLOWED_KINDS:
            kind = _infer_kind_from_name(canonical_name)

        try:
            confidence_float = float(confidence)
        except Exception:
            confidence_float = 0.75
        confidence_float = max(0.0, min(1.0, confidence_float))

        normalized_labels.append(
            {
                "source_field": source,
                "canonical_name": canonical_name,
                "kind": kind,
                "confidence": confidence_float,
            }
        )
        seen_source.add(source)

    if seen_source != source_set:
        for source in source_fields:
            if source in seen_source:
                continue
            fallback = _sanitize_field_name(source)
            normalized_labels.append(
                {
                    "source_field": source,
                    "canonical_name": fallback,
                    "kind": _infer_kind_from_name(fallback),
                    "confidence": 0.65,
                }
            )

    if not normalized_labels:
        return None

    return normalized_labels


def _sanitize_field_name(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", name.strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        return "field"
    return cleaned[:64]


def _infer_kind_from_name(name: str) -> str:
    normalized = name.lower()
    if any(token in normalized for token in {"url", "link", "href", "website"}):
        return "url"
    if any(token in normalized for token in {"price", "amount", "cost", "mrp"}):
        return "money"
    if any(token in normalized for token in {"rating", "score", "stars", "review"}):
        return "rating"
    if any(token in normalized for token in {"date", "time", "published", "created"}):
        return "date"
    if any(token in normalized for token in {"email", "mail"}):
        return "email"
    if any(token in normalized for token in {"phone", "mobile", "contact"}):
        return "phone"
    return "text"


def _apply_labels(
    *,
    source_fields: list[str],
    original_fields: list[FieldInfo],
    rows: list[dict[str, object]],
    labels: list[dict[str, object]],
) -> tuple[list[FieldInfo], list[dict[str, object]]]:
    by_source = {item["source_field"]: item for item in labels}
    original_confidence = {item.name: item.confidence for item in original_fields}

    used_names: dict[str, int] = {}
    rename_map: dict[str, tuple[str, str, float]] = {}

    for source in source_fields:
        label = by_source.get(source)
        if label is None:
            target_name = _sanitize_field_name(source)
            rename_map[source] = (target_name, _infer_kind_from_name(target_name), 0.65)
            continue

        target_name = str(label["canonical_name"])
        kind = str(label["kind"])
        confidence = float(label["confidence"])

        occurrence = used_names.get(target_name, 0) + 1
        used_names[target_name] = occurrence
        if occurrence > 1:
            target_name = f"{target_name}_{occurrence}"

        combined_confidence = _combine_confidence(original_confidence.get(source, 0.7), confidence)
        rename_map[source] = (target_name, kind, combined_confidence)

    remapped_rows: list[dict[str, object]] = []
    for row in rows:
        new_row: dict[str, object] = {}
        for key, value in row.items():
            if key in rename_map:
                target_name = rename_map[key][0]
            else:
                target_name = _sanitize_field_name(key)
            new_row[target_name] = value
        remapped_rows.append(new_row)

    remapped_fields: list[FieldInfo] = []
    for source in source_fields:
        mapped = rename_map[source]
        remapped_fields.append(FieldInfo(name=mapped[0], kind=mapped[1], confidence=mapped[2]))

    return remapped_fields, remapped_rows


def _combine_confidence(base: float, ai: float) -> float:
    value = (0.45 * base) + (0.55 * ai)
    return round(max(0.0, min(1.0, value)), 2)


def _heuristic_summary(rows: list[dict[str, object]], page_type: str) -> str:
    if not rows:
        return "No rows extracted."

    keys: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            keys.append(key)

    key_text = ", ".join(keys[:8]) if keys else "no fields"
    return f"Extracted {len(rows)} {page_type} rows with fields: {key_text}."


def _dedupe_strings(items: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output
