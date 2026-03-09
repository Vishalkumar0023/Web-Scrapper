from __future__ import annotations

import hashlib
import re
from collections import Counter
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from app.models import FieldInfo, TemplateRecord


URL_LIKE_FIELDS = {"url", "link", "product_url", "profile_url", "website", "href"}
PRICE_PATTERN = re.compile(r"(?:₹|\$|€|£|USD|EUR|INR|GBP|JPY|AUD|CAD)\s*\d[\d,]*(?:\.\d+)?", re.IGNORECASE)
RATING_PATTERN = re.compile(r"\b(\d(?:[.,]\d)?)\s*(?:/\s*5|out of 5|stars?)\b", re.IGNORECASE)


def normalize_domain(url: str) -> str:
    domain = urlparse(url).netloc.strip().lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def compute_page_fingerprint(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    tag_counts = Counter(node.name for node in soup.find_all(True)[:400])
    class_tokens: list[str] = []
    for node in soup.find_all(True)[:300]:
        classes = node.get("class", [])
        if isinstance(classes, list):
            class_tokens.extend(classes[:2])

    class_counts = Counter(class_tokens)
    anchors = len(soup.find_all("a", href=True))
    headings = len(soup.find_all(["h1", "h2", "h3", "h4"]))
    forms = len(soup.find_all("form"))

    payload = {
        "top_tags": sorted(tag_counts.items(), key=lambda item: (-item[1], item[0]))[:20],
        "top_classes": sorted(class_counts.items(), key=lambda item: (-item[1], item[0]))[:20],
        "anchors": anchors,
        "headings": headings,
        "forms": forms,
    }
    return hashlib.sha1(repr(payload).encode("utf-8")).hexdigest()


def fields_from_template(template: TemplateRecord) -> list[FieldInfo]:
    field_map = template.template.get("fields", {})
    if not isinstance(field_map, dict) or not field_map:
        return []

    fields: list[FieldInfo] = []
    for name in field_map.keys():
        kind = infer_kind_from_field_name(name)
        confidence = 0.95 if kind in {"url", "money", "rating"} else 0.9
        fields.append(FieldInfo(name=name, kind=kind, confidence=confidence))
    return fields


def infer_kind_from_field_name(field_name: str) -> str:
    normalized = field_name.lower()
    if any(token in normalized for token in {"url", "link", "href", "website"}):
        return "url"
    if any(token in normalized for token in {"price", "amount", "cost", "mrp"}):
        return "money"
    if any(token in normalized for token in {"rating", "score", "stars", "review"}):
        return "rating"
    return "text"


def apply_template_extract_rows(
    html: str,
    base_url: str,
    template: TemplateRecord,
    max_rows: int,
) -> tuple[list[dict[str, object]], list[str]]:
    warnings: list[str] = []
    soup = BeautifulSoup(html, "html.parser")

    container_selector = _read_container_selector(template)
    field_map = template.template.get("fields", {})

    if not container_selector or not isinstance(field_map, dict) or not field_map:
        return [], ["template_invalid_structure"]

    try:
        containers = soup.select(container_selector)
    except Exception:
        return [], ["template_selector_invalid"]

    if not containers:
        return [], ["template_container_not_found"]

    rows: list[dict[str, object]] = []
    for container in containers[: max_rows * 2]:
        row: dict[str, object] = {}
        for field_name, selector_expr in field_map.items():
            value = extract_value_with_selector(container=container, selector_expr=str(selector_expr), base_url=base_url)
            if value is None:
                value = ""

            row[field_name] = normalize_field_value(field_name=field_name, value=value)

        if _row_has_substance(row):
            rows.append(row)
        if len(rows) >= max_rows:
            break

    rows = dedupe_rows(rows)
    if not rows:
        warnings.append("template_rows_empty")

    return rows[:max_rows], warnings


def find_next_page_url_with_template(html: str, base_url: str, template: TemplateRecord) -> tuple[str | None, list[str]]:
    pagination = template.template.get("pagination", {})
    if not isinstance(pagination, dict):
        return None, []

    selector = pagination.get("selector")
    pagination_type = str(pagination.get("type", "")).lower()

    if not selector:
        return None, []

    soup = BeautifulSoup(html, "html.parser")
    try:
        node = soup.select_one(str(selector))
    except Exception:
        return None, ["template_pagination_selector_invalid"]

    if node is None:
        return None, ["template_pagination_not_found"]

    href = node.get("href") if isinstance(node, Tag) else None
    if isinstance(href, str) and href.strip():
        warning = []
        if pagination_type:
            warning.append(f"template_pagination_{pagination_type}_used")
        return urljoin(base_url, href.strip()), warning

    data_href = None
    if isinstance(node, Tag):
        for attr in ("data-next-url", "data-url", "data-href"):
            candidate = node.get(attr)
            if isinstance(candidate, str) and candidate.strip():
                data_href = candidate.strip()
                break

    if data_href:
        warning = []
        if pagination_type:
            warning.append(f"template_pagination_{pagination_type}_used")
        return urljoin(base_url, data_href), warning

    return None, ["template_pagination_no_url"]


def extract_value_with_selector(container: Tag, selector_expr: str, base_url: str) -> str | None:
    selector, attr = parse_selector_expression(selector_expr)

    target: Tag | None = None
    if selector:
        try:
            target = container.select_one(selector)
        except Exception:
            return None
    else:
        target = container

    if target is None:
        return None

    if attr:
        raw = target.get(attr)
        if raw is None:
            return None
        value = str(raw).strip()
        if _is_url_field_expression(selector_expr):
            return urljoin(base_url, value)
        return value

    text = target.get_text(" ", strip=True)
    if _is_url_field_expression(selector_expr):
        anchor = target if target.name == "a" else target.find("a", href=True)
        if anchor and anchor.get("href"):
            return urljoin(base_url, str(anchor.get("href")))
    return text


def parse_selector_expression(selector_expr: str) -> tuple[str, str | None]:
    expr = selector_expr.strip()
    if "@" not in expr:
        return expr, None

    selector, attr = expr.rsplit("@", 1)
    return selector.strip(), attr.strip() or None


def normalize_field_value(field_name: str, value: str) -> str:
    normalized_name = field_name.lower()
    cleaned = " ".join(value.split()).strip()

    if any(token in normalized_name for token in URL_LIKE_FIELDS):
        return cleaned

    if any(token in normalized_name for token in {"price", "amount", "cost", "mrp"}):
        match = PRICE_PATTERN.search(cleaned)
        return match.group(0) if match else cleaned

    if any(token in normalized_name for token in {"rating", "score", "stars"}):
        match = RATING_PATTERN.search(cleaned)
        if match:
            return match.group(1).replace(",", ".")

    return cleaned


def dedupe_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    seen: set[tuple[tuple[str, str], ...]] = set()
    out: list[dict[str, object]] = []

    for row in rows:
        signature = tuple(sorted((key, str(value).strip().lower()) for key, value in row.items()))
        if signature in seen:
            continue
        seen.add(signature)
        out.append(row)

    return out


def _is_url_field_expression(selector_expr: str) -> bool:
    return selector_expr.strip().endswith("@href")


def _read_container_selector(template: TemplateRecord) -> str | None:
    container = template.template.get("container_selector")
    if isinstance(container, str) and container.strip():
        return container.strip()

    list_selector = template.template.get("list_selector")
    if isinstance(list_selector, str) and list_selector.strip():
        return list_selector.strip()

    return None


def _row_has_substance(row: dict[str, object]) -> bool:
    for value in row.values():
        if value is None:
            continue
        if str(value).strip():
            return True
    return False
