from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import re
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from app.models import FieldInfo
from app.services.fetcher import PageFetchError, fetch_html_playwright, fetch_page_html
from app.services.scoring import field_score


DEFAULT_FIELDS = [
    ("title", "text"),
    ("price", "money"),
    ("rating", "rating"),
    ("product_url", "url"),
]
STRUCTURED_PRODUCT_FIELDS = [
    ("brand", "text"),
    ("category", "text"),
    ("product_family", "text"),
    ("model", "text"),
    ("sku", "text"),
    ("sku_confidence", "number"),
    ("ram", "text"),
    ("storage", "text"),
    ("processor", "text"),
    ("display", "text"),
    ("os_family", "text"),
    ("os_version", "text"),
    ("os", "text"),
    ("is_canonical_name", "boolean"),
    ("name_source", "text"),
    ("price_inr", "number"),
    ("rating", "rating"),
    ("review_count", "number"),
    ("review_scope", "text"),
    ("review_count_timestamp", "text"),
    ("availability", "text"),
    ("product_url", "url"),
]

PRICE_PATTERN = re.compile(r"(?:₹|\$|€|£)\s*\d[\d,]*(?:\.\d+)?")
RATING_PATTERN = re.compile(r"(\d(?:\.\d)?)\s*(?:/\s*5|out of 5|stars?)", re.IGNORECASE)
RATING_BEFORE_COUNT_PATTERN = re.compile(r"\b(\d(?:\.\d)?)\b(?=\s*[\d,]+\s+Ratings?\b)", re.IGNORECASE)
# Rating value should precede the "Ratings" token, not a raw review count.
RATING_BEFORE_WORD_PATTERN = re.compile(r"\b(\d(?:\.\d)?)\b(?=\s*Ratings?\b)", re.IGNORECASE)
RATING_BEFORE_REVIEW_COUNT_PATTERN = re.compile(
    r"\b(\d(?:\.\d)?)\b(?=\s*[\d,]+\s*Reviews?\b)",
    re.IGNORECASE,
)
NEXT_TEXT_PATTERN = re.compile(r"^(next|next page|>|>>|›|→)$", re.IGNORECASE)
PRICE_WITH_CODE_PATTERN = re.compile(
    r"(?:USD|EUR|INR|GBP|JPY|AUD|CAD)\s*\d[\d,]*(?:\.\d+)?",
    re.IGNORECASE,
)
RATING_ALT_PATTERN = re.compile(r"\b(\d(?:[.,]\d)?)\s*/\s*5\b", re.IGNORECASE)
PAGINATION_NEXT_HINT_PATTERN = re.compile(r"(next|older|forward|more results)", re.IGNORECASE)
PAGINATION_LOAD_MORE_HINT_PATTERN = re.compile(r"(load more|show more|view more|more results)", re.IGNORECASE)
PAGINATION_CONTAINER_HINT_PATTERN = re.compile(r"(pagination|pager|paginator|load-more)", re.IGNORECASE)
PRICE_CLASS_HINT_PATTERN = re.compile(r"(price|amount|cost|mrp|sale)", re.IGNORECASE)
RATING_CLASS_HINT_PATTERN = re.compile(r"(rating|stars?|score|review)", re.IGNORECASE)
AVAILABILITY_CLASS_HINT_PATTERN = re.compile(r"(availability|stock|status)", re.IGNORECASE)
AVAILABILITY_PATTERN = re.compile(
    r"\b(currently unavailable|coming soon|out of stock|in stock|available)\b",
    re.IGNORECASE,
)
DISPLAY_INCH_PATTERN = re.compile(r"\b(\d{1,2}(?:\.\d+)?)\s*(?:inch|inches|in|\"|'')\b", re.IGNORECASE)
DISPLAY_CM_PATTERN = re.compile(r"\b(\d{1,2}(?:\.\d+)?)\s*cm\b", re.IGNORECASE)
PROCESSOR_A_SERIES_PATTERN = re.compile(r"\bA(\d{1,2})(\s*Pro)?\b", re.IGNORECASE)
PROCESSOR_APPLE_SILICON_PATTERN = re.compile(r"\bM([1-5])(\s*(?:Pro|Max|Ultra))?\b", re.IGNORECASE)
PROCESSOR_INTEL_CORE_ULTRA_PATTERN = re.compile(
    r"\b(?:intel\s+)?core\s+ultra\s+[3579]\s+\d{3,4}[a-z]?\b",
    re.IGNORECASE,
)
PROCESSOR_INTEL_CORE_SERIES_PATTERN = re.compile(
    r"\b(?:intel\s+)?core\s+[3579]\s+series\s+\d+\s+\d{3,4}[a-z]?\b",
    re.IGNORECASE,
)
PROCESSOR_INTEL_PATTERN = re.compile(r"\b(?:Intel\s+)?Core\s+i([3579])\b", re.IGNORECASE)
PROCESSOR_RYZEN_PATTERN = re.compile(r"\b(?:AMD\s+)?Ryzen\s+(\d{3,5})\b", re.IGNORECASE)
REVIEW_COUNT_PATTERN = re.compile(r"\b(\d[\d,]*)\s*reviews?\b", re.IGNORECASE)
SKU_PATTERN = re.compile(r"\b([A-Z]{2,}[A-Z0-9]{2,}(?:[/-][A-Z0-9]{1,})+)\b")
SKU_NORMALIZED_PATTERN = re.compile(r"^[A-Z0-9]+(?:-[A-Z0-9]+)+$")
PROCESSOR_TRAIL_PATTERN = re.compile(
    r"\b(?:Apple\s+M[1-5](?:\s*(?:Pro|Max|Ultra))?|Intel\s+Core\s+[^,()/]+|AMD\s+Ryzen\s+\d+[A-Za-z0-9-]*)\b",
    re.IGNORECASE,
)
WINDOWS_OS_PATTERN = re.compile(r"\bwindows\s*(11|10)(?:\s*(home|pro|s))?\b", re.IGNORECASE)
MACOS_OS_PATTERN = re.compile(
    r"\bmac\s*os(?:\s*(sequoia|sonoma|ventura|monterey|big\s*sur))?\b|\bmacos(?:\s*(sequoia|sonoma|ventura|monterey|big\s*sur))?\b",
    re.IGNORECASE,
)
TITLE_BLACKLIST_PATTERN = re.compile(
    r"^(home|about|contact|privacy|terms|login|sign in|signup|register|menu|categories?)$",
    re.IGNORECASE,
)
TITLE_PREFIX_CLEANUPS = (
    "add to compare",
    "currently unavailable",
    "coming soon",
    "pre order",
    "trending",
)
TITLE_TRAILING_METADATA_PATTERN = re.compile(
    r"\b(?:ratings?|reviews?|gb\s*rom|display|camera|processor|battery|ram|rom)\b.*$",
    re.IGNORECASE,
)
TITLE_SIZE_SPEC_PATTERN = re.compile(r"\b\d{1,2}(?:\.\d{1,2})?\s*cm\b.*$", re.IGNORECASE)
ITEM_CONTAINER_TAGS = ("article", "li", "div", "tr", "section")
COLLECTION_PARENT_TAGS = ("main", "section", "div", "ul", "ol", "tbody")
MIN_ITEM_TEXT_LENGTH = 20
DEFAULT_DUPLICATE_ROW_CUTOFF_RATIO = 0.8
DEFAULT_MAX_CONSECUTIVE_LOW_YIELD_PAGES = 1


@dataclass
class FullScrapeResult:
    fields: list[FieldInfo]
    page_type: str
    rows: list[dict[str, object]]
    warnings: list[str]
    pages_processed: int
    partial: bool


@dataclass
class PreviewScrapeResult:
    fields: list[FieldInfo]
    page_type: str
    rows: list[dict[str, object]]
    warnings: list[str]


def infer_fields(prompt: str | None) -> list[FieldInfo]:
    if not prompt:
        return _with_scores(DEFAULT_FIELDS)

    if _prompt_requests_structured_product_schema(prompt):
        # Derive structured schema from canonical extraction fields while preserving
        # review/availability signals for downstream enrichment.
        return _with_scores(DEFAULT_FIELDS + [("review_count", "number"), ("availability", "text"), ("raw_text", "text")])

    prompt_l = prompt.lower()
    chosen: list[tuple[str, str]] = []

    mapping = {
        "title": ("title", "text"),
        "name": ("title", "text"),
        "price": ("price", "money"),
        "rating": ("rating", "rating"),
        "review_count": ("review_count", "number"),
        "review count": ("review_count", "number"),
        "reviews": ("review_count", "number"),
        "url": ("product_url", "url"),
        "link": ("product_url", "url"),
        "availability": ("availability", "text"),
        "unavailable": ("availability", "text"),
        "stock": ("availability", "text"),
        "seller": ("seller", "text"),
        "company": ("company", "text"),
    }

    for key, value in mapping.items():
        if key in prompt_l and value not in chosen:
            chosen.append(value)

    if not chosen:
        chosen = DEFAULT_FIELDS

    return _with_scores(chosen)


def scrape_preview(
    url: str,
    prompt: str | None,
    max_rows: int,
    timeout_seconds: int,
    playwright_fallback_enabled: bool = True,
    playwright_timeout_seconds: int | None = None,
    extension_dom_payload: dict[str, object] | None = None,
) -> PreviewScrapeResult:
    fields = infer_fields(prompt)
    warnings: list[str] = []

    html = _extract_extension_html(extension_dom_payload)
    fetch_source = "extension"
    if html is None:
        try:
            fetch_result = fetch_page_html(
                url=url,
                timeout_seconds=timeout_seconds,
                allow_playwright_fallback=playwright_fallback_enabled,
                playwright_timeout_seconds=playwright_timeout_seconds,
            )
            html = fetch_result.html
            fetch_source = fetch_result.source
            warnings.extend(fetch_result.warnings)
        except PageFetchError:
            warnings.append("page_load_failed")
            rows = generate_rows(url, fields, count=min(max_rows, 10))
            warnings.append("fallback_generated_rows")
            return PreviewScrapeResult(fields=fields, page_type="listing", rows=rows, warnings=warnings)
    else:
        warnings.append("source_extension_dom")

    rows, parse_warnings = parse_rows_from_html(html=html, base_url=url, fields=fields, max_rows=max_rows)
    warnings.extend(parse_warnings)

    if not rows and html is not None and fetch_source == "http" and playwright_fallback_enabled:
        playwright_html = fetch_html_playwright(
            url=url,
            timeout_seconds=playwright_timeout_seconds or max(timeout_seconds * 2, timeout_seconds + 4),
        )
        if playwright_html:
            warnings.append("playwright_retry_after_extraction_empty")
            rows, parse_warnings = parse_rows_from_html(
                html=playwright_html,
                base_url=url,
                fields=fields,
                max_rows=max_rows,
            )
            warnings.extend(parse_warnings)

    if not rows:
        warnings.append("extraction_empty")
        rows = generate_rows(url, fields, count=min(max_rows, 10))
        warnings.append("fallback_generated_rows")

    page_type = "listing" if len(rows) > 1 else "detail"
    return PreviewScrapeResult(fields=fields, page_type=page_type, rows=rows, warnings=_dedupe_strings(warnings))


def scrape_full(
    url: str,
    prompt: str | None,
    max_pages: int,
    max_rows: int,
    timeout_seconds: int,
    playwright_fallback_enabled: bool = True,
    playwright_timeout_seconds: int | None = None,
    duplicate_row_cutoff_ratio: float = DEFAULT_DUPLICATE_ROW_CUTOFF_RATIO,
    max_consecutive_low_yield_pages: int = DEFAULT_MAX_CONSECUTIVE_LOW_YIELD_PAGES,
) -> FullScrapeResult:
    fields = infer_fields(prompt)
    rows: list[dict[str, object]] = []
    warnings: list[str] = []
    pages_processed = 0
    visited: set[str] = set()
    seen_page_signatures: set[str] = set()
    seen_row_signatures: set[tuple[tuple[str, str], ...]] = set()
    low_yield_streak = 0
    current_url = url
    partial = False

    while current_url and pages_processed < max_pages and len(rows) < max_rows:
        if current_url in visited:
            warnings.append("repeated_page_detected")
            break
        visited.add(current_url)

        try:
            fetch_result = fetch_page_html(
                url=current_url,
                timeout_seconds=timeout_seconds,
                allow_playwright_fallback=playwright_fallback_enabled,
                playwright_timeout_seconds=playwright_timeout_seconds,
            )
            html = fetch_result.html
            warnings.extend(fetch_result.warnings)
        except PageFetchError:
            warnings.append("page_load_failed")
            partial = pages_processed > 0
            break

        remaining = max_rows - len(rows)
        page_rows, parse_warnings = parse_rows_from_html(
            html=html,
            base_url=current_url,
            fields=fields,
            max_rows=remaining,
        )
        warnings.extend(parse_warnings)
        page_signature = compute_page_signature(page_rows=page_rows, html=html)
        if page_signature in seen_page_signatures:
            warnings.append("repeated_page_signature_detected")
            partial = pages_processed > 0
            break
        seen_page_signatures.add(page_signature)

        unique_page_rows, duplicate_ratio = filter_duplicate_rows(
            page_rows=page_rows,
            seen_row_signatures=seen_row_signatures,
        )
        if duplicate_ratio >= duplicate_row_cutoff_ratio and pages_processed > 0:
            warnings.append("duplicate_row_cutoff_reached")
            partial = True
            break

        if not unique_page_rows:
            low_yield_streak += 1
            warnings.append("page_yield_low")
        else:
            low_yield_streak = 0

        rows.extend(unique_page_rows)
        pages_processed += 1

        if len(rows) >= max_rows:
            break

        if low_yield_streak > max_consecutive_low_yield_pages:
            warnings.append("consecutive_low_yield_cutoff_reached")
            partial = True
            break

        next_url, pagination_warnings = find_next_page_url(html=html, base_url=current_url, current_url=current_url)
        warnings.extend(pagination_warnings)
        if not next_url:
            break
        current_url = next_url

    if not rows:
        rows = generate_rows(url, fields, count=min(max_rows, 20))
        warnings.append("fallback_generated_rows")
        pages_processed = max(pages_processed, 1)

    page_type = "listing" if len(rows) > 1 else "detail"
    return FullScrapeResult(
        fields=fields,
        page_type=page_type,
        rows=rows,
        warnings=_dedupe_strings(warnings),
        pages_processed=pages_processed,
        partial=partial,
    )


def parse_rows_from_html(
    html: str,
    base_url: str,
    fields: list[FieldInfo],
    max_rows: int,
) -> tuple[list[dict[str, object]], list[str]]:
    soup = BeautifulSoup(html, "html.parser")

    candidates = detect_repeated_containers(soup)
    warnings: list[str] = []

    rows: list[dict[str, object]] = []
    if candidates:
        for container in candidates[: max_rows * 2]:
            row = extract_row(container=container, base_url=base_url, fields=fields)
            if _has_substance(row) and _is_probable_product_row(row):
                rows.append(row)
            if len(rows) >= max_rows:
                break
    else:
        warnings.append("container_detection_fallback")
        for anchor in soup.find_all("a", href=True)[:max_rows]:
            anchor_text = anchor.get_text(" ", strip=True)
            if not anchor_text or _looks_like_navigation_text(anchor_text):
                continue
            parent_container = anchor.find_parent(ITEM_CONTAINER_TAGS)
            if parent_container and _is_candidate_container(parent_container):
                row = extract_row(container=parent_container, base_url=base_url, fields=fields)
            else:
                row = {}
                for field in fields:
                    if field.name in {"title", "name"}:
                        row[field.name] = anchor_text
                    elif field.name in {"product_url", "url", "link"}:
                        row[field.name] = urljoin(base_url, anchor.get("href", ""))
                    else:
                        row[field.name] = ""
            if _is_probable_product_row(row):
                rows.append(row)

    deduped = dedupe_rows(rows)
    return deduped[:max_rows], warnings


def detect_repeated_containers(soup: BeautifulSoup) -> list[Tag]:
    best_group: list[Tag] = []
    best_score = -1.0

    for parent in soup.find_all(COLLECTION_PARENT_TAGS):
        direct_children = [child for child in parent.find_all(ITEM_CONTAINER_TAGS, recursive=False)]
        candidates = [child for child in direct_children if _is_candidate_container(child)]
        if len(candidates) < 2:
            continue

        # Keep full direct-child order for the best product collection parent.
        # Group-by-signature can drop legitimate cards when structures vary slightly.
        diversity = len({container_signature(node) for node in candidates}) / max(1, len(candidates))
        score = _score_container_group(candidates) + min(diversity, 0.6)
        if score > best_score:
            best_score = score
            best_group = candidates

    if best_group:
        return _unique_tags(best_group)

    return _anchor_parent_grouping(soup)


def container_signature(node: Tag) -> str:
    child_tags = [child.name for child in node.find_all(recursive=False) if isinstance(child, Tag)]
    classes = node.get("class", [])
    class_token = ".".join(classes[:2]) if isinstance(classes, list) else str(classes)
    anchor_count = len(node.find_all("a", href=True))
    heading_count = len(node.find_all(["h1", "h2", "h3", "h4"]))
    return f"{node.name}|{class_token}|{'-'.join(child_tags[:6])}|a{anchor_count}|h{heading_count}"


def extract_row(container: Tag, base_url: str, fields: list[FieldInfo]) -> dict[str, object]:
    text = container.get_text(" ", strip=True)
    anchor = _select_primary_anchor(container)
    title = _extract_title(container, anchor, text)
    price = _extract_price(container, text)
    rating = _extract_rating(container, text)
    product_url = _extract_product_url(base_url, anchor)

    row: dict[str, object] = {}
    for field in fields:
        if field.name in {"title", "name"}:
            row[field.name] = title
            continue

        if field.name == "price":
            row[field.name] = price
            continue

        if field.name == "rating":
            row[field.name] = rating
            continue

        if field.name == "review_count":
            row[field.name] = _extract_review_count(container, text)
            continue

        if field.name == "raw_text":
            row[field.name] = text
            continue

        if field.name in {"product_url", "url", "link"}:
            row[field.name] = product_url
            continue

        if field.name == "availability":
            row[field.name] = _extract_availability(container, text)
            continue

        if field.name in {"seller", "company"}:
            row[field.name] = _extract_named_entity(text, field.name)
            continue

        row[field.name] = _extract_generic_field(field_name=field.name, text=text, title=title, product_url=product_url)

    return row


def _is_candidate_container(node: Tag) -> bool:
    if _has_navigation_context(node):
        return False

    text = node.get_text(" ", strip=True)
    if len(text) < MIN_ITEM_TEXT_LENGTH:
        return False

    anchors = node.find_all("a", href=True)
    if not anchors and not node.find(["h1", "h2", "h3", "h4"]):
        return False

    if anchors:
        anchor_texts = [anchor.get_text(" ", strip=True) for anchor in anchors[:5]]
        cleaned = [text for text in anchor_texts if text]
        if cleaned and all(_looks_like_navigation_text(value) for value in cleaned):
            has_signal = bool(PRICE_PATTERN.search(text) or RATING_PATTERN.search(text))
            if not has_signal:
                return False

    return True


def _anchor_parent_grouping(soup: BeautifulSoup) -> list[Tag]:
    grouped: dict[str, list[Tag]] = {}

    for anchor in soup.find_all("a", href=True):
        parent = anchor.find_parent(ITEM_CONTAINER_TAGS)
        if not parent or not _is_candidate_container(parent):
            continue
        signature = container_signature(parent)
        grouped.setdefault(signature, []).append(parent)

    repeated_groups = [group for group in grouped.values() if len(group) >= 2]
    if not repeated_groups:
        return []

    scored = sorted(repeated_groups, key=_score_container_group, reverse=True)
    return _unique_tags(scored[0])


def _score_container_group(group: list[Tag]) -> float:
    if not group:
        return 0.0

    count = len(group)
    texts = [node.get_text(" ", strip=True) for node in group]
    avg_text_len = sum(len(text) for text in texts) / count
    link_ratio = sum(1 for node in group if node.find("a", href=True)) / count
    heading_ratio = sum(1 for node in group if node.find(["h1", "h2", "h3", "h4"])) / count
    price_ratio = sum(1 for text in texts if PRICE_PATTERN.search(text)) / count
    rating_ratio = sum(1 for text in texts if RATING_PATTERN.search(text)) / count

    titles = [_extract_title(node, _select_primary_anchor(node), node.get_text(" ", strip=True)) for node in group]
    non_empty_titles = [title for title in titles if title]
    unique_title_ratio = len(set(non_empty_titles)) / max(1, len(non_empty_titles))

    nav_penalty = sum(1 for title in non_empty_titles if _looks_like_navigation_text(title)) / max(1, len(non_empty_titles))

    return (
        (count * 2.0)
        + min(avg_text_len / 120.0, 2.0)
        + (link_ratio * 1.2)
        + (heading_ratio * 1.1)
        + (price_ratio * 2.2)
        + (rating_ratio * 1.5)
        + (unique_title_ratio * 1.8)
        - (nav_penalty * 3.0)
    )


def _unique_tags(nodes: list[Tag]) -> list[Tag]:
    unique_nodes: list[Tag] = []
    seen: set[int] = set()
    for node in nodes:
        marker = id(node)
        if marker in seen:
            continue
        seen.add(marker)
        unique_nodes.append(node)
    return unique_nodes


def _has_navigation_context(node: Tag) -> bool:
    if node.name in {"nav", "header", "footer"}:
        return True

    for parent in node.parents:
        if not isinstance(parent, Tag):
            continue
        if parent.name in {"nav", "header", "footer"}:
            return True
        classes = " ".join(parent.get("class", [])) if parent.get("class") else ""
        if re.search(r"(nav|menu|breadcrumb|footer|header)", classes, re.IGNORECASE):
            return True
    return False


def _looks_like_navigation_text(text: str) -> bool:
    value = " ".join(text.split()).strip().lower()
    if not value:
        return True
    if TITLE_BLACKLIST_PATTERN.match(value):
        return True
    if value.startswith(("shop by ", "browse ", "view all")) and len(value) < 40:
        return True
    return False


def _select_primary_anchor(container: Tag) -> Tag | None:
    anchors = container.find_all("a", href=True)
    if not anchors:
        return None

    best_anchor: Tag | None = None
    best_score = -1.0
    for anchor in anchors:
        href = (anchor.get("href") or "").strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue

        text = anchor.get_text(" ", strip=True)
        score = 0.0
        if text:
            score += min(len(text), 100) / 18.0
            if _looks_like_navigation_text(text):
                score -= 2.0
        if anchor.find(["h1", "h2", "h3", "h4"]):
            score += 2.0
        if re.search(r"(product|item|details?|dp/|/p/)", href, re.IGNORECASE):
            score += 0.8
        if href.startswith("/"):
            score += 0.2

        if score > best_score:
            best_score = score
            best_anchor = anchor

    return best_anchor or anchors[0]


def _extract_title(container: Tag, anchor: Tag | None, fallback_text: str) -> str:
    headings = container.find_all(["h1", "h2", "h3", "h4"])
    for heading in headings:
        value = heading.get_text(" ", strip=True)
        if value and not _looks_like_navigation_text(value):
            return _clean_product_title(value)[:160]

    if anchor:
        anchor_text = anchor.get_text(" ", strip=True)
        if anchor_text and not _looks_like_navigation_text(anchor_text):
            return _clean_product_title(anchor_text)[:160]

    for candidate in container.find_all(["span", "p", "div"], limit=10):
        value = candidate.get_text(" ", strip=True)
        if len(value) >= 6 and not _looks_like_navigation_text(value):
            if not PRICE_PATTERN.search(value) and not RATING_PATTERN.search(value):
                return _clean_product_title(value)[:160]

    cleaned = _clean_product_title(fallback_text)
    return cleaned[:160]


def _extract_price(container: Tag, text: str) -> str:
    class_prioritized = _extract_by_class_hint(container, PRICE_CLASS_HINT_PATTERN, PRICE_PATTERN)
    if class_prioritized:
        return _normalize_price(class_prioritized)

    class_with_code = _extract_by_class_hint(container, PRICE_CLASS_HINT_PATTERN, PRICE_WITH_CODE_PATTERN)
    if class_with_code:
        return _normalize_price(class_with_code)

    candidates = []
    symbol_match = PRICE_PATTERN.search(text)
    if symbol_match:
        candidates.append(symbol_match.group(0))
    code_match = PRICE_WITH_CODE_PATTERN.search(text)
    if code_match:
        candidates.append(code_match.group(0))

    if candidates:
        return _normalize_price(candidates[0])
    return ""


def _extract_rating(container: Tag, text: str) -> str:
    class_count_hint = _extract_by_class_hint(container, RATING_CLASS_HINT_PATTERN, RATING_BEFORE_COUNT_PATTERN, capture_group=1)
    if class_count_hint:
        normalized = _normalize_rating(class_count_hint)
        if normalized:
            return normalized

    class_word_hint = _extract_by_class_hint(container, RATING_CLASS_HINT_PATTERN, RATING_BEFORE_WORD_PATTERN, capture_group=1)
    if class_word_hint:
        normalized = _normalize_rating(class_word_hint)
        if normalized:
            return normalized

    class_review_hint = _extract_by_class_hint(
        container,
        RATING_CLASS_HINT_PATTERN,
        RATING_BEFORE_REVIEW_COUNT_PATTERN,
        capture_group=1,
    )
    if class_review_hint:
        normalized = _normalize_rating(class_review_hint)
        if normalized:
            return normalized

    class_prioritized = _extract_by_class_hint(container, RATING_CLASS_HINT_PATTERN, RATING_PATTERN, capture_group=1)
    if class_prioritized:
        normalized = _normalize_rating(class_prioritized)
        if normalized:
            return normalized

    class_alt = _extract_by_class_hint(container, RATING_CLASS_HINT_PATTERN, RATING_ALT_PATTERN, capture_group=1)
    if class_alt:
        normalized = _normalize_rating(class_alt)
        if normalized:
            return normalized

    before_count = RATING_BEFORE_COUNT_PATTERN.search(text)
    if before_count:
        normalized = _normalize_rating(before_count.group(1))
        if normalized:
            return normalized

    before_word = RATING_BEFORE_WORD_PATTERN.search(text)
    if before_word:
        normalized = _normalize_rating(before_word.group(1))
        if normalized:
            return normalized

    before_reviews = RATING_BEFORE_REVIEW_COUNT_PATTERN.search(text)
    if before_reviews:
        normalized = _normalize_rating(before_reviews.group(1))
        if normalized:
            return normalized

    match = RATING_PATTERN.search(text)
    if match:
        normalized = _normalize_rating(match.group(1))
        if normalized:
            return normalized

    alt_match = RATING_ALT_PATTERN.search(text)
    if alt_match:
        normalized = _normalize_rating(alt_match.group(1))
        if normalized:
            return normalized
    return ""


def _extract_review_count(container: Tag, text: str) -> int | None:
    class_prioritized = _extract_by_class_hint(
        container,
        RATING_CLASS_HINT_PATTERN,
        REVIEW_COUNT_PATTERN,
        capture_group=1,
    )
    if class_prioritized:
        return _extract_int_count(class_prioritized)

    match = REVIEW_COUNT_PATTERN.search(text)
    if not match:
        return None
    return _extract_int_count(match.group(1))


def _extract_product_url(base_url: str, anchor: Tag | None) -> str:
    if not anchor:
        return ""
    href = (anchor.get("href") or "").strip()
    return urljoin(base_url, href) if href else ""


def _extract_availability(container: Tag, text: str) -> str:
    class_hint = _extract_by_class_hint(
        container=container,
        class_pattern=AVAILABILITY_CLASS_HINT_PATTERN,
        value_pattern=AVAILABILITY_PATTERN,
        capture_group=1,
    )
    if class_hint:
        return _normalize_availability(class_hint)

    match = AVAILABILITY_PATTERN.search(text)
    if not match:
        return ""
    return _normalize_availability(match.group(1))


def _extract_generic_field(field_name: str, text: str, title: str, product_url: str) -> str:
    normalized = field_name.lower()
    if "title" in normalized or "name" in normalized:
        return title
    if "url" in normalized or "link" in normalized:
        return product_url
    return _clean_text(text)[:160]


def _clean_text(value: str) -> str:
    return " ".join(value.split()).strip()


def _clean_product_title(value: str) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        return ""

    for prefix in TITLE_PREFIX_CLEANUPS:
        cleaned = re.sub(rf"^\s*{re.escape(prefix)}\s*[:|-]?\s*", "", cleaned, flags=re.IGNORECASE)
    for prefix in TITLE_PREFIX_CLEANUPS:
        cleaned = re.sub(rf"\b{re.escape(prefix)}\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = _clean_text(cleaned)

    # Prefer cutting at explicit rating metadata boundary (e.g. "4.6 2,835 Ratings").
    rating_boundary = RATING_BEFORE_COUNT_PATTERN.search(cleaned) or RATING_BEFORE_WORD_PATTERN.search(cleaned)
    if rating_boundary:
        cleaned = cleaned[: rating_boundary.start()].strip()

    cleaned = TITLE_SIZE_SPEC_PATTERN.sub("", cleaned)
    cleaned = TITLE_TRAILING_METADATA_PATTERN.sub("", cleaned)
    cleaned = _clean_text(cleaned).strip(" ,;|-")

    # Keep variant in parentheses but drop metadata that follows it.
    if ")" in cleaned:
        before, after = cleaned.split(")", 1)
        if after and re.search(
            r"(ratings?|reviews?|gb\b|tb\b|cm\b|inch\b|display|camera|rom|ram|processor|battery|ssd|hdd)",
            after,
            re.IGNORECASE,
        ):
            cleaned = f"{before})".strip()

    return _clean_text(cleaned)


def _normalize_price(value: str) -> str:
    cleaned = _clean_text(value)
    # Keep original currency symbol/code and separators, just normalize spaces.
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _normalize_rating(value: str) -> str:
    cleaned = _clean_text(value).replace(",", ".")
    match = re.search(r"\d(?:\.\d+)?", cleaned)
    if not match:
        return ""
    numeric = float(match.group(0))
    if numeric > 5:
        return ""
    return f"{numeric:.1f}".rstrip("0").rstrip(".")


def _normalize_availability(value: str) -> str:
    normalized = _clean_text(value).lower()
    if "pre order" in normalized or "pre-order" in normalized or "preorder" in normalized:
        return "pre order"
    if "currently unavailable" in normalized:
        return "currently unavailable"
    if "coming soon" in normalized:
        return "coming soon"
    if "out of stock" in normalized:
        return "out of stock"
    if "in stock" in normalized:
        return "in stock"
    if "available" in normalized:
        return "available"
    return ""


def transform_rows_for_prompt_schema(
    *,
    fields: list[FieldInfo],
    rows: list[dict[str, object]],
    prompt: str | None,
    page_url: str,
) -> tuple[list[FieldInfo], list[dict[str, object]], list[str]]:
    if not rows or not _prompt_requests_structured_product_schema(prompt):
        return fields, rows, []

    transformed: list[dict[str, object]] = []
    for row in rows:
        transformed.append(_build_structured_product_row(row=row, page_url=page_url))

    deduped = _dedupe_structured_product_rows(transformed)
    warnings = ["structured_product_schema_applied"]
    if len(deduped) < len(transformed):
        warnings.append("structured_product_rows_deduped")
    return _with_scores(STRUCTURED_PRODUCT_FIELDS), deduped, warnings


def _prompt_requests_structured_product_schema(prompt: str | None) -> bool:
    if not prompt:
        return False

    normalized = prompt.lower().replace("-", "_")
    required_hits = 0
    for token in ("brand", "category", "model", "ram", "storage"):
        if token in normalized:
            required_hits += 1

    has_price_inr = bool(re.search(r"price\s*_?\s*inr", normalized))
    has_product_url = "product_url" in normalized or "product url" in normalized
    return required_hits >= 3 and has_price_inr and has_product_url


def _build_structured_product_row(row: dict[str, object], page_url: str) -> dict[str, object]:
    raw_title = str(row.get("title", "") or row.get("name", "")).strip()
    source_text = str(row.get("raw_text", "")).strip()
    title = _clean_product_title(raw_title)
    raw_product_url = str(row.get("product_url", "") or row.get("url", "") or row.get("link", "")).strip()
    product_url = _canonical_product_url(raw_product_url)
    slug = _product_slug_from_url(product_url)

    brand = _extract_brand(title=title, slug=slug)
    category = _infer_category(title=title, slug=slug, page_url=page_url)
    processor = _extract_processor(title=title, slug=slug, brand=brand, source_text=source_text)
    sku, sku_confidence = _extract_sku_with_confidence(title=title, slug=slug, source_text=source_text)
    model = _extract_model(title=title, brand=brand, processor=processor, sku=sku)
    product_family = _extract_product_family(model=model, brand=brand)
    is_canonical_name, name_source = _infer_name_canonicality(
        title=title,
        brand=brand,
        product_family=product_family,
        model=model,
    )
    ram, storage = _extract_memory_specs(title=title, slug=slug, source_text=source_text)
    display = _extract_display(title=title, slug=slug, source_text=source_text)
    os_family, os_version = _extract_os_parts(
        title=title,
        slug=slug,
        category=category,
        brand=brand,
        source_text=source_text,
    )
    os_name = os_family
    price_inr = _extract_price_inr(row.get("price"))
    rating = _extract_numeric_rating(row.get("rating"))
    if rating is None:
        rating = _extract_numeric_rating_from_text(source_text or raw_title)
    review_count = _extract_int_count(row.get("review_count"))
    if review_count is None:
        review_count = _extract_review_count_from_text(source_text or raw_title)
    review_scope = _infer_review_scope(
        title=title,
        source_text=source_text or raw_title,
        review_count=review_count,
        rating=rating,
    )
    review_count_timestamp = _now_utc_iso() if review_count is not None else None
    availability = _extract_structured_availability(row=row, source_text=source_text or raw_title)

    return {
        "brand": brand,
        "category": category,
        "product_family": product_family,
        "model": model,
        "sku": sku,
        "sku_confidence": sku_confidence,
        "ram": ram,
        "storage": storage,
        "processor": processor,
        "display": display,
        "os_family": os_family,
        "os_version": os_version,
        "os": os_name,
        "is_canonical_name": is_canonical_name,
        "name_source": name_source,
        "price_inr": price_inr,
        "rating": rating,
        "review_count": review_count,
        "review_scope": review_scope,
        "review_count_timestamp": review_count_timestamp,
        "availability": availability,
        "product_url": product_url,
    }


def _dedupe_structured_product_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    # Keep one canonical row per logical variant identity.
    # This intentionally ignores per-seller SKU URL differences.
    deduped: dict[tuple[str, str, str, str, str], dict[str, object]] = {}
    order: list[tuple[str, str, str, str, str]] = []

    for row in rows:
        key = (
            _normalize_signature_value(row.get("brand")),
            _normalize_signature_value(row.get("category")),
            _normalize_signature_value(row.get("model")),
            _normalize_signature_value(row.get("ram")),
            _normalize_signature_value(row.get("storage")),
        )
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = row
            order.append(key)
            continue

        deduped[key] = _merge_structured_rows(existing=existing, incoming=row)

    return [deduped[key] for key in order]


def _merge_structured_rows(existing: dict[str, object], incoming: dict[str, object]) -> dict[str, object]:
    # Preserve first-seen row for stable ordering/URL, only fill missing values.
    merged = dict(existing)
    for field in (
        "brand",
        "category",
        "product_family",
        "model",
        "sku",
        "sku_confidence",
        "ram",
        "storage",
        "processor",
        "display",
        "os_family",
        "os_version",
        "os",
        "is_canonical_name",
        "name_source",
        "price_inr",
        "rating",
        "review_count",
        "review_scope",
        "review_count_timestamp",
        "availability",
        "product_url",
    ):
        existing_value = merged.get(field)
        incoming_value = incoming.get(field)
        if field == "sku_confidence":
            if isinstance(existing_value, (int, float)) and isinstance(incoming_value, (int, float)):
                if float(incoming_value) > float(existing_value):
                    merged[field] = incoming_value
                continue
            if _is_blank_value(existing_value) and not _is_blank_value(incoming_value):
                merged[field] = incoming_value
            continue

        if field == "is_canonical_name":
            if existing_value is True or incoming_value is True:
                merged[field] = True
                continue
            if existing_value is False and incoming_value is False:
                merged[field] = False
                continue
            if existing_value is None and incoming_value in {True, False}:
                merged[field] = incoming_value
            continue

        if _is_blank_value(existing_value) and not _is_blank_value(incoming_value):
            merged[field] = incoming_value
    return merged


def _canonical_product_url(value: str) -> str:
    cleaned = _clean_text(value)
    if not cleaned:
        return ""
    parsed = urlparse(cleaned)
    if not parsed.scheme or not parsed.netloc:
        return cleaned
    return parsed._replace(query="", fragment="").geturl()


def _product_slug_from_url(product_url: str) -> str:
    if not product_url:
        return ""
    path = urlparse(product_url).path.strip("/")
    if not path:
        return ""
    parts = [part for part in path.split("/") if part]
    if not parts:
        return ""
    if "p" in parts:
        index = parts.index("p")
        if index > 0:
            return parts[index - 1]
    return parts[0]


def _extract_brand(title: str, slug: str) -> str:
    candidate = ""
    if title:
        first = title.split()[0]
        if first and not first[0].isdigit():
            candidate = first

    if not candidate and slug:
        first_slug = slug.split("-", 1)[0]
        if first_slug and first_slug.isalpha():
            candidate = first_slug

    if not candidate:
        return ""

    lowered = candidate.lower()
    brand_fixups = {
        "apple": "Apple",
        "samsung": "Samsung",
        "lenovo": "Lenovo",
        "dell": "Dell",
        "hp": "HP",
        "asus": "ASUS",
        "acer": "Acer",
        "msi": "MSI",
    }
    return brand_fixups.get(lowered, candidate.title())


def _extract_model(title: str, brand: str, processor: str | None = None, sku: str | None = None) -> str:
    model = title
    known_brand_prefixes = {"apple", "samsung", "lenovo", "dell", "hp", "asus", "acer", "msi", "motorola"}
    if brand and brand.lower() in known_brand_prefixes and model.lower().startswith(f"{brand.lower()} "):
        model = model[len(brand) :].strip()

    # Remove bracketed variant/spec metadata chunks.
    model = re.sub(
        r"\((?:[^)]*\b(?:gb|tb|ram|rom|ssd|hdd|ufs|emmc|windows|macos|display|inch|cm)\b[^)]*)\)",
        "",
        model,
        flags=re.IGNORECASE,
    )
    model = re.sub(r"\(\s*\d{4}\s*\)", "", model)
    model = re.sub(
        r"\b\d+\s*(?:gb|tb)\b(?:\s*(?:ram|rom|ssd|hdd|ufs|emmc))?",
        "",
        model,
        flags=re.IGNORECASE,
    )
    model = re.sub(r"\b\d+(?:\.\d+)?\s*(?:inch|inches|cm)\b", "", model, flags=re.IGNORECASE)

    if sku:
        model = re.sub(re.escape(sku), "", model, flags=re.IGNORECASE)
    model = SKU_PATTERN.sub("", model)
    model = PROCESSOR_TRAIL_PATTERN.sub("", model)
    if processor:
        model = re.sub(re.escape(processor), "", model, flags=re.IGNORECASE)

    model = re.sub(r"\s*[-|:]\s*\(\s*$", "", model)
    if model.count("(") > model.count(")"):
        model = model[: model.rfind("(")].strip()
    model = re.sub(r"[-|:/\s(]+$", "", model)
    model = re.sub(r"\bmacbook\b", "MacBook", model, flags=re.IGNORECASE)
    model = re.sub(r"\bair\b", "Air", model, flags=re.IGNORECASE)
    model = re.sub(r"\bpro\b", "Pro", model, flags=re.IGNORECASE)
    model = re.sub(r"\s+", " ", model).strip(" ,;|-")

    # Family-specific canonical cleanup.
    mac_match = re.search(r"\bMacBook\s+(Air|Pro)\b", model, flags=re.IGNORECASE)
    if mac_match:
        base = f"MacBook {mac_match.group(1).title()}"
        chip_match = re.search(r"\bM([1-9])\b", model, flags=re.IGNORECASE)
        if not chip_match and processor:
            chip_match = re.search(r"\bM([1-9])\b", processor, flags=re.IGNORECASE)
        if chip_match:
            return f"{base} M{chip_match.group(1)}"
        return base

    galaxy_match = re.search(r"\bGalaxy\s+Book\d+\b", model, flags=re.IGNORECASE)
    if galaxy_match:
        return _clean_text(galaxy_match.group(0).replace("galaxy", "Galaxy").replace("book", "Book"))

    moto_match = re.search(r"\bMotobook\s+\d+(?:\s+Pro)?\b", model, flags=re.IGNORECASE)
    if moto_match:
        return _clean_text(moto_match.group(0).replace("motobook", "Motobook"))

    return model


def _extract_product_family(model: str, brand: str) -> str | None:
    if not model:
        return None

    if brand.lower() == "apple":
        mac_match = re.search(r"\bMacBook\s+(Air|Pro)\b", model, flags=re.IGNORECASE)
        if mac_match:
            return f"MacBook {mac_match.group(1).title()}"
        if re.search(r"\bMacBook\b", model, flags=re.IGNORECASE):
            return "MacBook"

    galaxy_match = re.search(r"\bGalaxy\s+Book\d+\b", model, flags=re.IGNORECASE)
    if galaxy_match:
        return _clean_text(galaxy_match.group(0).replace("galaxy", "Galaxy").replace("book", "Book"))

    moto_match = re.search(r"\bMotobook\s+\d+\b", model, flags=re.IGNORECASE)
    if moto_match:
        return _clean_text(moto_match.group(0).replace("motobook", "Motobook"))

    words = model.split()
    if len(words) >= 2:
        return " ".join(words[:2])
    return model if model else brand


def _extract_sku(title: str, slug: str, source_text: str = "") -> str | None:
    sku, _confidence = _extract_sku_with_confidence(title=title, slug=slug, source_text=source_text)
    return sku


def _extract_sku_with_confidence(title: str, slug: str, source_text: str = "") -> tuple[str | None, float | None]:
    haystack = f"{source_text} {title}"
    match = SKU_PATTERN.search(haystack)
    if match:
        normalized = _normalize_sku(match.group(1))
        if not normalized:
            return None, None
        confidence = 0.95 if SKU_NORMALIZED_PATTERN.match(normalized) else 0.85
        return normalized, confidence

    # Fallback from URL slug tail (common marketplace part-number format).
    tokens = [token for token in slug.split("-") if token]
    if len(tokens) >= 2:
        candidate_left = tokens[-2]
        candidate_right = tokens[-1]
        candidate = f"{candidate_left}-{candidate_right}"
        stopwords = {
            "windows",
            "macos",
            "intel",
            "core",
            "ultra",
            "series",
            "ssd",
            "hdd",
            "display",
            "home",
            "pro",
        }
        if candidate_left.lower() in stopwords:
            return None, None
        if re.fullmatch(r"(?=[a-z0-9-]*\d)[a-z]{2,}[a-z0-9]{3,}-[a-z0-9]{1,8}", candidate, flags=re.IGNORECASE):
            left_upper = candidate_left.upper()
            right_upper = candidate_right.upper()
            normalized = _normalize_sku(f"{left_upper}-{right_upper}")
            if normalized:
                return normalized, 0.7
    return None, None


def _normalize_sku(value: str) -> str:
    cleaned = _clean_text(value).upper()
    cleaned = cleaned.replace("/", "-")
    cleaned = re.sub(r"-{2,}", "-", cleaned)
    normalized = cleaned.strip("-")
    if not normalized:
        return ""
    if not SKU_NORMALIZED_PATTERN.match(normalized):
        return ""
    return normalized


def _extract_processor(title: str, slug: str, brand: str, source_text: str = "") -> str | None:
    haystack = f"{source_text} {title} {slug.replace('-', ' ')}"

    intel_ultra = PROCESSOR_INTEL_CORE_ULTRA_PATTERN.search(haystack)
    if intel_ultra:
        lower = _clean_text(intel_ultra.group(0)).lower()
        match = re.search(r"core\s+ultra\s+([3579])\s+(\d{3,4}[a-z]?)", lower)
        if match:
            return f"Intel Core Ultra {match.group(1)} {match.group(2).upper()}"
        return "Intel Core Ultra"

    intel_series = PROCESSOR_INTEL_CORE_SERIES_PATTERN.search(haystack)
    if intel_series:
        lower = _clean_text(intel_series.group(0)).lower()
        match = re.search(r"core\s+([3579])\s+series\s+(\d+)\s+(\d{3,4}[a-z]?)", lower)
        if match:
            return f"Intel Core {match.group(1)} Series {match.group(2)} {match.group(3).upper()}"
        return "Intel Core"

    a_match = PROCESSOR_A_SERIES_PATTERN.search(haystack)
    if a_match:
        token = f"A{a_match.group(1)}"
        if a_match.group(2):
            token = f"{token} Pro"
        return f"Apple {token}" if brand.lower() == "apple" else token

    m_match = PROCESSOR_APPLE_SILICON_PATTERN.search(haystack)
    if m_match:
        token = f"M{m_match.group(1)}"
        suffix = _clean_text(m_match.group(2) or "")
        if suffix:
            token = f"{token} {suffix}"
        return f"Apple {token}" if brand.lower() == "apple" else token

    intel_match = PROCESSOR_INTEL_PATTERN.search(haystack)
    if intel_match:
        return f"Intel Core i{intel_match.group(1)}"

    ryzen_match = PROCESSOR_RYZEN_PATTERN.search(haystack)
    if ryzen_match:
        return f"AMD Ryzen {ryzen_match.group(1)}"

    return None


def _extract_display(title: str, slug: str, source_text: str = "") -> str | None:
    haystack = f"{source_text} {title} {slug.replace('-', ' ')}"

    inch_match = DISPLAY_INCH_PATTERN.search(haystack)
    if inch_match:
        return f"{inch_match.group(1)} inch"

    cm_match = DISPLAY_CM_PATTERN.search(haystack)
    if cm_match:
        return f"{cm_match.group(1)} cm"

    return None


def _extract_os(title: str, slug: str, category: str, brand: str, source_text: str = "") -> str | None:
    family, _version = _extract_os_parts(
        title=title,
        slug=slug,
        category=category,
        brand=brand,
        source_text=source_text,
    )
    return family


def _extract_os_parts(
    title: str,
    slug: str,
    category: str,
    brand: str,
    source_text: str = "",
) -> tuple[str | None, str | None]:
    haystack = f"{source_text} {title} {slug}".lower()

    windows_match = WINDOWS_OS_PATTERN.search(haystack)
    if windows_match:
        family = "Windows"
        version = f"Windows {windows_match.group(1)}"
        edition = windows_match.group(2)
        if edition:
            version = f"{version} {edition.title()}"
        return family, version

    macos_match = MACOS_OS_PATTERN.search(haystack)
    if macos_match:
        family = "macOS"
        release = macos_match.group(1) or macos_match.group(2)
        if release:
            return family, _normalize_os_release(release)
        return family, None

    if "chrome os" in haystack or "chromeos" in haystack:
        return "ChromeOS", None
    if "linux" in haystack or "ubuntu" in haystack:
        return "Linux", None
    if "android" in haystack:
        match = re.search(r"\bandroid\s*(\d+(?:\.\d+)?)\b", haystack)
        return "Android", f"Android {match.group(1)}" if match else None
    if "ios" in haystack:
        match = re.search(r"\bios\s*(\d+(?:\.\d+)?)\b", haystack)
        return "iOS", f"iOS {match.group(1)}" if match else None

    if brand.lower() == "apple" and category == "laptop":
        return "macOS", None

    return None, None


def _normalize_os_release(value: str) -> str:
    normalized = _clean_text(value).replace("  ", " ").strip().lower()
    mapping = {
        "big sur": "Big Sur",
        "monterey": "Monterey",
        "ventura": "Ventura",
        "sonoma": "Sonoma",
        "sequoia": "Sequoia",
    }
    return mapping.get(normalized, normalized.title())


def _extract_structured_availability(row: dict[str, object], source_text: str = "") -> str | None:
    raw = str(row.get("availability") or "").strip()
    normalized = _normalize_availability(raw or source_text)
    return normalized or None


def _extract_memory_specs(title: str, slug: str, source_text: str = "") -> tuple[str, str]:
    source = f"{source_text} {title}"
    explicit_pair = re.search(
        r"\b(\d{1,3})\s*gb\s*/\s*(\d{2,4})\s*(gb|tb)(?:\s*(ssd|hdd|ufs|rom|emmc))?\b",
        source,
        flags=re.IGNORECASE,
    )
    if explicit_pair:
        ram = f"{explicit_pair.group(1)} GB"
        storage = f"{explicit_pair.group(2)} {explicit_pair.group(3).upper()}"
        if explicit_pair.group(4):
            suffix = explicit_pair.group(4)
            storage = f"{storage} {'eMMC' if suffix.lower() == 'emmc' else suffix.upper()}"
        return ram, storage

    tokens = [token for token in slug.split("-") if token]
    memory_specs: list[tuple[str, str, int]] = []

    for index, token in enumerate(tokens):
        compact = re.fullmatch(r"(\d{1,4})(gb|tb)", token)
        if compact:
            memory_specs.append((compact.group(1), compact.group(2).upper(), index + 1))
            continue

        if not token.isdigit() or index + 1 >= len(tokens):
            continue
        unit = tokens[index + 1].lower()
        if unit in {"gb", "tb"}:
            memory_specs.append((token, unit.upper(), index + 2))

    ram = ""
    storage = ""
    if len(memory_specs) >= 2:
        ram = f"{memory_specs[0][0]} {memory_specs[0][1]}"
        storage = f"{memory_specs[1][0]} {memory_specs[1][1]}"
        storage_suffix = _storage_suffix(tokens, memory_specs[1][2])
        if storage_suffix:
            storage = f"{storage} {storage_suffix}"
        return ram, storage

    if len(memory_specs) == 1:
        value = f"{memory_specs[0][0]} {memory_specs[0][1]}"
        if re.search(rf"\b{memory_specs[0][0]}\s*{memory_specs[0][1]}\s*ram\b", title, re.IGNORECASE):
            return value, ""

        storage = value
        storage_suffix = _storage_suffix(tokens, memory_specs[0][2])
        if storage_suffix:
            storage = f"{storage} {storage_suffix}"
        return "", storage

    return "", ""


def _storage_suffix(tokens: list[str], start_index: int) -> str:
    if start_index >= len(tokens):
        return ""
    suffix_map = {
        "ssd": "SSD",
        "hdd": "HDD",
        "ufs": "UFS",
        "rom": "ROM",
        "emmc": "eMMC",
    }
    token = tokens[start_index].lower()
    return suffix_map.get(token, "")


def _extract_price_inr(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)

    cleaned = _clean_text(str(value))
    digits = re.sub(r"[^\d]", "", cleaned)
    if not digits:
        return None
    return int(digits)


def _extract_int_count(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        parsed = int(value)
        return parsed if parsed >= 0 else None

    digits = re.sub(r"[^\d]", "", _clean_text(str(value)))
    if not digits:
        return None
    return int(digits)


def _extract_numeric_rating(value: object) -> float | None:
    if value is None:
        return None
    normalized = _normalize_rating(str(value))
    if not normalized:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def _extract_numeric_rating_from_text(value: str) -> float | None:
    if not value:
        return None

    before_count = RATING_BEFORE_COUNT_PATTERN.search(value)
    if before_count:
        normalized = _normalize_rating(before_count.group(1))
        if normalized:
            return float(normalized)

    before_word = RATING_BEFORE_WORD_PATTERN.search(value)
    if before_word:
        normalized = _normalize_rating(before_word.group(1))
        if normalized:
            return float(normalized)

    before_reviews = RATING_BEFORE_REVIEW_COUNT_PATTERN.search(value)
    if before_reviews:
        normalized = _normalize_rating(before_reviews.group(1))
        if normalized:
            return float(normalized)

    rating_match = RATING_PATTERN.search(value)
    if rating_match:
        normalized = _normalize_rating(rating_match.group(1))
        if normalized:
            return float(normalized)

    alt_match = RATING_ALT_PATTERN.search(value)
    if alt_match:
        normalized = _normalize_rating(alt_match.group(1))
        if normalized:
            return float(normalized)

    return None


def _extract_review_count_from_text(value: str) -> int | None:
    if not value:
        return None

    match = REVIEW_COUNT_PATTERN.search(value)
    if not match:
        return None
    return _extract_int_count(match.group(1))


def _infer_name_canonicality(
    title: str,
    brand: str,
    product_family: str | None,
    model: str,
) -> tuple[bool | None, str | None]:
    if not model:
        return None, None

    normalized_model = _clean_text(model)
    normalized_title = _clean_text(title)
    if not normalized_model:
        return None, None

    # Canonical patterns represent catalog-grade normalized names.
    canonical_patterns = (
        r"^MacBook (Air|Pro)(?: M[1-9])?$",
        r"^Galaxy Book\d+$",
        r"^Motobook \d+(?: Pro)?$",
    )
    for pattern in canonical_patterns:
        if re.fullmatch(pattern, normalized_model, flags=re.IGNORECASE):
            return True, "catalog_pattern"

    noisy_markers = (
        "add to compare",
        "currently unavailable",
        "coming soon",
        "trending",
        "pre order",
        "http://",
        "https://",
        "pid=",
    )
    haystack = f"{normalized_title} {normalized_model}".lower()
    if any(marker in haystack for marker in noisy_markers):
        return False, "marketplace_title"

    if re.search(r"[?=&]", normalized_model):
        return False, "marketplace_title"
    if normalized_model.endswith("(") or normalized_model.endswith("-"):
        return False, "marketplace_title"
    if brand.lower() == "apple" and re.search(r"\bmacbook\s+neo\b", normalized_model, flags=re.IGNORECASE):
        return False, "marketplace_naming"
    if brand and normalized_model.lower().startswith(brand.lower()):
        return False, "brand_prefixed"

    return True, "normalized_title"


def _infer_review_scope(
    title: str,
    source_text: str,
    review_count: int | None,
    rating: float | None,
) -> str | None:
    if review_count is None and rating is None:
        return None

    haystack = f"{source_text} {title}".lower()
    has_variant_specs = bool(re.search(r"\b\d+\s*gb\b.*\b(ssd|hdd|rom|ufs)\b", haystack))
    has_bulk_rating_label = "ratings &" in haystack or "reviews" in haystack or "ratings" in haystack

    if has_variant_specs:
        return "variant"
    if has_bulk_rating_label:
        return "listing"
    return "unknown"


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _infer_category(title: str, slug: str, page_url: str) -> str:
    haystack = f"{title} {slug} {page_url}".lower()
    if any(token in haystack for token in ("macbook", "laptop", "notebook", "ultrabook")):
        return "laptop"
    if any(token in haystack for token in ("iphone", "smartphone", "mobile", "phone")):
        return "smartphone"
    if any(token in haystack for token in ("headphone", "earbud", "airpods")):
        return "audio"
    return "product"


def _extract_by_class_hint(
    container: Tag,
    class_pattern: re.Pattern[str],
    value_pattern: re.Pattern[str],
    capture_group: int = 0,
) -> str:
    for node in container.find_all(True, limit=120):
        classes = " ".join(node.get("class", [])) if node.get("class") else ""
        if classes and class_pattern.search(classes):
            value = node.get_text(" ", strip=True)
            match = value_pattern.search(value)
            if match:
                return match.group(capture_group)
    return ""


def find_next_page_url(html: str, base_url: str, current_url: str) -> tuple[str | None, list[str]]:
    soup = BeautifulSoup(html, "html.parser")
    warnings: list[str] = []

    rel_next = soup.find("a", attrs={"rel": "next"}, href=True)
    if rel_next:
        return urljoin(base_url, rel_next.get("href", "")), warnings

    head_next = soup.find("link", attrs={"rel": "next"}, href=True)
    if head_next:
        return urljoin(base_url, head_next.get("href", "")), warnings

    anchor_candidate = _find_pagination_anchor(soup=soup, base_url=base_url)
    if anchor_candidate:
        next_url, mode = anchor_candidate
        if mode == "load_more":
            warnings.append("pagination_load_more_detected")
        elif mode == "next":
            warnings.append("pagination_next_detected")
        elif mode == "infinite":
            warnings.append("pagination_infinite_scroll_hint_detected")
        return next_url, warnings

    data_url_candidate = _find_data_next_url(soup=soup, base_url=base_url)
    if data_url_candidate:
        warnings.append("pagination_data_next_detected")
        return data_url_candidate, warnings

    inferred = _infer_next_page_from_url(current_url=current_url, html=html)
    if inferred:
        warnings.append("pagination_query_increment_used")
        return inferred, warnings

    return None, warnings


def generate_rows(url: str, fields: list[FieldInfo], count: int, page: int = 1) -> list[dict[str, object]]:
    parsed = urlparse(url)
    host = parsed.netloc or "example.com"

    rows: list[dict[str, object]] = []
    for i in range(count):
        idx = ((page - 1) * count) + i + 1
        row: dict[str, object] = {}
        for field in fields:
            if field.name == "title":
                row[field.name] = f"Item {idx} from {host}"
            elif field.name == "price":
                row[field.name] = 999 + (idx * 5)
            elif field.name == "rating":
                row[field.name] = round(3.8 + ((idx % 10) / 10), 1)
            elif field.name == "product_url":
                row[field.name] = f"https://{host}/item/{idx}"
            elif field.name == "seller":
                row[field.name] = f"Seller {((idx - 1) % 7) + 1}"
            elif field.name == "company":
                row[field.name] = f"Company {((idx - 1) % 9) + 1}"
            else:
                row[field.name] = f"{field.name}-{idx}"
        rows.append(row)

    return rows


def _with_scores(fields: list[tuple[str, str]]) -> list[FieldInfo]:
    out: list[FieldInfo] = []
    for index, (name, kind) in enumerate(fields):
        confidence = field_score(
            selector_stability=0.88 - (index * 0.03),
            label_proximity=0.84,
            format_validity=0.9 if kind in {"money", "url", "rating"} else 0.82,
            template_reliability=0.75,
            signal_agreement=0.8,
        )
        out.append(FieldInfo(name=name, kind=kind, confidence=confidence))
    return out


def _extract_named_entity(text: str, field_name: str) -> str:
    if field_name == "seller":
        match = re.search(r"(?:seller|sold by)\s*[:\-]?\s*([A-Za-z0-9 &._-]{2,40})", text, re.IGNORECASE)
        return match.group(1).strip() if match else ""

    match = re.search(r"(?:company|by)\s*[:\-]?\s*([A-Za-z0-9 &._-]{2,40})", text, re.IGNORECASE)
    return match.group(1).strip() if match else ""


def _has_substance(row: dict[str, object]) -> bool:
    for value in row.values():
        if value is None:
            continue
        if isinstance(value, str) and value.strip():
            return True
        if not isinstance(value, str):
            return True
    return False


def _is_probable_product_row(row: dict[str, object]) -> bool:
    title = str(row.get("title", "") or row.get("name", "")).strip()
    price = str(row.get("price", "")).strip()
    rating = str(row.get("rating", "")).strip()
    product_url = str(row.get("product_url", "") or row.get("url", "") or row.get("link", "")).strip()

    if not title:
        return False
    if _looks_like_navigation_text(title):
        return False

    if price or rating:
        return True

    if product_url and _looks_like_product_url(product_url):
        return True

    # Conservative fallback for cases where price/rating are absent.
    return len(title.split()) >= 3


def _looks_like_product_url(url: str) -> bool:
    normalized = url.lower()
    parsed = urlparse(normalized)
    path = parsed.path or ""
    query = parsed.query or ""

    if any(token in path for token in ("/p/", "/dp/", "/product/", "/itm", "/item/")):
        return True
    if "pid=" in query:
        return True
    if path.endswith("/pr"):
        return False
    return False


def _extract_extension_html(extension_dom_payload: dict[str, object] | None) -> str | None:
    if not extension_dom_payload:
        return None

    html = extension_dom_payload.get("html")
    if isinstance(html, str) and html.strip():
        return html
    return None


def _dedupe_strings(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _is_blank_value(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def dedupe_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    seen: set[tuple[tuple[str, str], ...]] = set()
    out: list[dict[str, object]] = []

    for row in rows:
        signature = row_signature(row)
        if signature in seen:
            continue
        seen.add(signature)
        out.append(row)

    return out


def row_signature(row: dict[str, object]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((key, _normalize_signature_value(value)) for key, value in row.items()))


def filter_duplicate_rows(
    page_rows: list[dict[str, object]],
    seen_row_signatures: set[tuple[tuple[str, str], ...]],
) -> tuple[list[dict[str, object]], float]:
    if not page_rows:
        return [], 0.0

    duplicates = 0
    unique_rows: list[dict[str, object]] = []
    for row in page_rows:
        signature = row_signature(row)
        if signature in seen_row_signatures:
            duplicates += 1
            continue
        seen_row_signatures.add(signature)
        unique_rows.append(row)

    duplicate_ratio = duplicates / len(page_rows)
    return unique_rows, duplicate_ratio


def compute_page_signature(page_rows: list[dict[str, object]], html: str) -> str:
    if page_rows:
        signatures = [str(row_signature(row)) for row in page_rows[:20]]
        payload = "|".join(signatures)
    else:
        soup = BeautifulSoup(html, "html.parser")
        body_text = soup.get_text(" ", strip=True)[:1200]
        payload = body_text

    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _normalize_signature_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value).strip().lower()


def _find_pagination_anchor(soup: BeautifulSoup, base_url: str) -> tuple[str, str] | None:
    for anchor in soup.find_all("a", href=True):
        href = (anchor.get("href") or "").strip()
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue

        text = anchor.get_text(" ", strip=True)
        attrs = " ".join(
            [
                text,
                anchor.get("aria-label", ""),
                anchor.get("title", ""),
                " ".join(anchor.get("class", [])) if anchor.get("class") else "",
                anchor.get("id", ""),
            ]
        )

        if NEXT_TEXT_PATTERN.match(text) or PAGINATION_NEXT_HINT_PATTERN.search(attrs):
            return urljoin(base_url, href), "next"

        if PAGINATION_LOAD_MORE_HINT_PATTERN.search(attrs):
            return urljoin(base_url, href), "load_more"

        if _has_pagination_context(anchor):
            return urljoin(base_url, href), "infinite"

    return None


def _find_data_next_url(soup: BeautifulSoup, base_url: str) -> str | None:
    attributes = [
        "data-next-url",
        "data-next",
        "data-url",
        "data-href",
        "data-load-more-url",
        "data-page-url",
    ]
    for node in soup.find_all(True):
        for attr in attributes:
            candidate = node.get(attr)
            if isinstance(candidate, str) and candidate.strip():
                return urljoin(base_url, candidate.strip())

    for script in soup.find_all("script"):
        script_text = script.string or script.get_text(" ", strip=True)
        if not script_text:
            continue
        match = re.search(r'"(?:next|nextUrl|next_url|nextPage|next_page)"\s*:\s*"([^"]+)"', script_text)
        if match:
            return urljoin(base_url, match.group(1))
    return None


def _infer_next_page_from_url(current_url: str, html: str) -> str | None:
    parsed = urlparse(current_url)
    query = parsed.query
    if not query:
        return None

    if not _html_has_pagination_hints(html):
        return None

    candidates = ["page", "p", "pg", "pageno", "page_no"]
    for key in candidates:
        match = re.search(rf"([?&]{key}=)(\d+)", current_url, re.IGNORECASE)
        if not match:
            continue
        current_page = int(match.group(2))
        next_page = current_page + 1
        start, end = match.span(2)
        return f"{current_url[:start]}{next_page}{current_url[end:]}"
    return None


def _html_has_pagination_hints(html: str) -> bool:
    sample = html[:3500]
    if re.search(r"(next|load more|show more|pagination)", sample, re.IGNORECASE):
        return True
    return False


def _has_pagination_context(node: Tag) -> bool:
    for parent in node.parents:
        if not isinstance(parent, Tag):
            continue
        classes = " ".join(parent.get("class", [])) if parent.get("class") else ""
        attrs = f"{parent.get('id', '')} {classes}".strip()
        if attrs and PAGINATION_CONTAINER_HINT_PATTERN.search(attrs):
            return True
    return False
