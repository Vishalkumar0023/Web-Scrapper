#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

OUTPUT_FIELDS = [
    "brand",
    "category",
    "model",
    "variant",
    "ram",
    "storage",
    "processor",
    "display",
    "os",
    "price_inr",
    "rating",
    "review_count",
    "availability",
    "product_url",
]

NOISY_PREFIXES = (
    "add to compare",
    "currently unavailable",
    "coming soon",
    "pre order",
    "pre-order",
    "trending",
)

PLACEHOLDER_VALUES = {
    "",
    "na",
    "n/a",
    "none",
    "null",
    "unknown",
    "item",
    "product",
    "placeholder",
    "test",
}

CATEGORY_MAP = {
    "laptops": "laptop",
    "laptop": "laptop",
    "notebook": "laptop",
    "ultrabook": "laptop",
    "mobiles": "smartphone",
    "mobile": "smartphone",
    "phone": "smartphone",
    "smartphone": "smartphone",
    "tablets": "tablet",
    "tablet": "tablet",
}

CATEGORY_KEYWORDS = {
    "laptop": ("laptop", "notebook", "ultrabook", "macbook"),
    "smartphone": ("smartphone", "mobile", "iphone", "android phone"),
    "tablet": ("tablet", "ipad"),
}

BRAND_MAP = {
    "apple": "Apple",
    "samsung": "Samsung",
    "lenovo": "Lenovo",
    "dell": "Dell",
    "hp": "HP",
    "asus": "ASUS",
    "acer": "Acer",
    "msi": "MSI",
    "xiaomi": "Xiaomi",
    "oneplus": "OnePlus",
    "realme": "Realme",
    "oppo": "OPPO",
    "vivo": "Vivo",
}

VALID_AVAILABILITY = {"in_stock", "out_of_stock", "unavailable", "preorder"}


def replace_empty_with_none(value: Any) -> Any:
    if isinstance(value, str):
        cleaned = value.strip()
        return None if cleaned == "" else cleaned
    if isinstance(value, list):
        return [replace_empty_with_none(item) for item in value]
    if isinstance(value, dict):
        return {k: replace_empty_with_none(v) for k, v in value.items()}
    return value


def load_records(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("Input JSON must be an array of objects.")
    if not all(isinstance(item, dict) for item in data):
        raise ValueError("Every item in input JSON array must be an object.")
    return [replace_empty_with_none(item) for item in data]


def validate_schema_consistency(records: list[dict[str, Any]]) -> None:
    scalar_keys = {
        "title",
        "price",
        "rating",
        "review_count",
        "product_url",
        "url",
        "link",
        "brand",
        "category",
        "model",
        "variant",
        "ram",
        "storage",
        "processor",
        "display",
        "os",
        "price_inr",
        "availability",
    }

    for idx, row in enumerate(records):
        if not any(key in row for key in ("title", "model", "product_url", "url", "link")):
            raise ValueError(f"Record {idx} is missing product identity fields.")

        for key in scalar_keys:
            if key not in row:
                continue
            value = row[key]
            if isinstance(value, (dict, list)):
                raise ValueError(f"Record {idx} has non-scalar value for key '{key}'.")


def is_placeholder_value(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip().lower()
    return text in PLACEHOLDER_VALUES


def canonical_product_url(url_value: Any) -> str | None:
    if url_value is None:
        return None

    raw = str(url_value).strip()
    if not raw:
        return None

    if raw.startswith("//"):
        raw = f"https:{raw}"

    parsed = urlparse(raw)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None

    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    path = path.rstrip("/") or "/"

    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "", "", ""))


def is_valid_canonical_product_url(url_value: str | None) -> bool:
    if not url_value:
        return False

    parsed = urlparse(url_value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False

    if parsed.query or parsed.fragment or parsed.params:
        return False

    path = (parsed.path or "").lower()
    if path in {"", "/"}:
        return False

    if re.search(r"/(search|category|categories|collections?|offers?|deals?)(/|$)", path):
        return False

    if "flipkart.com" in parsed.netloc and "/p/" not in path:
        return False

    if "amazon." in parsed.netloc and "/dp/" not in path and "/gp/product/" not in path:
        return False

    return True


def parse_price_inr(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value) if int(value) > 0 else None

    digits = re.sub(r"[^\d]", "", str(value))
    if not digits:
        return None

    parsed = int(digits)
    return parsed if parsed > 0 else None


def parse_rating(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        return numeric if 0 <= numeric <= 5 else None

    text = str(value).strip().replace(",", ".")
    match = re.search(r"\d+(?:\.\d+)?", text)
    if not match:
        return None

    numeric = float(match.group(0))
    return numeric if 0 <= numeric <= 5 else None


def parse_review_count(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = int(value)
        return parsed if parsed >= 0 else None

    text = str(value).strip().lower().replace(",", "")
    compact = re.search(r"(\d+(?:\.\d+)?)\s*([km])\b", text)
    if compact:
        base = float(compact.group(1))
        suffix = compact.group(2)
        multiplier = 1000 if suffix == "k" else 1000000
        return int(base * multiplier)

    digits = re.sub(r"[^\d]", "", text)
    if not digits:
        return None

    parsed = int(digits)
    return parsed if parsed >= 0 else None


def infer_rating_from_text(*sources: Any) -> float | None:
    text = " ".join(str(source) for source in sources if source is not None)
    if not text:
        return None

    # Prefer patterns where rating appears before Ratings/Reviews.
    pref = re.search(
        r"\b(\d(?:\.\d)?)\s+\d[\d,]*\s*ratings?\b",
        text,
        flags=re.IGNORECASE,
    )
    if pref:
        return parse_rating(pref.group(1))

    generic = re.search(r"\b(\d(?:\.\d)?)\s*(?:ratings?|reviews?)\b", text, flags=re.IGNORECASE)
    if generic:
        return parse_rating(generic.group(1))

    return None


def infer_review_count_from_text(*sources: Any) -> int | None:
    text = " ".join(str(source) for source in sources if source is not None)
    if not text:
        return None

    match = re.search(r"\b(\d[\d,]*)\s*reviews?\b", text, flags=re.IGNORECASE)
    if match:
        return parse_review_count(match.group(1))

    return None


def clean_title(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    for prefix in NOISY_PREFIXES:
        text = re.sub(rf"^\s*{re.escape(prefix)}\s*[:|\-]?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(rf"\b{re.escape(prefix)}\b", " ", text, flags=re.IGNORECASE)

    # Strip post-title listing metadata.
    split_patterns = [
        r"\b\d+(?:\.\d+)?\s*\d[\d,]*\s*ratings?\b.*$",
        r"\b\d+(?:\.\d+)?\s*ratings?\b.*$",
        r"\b\d[\d,]*\s*reviews?\b.*$",
        r"\bno\s+cost\s+emi\b.*$",
        r"\bbank\s+offer\b.*$",
    ]
    for pattern in split_patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)

    text = re.sub(r"\s+", " ", text).strip(" ,;|-")
    return text


def extract_slug_from_url(url_value: str | None) -> str:
    if not url_value:
        return ""

    path = urlparse(url_value).path.strip("/")
    if not path:
        return ""

    parts = [part for part in path.split("/") if part]
    if not parts:
        return ""

    if "p" in parts:
        idx = parts.index("p")
        if idx > 0:
            return parts[idx - 1]
    return parts[-1]


def normalize_brand(value: Any, title: str, slug: str) -> str | None:
    raw = str(value).strip() if value is not None else ""
    if not raw:
        token = title.split()[0] if title else ""
        if token:
            raw = token
        elif slug:
            raw = slug.split("-", 1)[0]

    raw = re.sub(r"[^A-Za-z0-9+\- ]", "", raw).strip()
    if not raw:
        return None

    lowered = raw.lower()
    if lowered in BRAND_MAP:
        return BRAND_MAP[lowered]

    return raw.upper() if raw.isupper() else raw.title()


def normalize_category(value: Any, title: str, slug: str, breadcrumbs: Any) -> str | None:
    candidates = [value, title, slug, breadcrumbs]
    text = " ".join(str(item) for item in candidates if item is not None).lower()

    raw = str(value).strip().lower() if value is not None else ""
    if raw:
        for key, normalized in CATEGORY_MAP.items():
            if key in raw:
                return normalized

    for normalized, keywords in CATEGORY_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            return normalized

    return None


def normalize_variant(value: Any, title: str) -> str | None:
    raw = str(value).strip() if value is not None else ""
    if raw:
        return raw

    matches = re.findall(r"\(([^)]{1,120})\)", title)
    for match in matches:
        token = match.strip()
        if token:
            return token
    return None


def extract_memory_specs(*sources: str) -> tuple[str | None, str | None]:
    joined = " ".join(source for source in sources if source)
    if not joined:
        return None, None

    lower = joined.lower()
    normalized = re.sub(r"[-_/]+", " ", lower)

    pair_match = re.search(
        r"\b(\d{1,3})\s*gb\s*(?:/|\s)\s*(\d{2,4})\s*(gb|tb)(?:\s*(ssd|hdd|ufs|rom|emmc))?\b",
        normalized,
    )
    if pair_match:
        ram = f"{pair_match.group(1)} GB"
        storage = f"{pair_match.group(2)} {pair_match.group(3).upper()}"
        if pair_match.group(4):
            kind = pair_match.group(4)
            storage = f"{storage} {'eMMC' if kind == 'emmc' else kind.upper()}"
        return ram, storage

    ram_match = re.search(r"\b(\d{1,3})\s*gb\s*ram\b", normalized)
    ram = f"{ram_match.group(1)} GB" if ram_match else None

    storage_match = re.search(r"\b(\d{2,4})\s*(gb|tb)\s*(ssd|hdd|ufs|rom|emmc)\b", normalized)
    storage = None
    if storage_match:
        size = f"{storage_match.group(1)} {storage_match.group(2).upper()}"
        kind = storage_match.group(3)
        storage = f"{size} {'eMMC' if kind == 'emmc' else kind.upper()}"

    generic_specs: list[tuple[int, str, str | None]] = []
    tokens = re.findall(r"[a-z0-9]+", normalized)
    for idx, token in enumerate(tokens):
        value = ""
        unit = ""
        if re.fullmatch(r"\d{1,4}", token):
            if idx + 1 < len(tokens) and tokens[idx + 1] in {"gb", "tb"}:
                value = token
                unit = tokens[idx + 1].upper()
        else:
            fused = re.fullmatch(r"(\d{1,4})(gb|tb)", token)
            if fused:
                value = fused.group(1)
                unit = fused.group(2).upper()
        if not value:
            continue
        kind = None
        if idx + 2 < len(tokens) and tokens[idx + 2] in {"ssd", "hdd", "ufs", "rom", "emmc"}:
            token_kind = tokens[idx + 2]
            kind = "eMMC" if token_kind == "emmc" else token_kind.upper()
        generic_specs.append((int(value), unit, kind))

    if len(generic_specs) >= 2:
        if ram is None:
            small = min(generic_specs, key=lambda item: item[0] * (1024 if item[1] == "TB" else 1))
            if small[1] == "GB" and small[0] <= 64:
                ram = f"{small[0]} GB"

        if storage is None:
            large = max(generic_specs, key=lambda item: item[0] * (1024 if item[1] == "TB" else 1))
            storage = f"{large[0]} {large[1]}"
            if large[2]:
                storage = f"{storage} {large[2]}"

    if ram is None and len(generic_specs) == 1:
        only = generic_specs[0]
        if re.search(rf"\b{only[0]}\s*{only[1].lower()}\s*ram\b", normalized):
            ram = f"{only[0]} {only[1]}"

    if storage is None and len(generic_specs) == 1:
        only = generic_specs[0]
        if re.search(rf"\b{only[0]}\s*{only[1].lower()}\s*(ssd|hdd|ufs|rom|emmc)\b", normalized):
            storage = f"{only[0]} {only[1]}"
            if only[2]:
                storage = f"{storage} {only[2]}"

    # If storage medium is mentioned globally and storage was detected without medium, attach it.
    if storage and not re.search(r"\b(SSD|HDD|UFS|ROM|eMMC)\b", storage):
        if re.search(r"\bssd\b", normalized):
            storage = f"{storage} SSD"
        elif re.search(r"\bhdd\b", normalized):
            storage = f"{storage} HDD"
        elif re.search(r"\bufs\b", normalized):
            storage = f"{storage} UFS"
        elif re.search(r"\brom\b", normalized):
            storage = f"{storage} ROM"

    return ram, storage


def normalize_ram(value: Any, *sources: str) -> str | None:
    raw = str(value).strip() if value is not None else ""
    if raw:
        match = re.search(r"(\d{1,3})\s*(GB|TB)", raw, re.IGNORECASE)
        if match:
            return f"{match.group(1)} {match.group(2).upper()}"

    ram, _storage = extract_memory_specs(raw, *sources)
    return ram


def normalize_storage(value: Any, *sources: str) -> str | None:
    raw = str(value).strip() if value is not None else ""
    if raw:
        match = re.search(r"(\d{2,4})\s*(GB|TB)(?:\s*(SSD|HDD|UFS|ROM|eMMC))?", raw, re.IGNORECASE)
        if match:
            storage = f"{match.group(1)} {match.group(2).upper()}"
            if match.group(3):
                suffix = match.group(3)
                storage = f"{storage} {'eMMC' if suffix.lower() == 'emmc' else suffix.upper()}"
            return storage

    _ram, storage = extract_memory_specs(raw, *sources)
    return storage


def normalize_processor(value: Any, *sources: str) -> str | None:
    raw = str(value).strip() if value is not None else ""
    text = " ".join(part for part in (raw, *sources) if part)

    patterns = [
        r"\bA\d{1,2}\s*Pro\b",
        r"\bA\d{1,2}\b",
        r"\bM[1-5](?:\s*Pro|\s*Max|\s*Ultra)?\b",
        r"\bIntel\s+Core\s+i[3579]\b",
        r"\bCore\s+i[3579]\b",
        r"\bRyzen\s+\d+\b",
        r"\bSnapdragon\s+[A-Za-z0-9+\-]+\b",
        r"\bDimensity\s+\d+\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return re.sub(r"\s+", " ", match.group(0)).strip()
    return None


def normalize_display(value: Any, *sources: str) -> str | None:
    raw = str(value).strip() if value is not None else ""
    if raw:
        return raw

    text = " ".join(source for source in sources if source)
    inch_match = re.search(r"\b(\d{1,2}(?:\.\d+)?)\s*(?:inch|inches|\")\b", text, flags=re.IGNORECASE)
    if inch_match:
        return f"{inch_match.group(1)} inch"

    cm_match = re.search(r"\b(\d{1,2}(?:\.\d+)?)\s*cm\b", text, flags=re.IGNORECASE)
    if cm_match:
        return f"{cm_match.group(1)} cm"

    return None


def normalize_os(value: Any, *sources: str) -> str | None:
    raw = str(value).strip() if value is not None else ""
    text = " ".join(part for part in (raw, *sources) if part).lower()

    if "macos" in text or "mac os" in text:
        return "macOS"
    if "windows" in text:
        return "Windows"
    if "chrome os" in text or "chromeos" in text:
        return "ChromeOS"
    if "linux" in text or "ubuntu" in text:
        return "Linux"
    if "android" in text:
        return "Android"
    if "ios" in text:
        return "iOS"

    return raw or None


def normalize_model(value: Any, title: str, brand: str | None) -> str | None:
    model = str(value).strip() if value is not None else ""
    if not model:
        model = title
    if not model:
        return None

    for prefix in NOISY_PREFIXES:
        model = re.sub(rf"\b{re.escape(prefix)}\b", " ", model, flags=re.IGNORECASE)

    if brand and model.lower().startswith(brand.lower() + " "):
        model = model[len(brand) :].strip()

    model = re.sub(r"\([^)]*\)", " ", model)
    model = re.sub(r"\b\d{1,4}\s*(?:GB|TB)\b(?:\s*(?:RAM|ROM|SSD|HDD|UFS|eMMC))?", " ", model, flags=re.IGNORECASE)
    model = re.sub(r"\b\d+(?:\.\d+)?\s*(?:inch|inches|cm)\b", " ", model, flags=re.IGNORECASE)
    model = re.sub(r"\b\d+(?:\.\d+)?\s*ratings?\b.*$", "", model, flags=re.IGNORECASE)
    model = re.sub(r"\b\d+[A-Za-z0-9\-]{6,}\b", " ", model)
    model = re.sub(r"\s*/\s*", " ", model)
    model = re.sub(r"\bmacbook\b", "MacBook", model, flags=re.IGNORECASE)
    model = re.sub(r"\s+", " ", model).strip(" ,;|-")

    if model and "http" in model.lower():
        return None

    return model or None


def normalize_availability(value: Any, *sources: str) -> str | None:
    raw = str(value).strip().lower() if value is not None else ""
    text = " ".join(str(source) for source in sources if source is not None).lower()
    merged = f"{raw} {text}".strip()

    if any(token in merged for token in ("currently unavailable", "unavailable")):
        return "unavailable"
    if any(token in merged for token in ("out of stock", "sold out")):
        return "out_of_stock"
    if any(token in merged for token in ("pre order", "pre-order", "coming soon", "preorder")):
        return "preorder"
    if any(token in merged for token in ("in stock", "available now")):
        return "in_stock"

    if raw in VALID_AVAILABILITY:
        return raw

    return None


def has_memory_signal(text: str) -> bool:
    lower = text.lower()
    if re.search(r"\b\d{1,4}\s*(gb|tb)\s*(ram|rom|ssd|hdd|ufs|emmc)?\b", lower):
        return True
    return False


def is_realistic_price(category: str | None, price_inr: int | None) -> bool:
    if price_inr is None:
        return False

    if category == "laptop":
        return 10000 <= price_inr <= 1000000
    if category == "smartphone":
        return 2000 <= price_inr <= 300000
    if category == "tablet":
        return 3000 <= price_inr <= 300000

    return 100 <= price_inr <= 10000000


def validate_normalized_record(record: dict[str, Any], source_row: dict[str, Any]) -> bool:
    brand = record.get("brand")
    model = record.get("model")
    product_url = record.get("product_url")
    category = record.get("category")
    price_inr = record.get("price_inr")

    if is_placeholder_value(brand):
        return False

    if is_placeholder_value(model):
        return False

    model_text = str(model)
    if any(token in model_text.lower() for token in ("http://", "https://", "pid=", "lid=")):
        return False

    if not is_valid_canonical_product_url(product_url):
        return False

    if not is_realistic_price(category, price_inr):
        return False

    source_blob = " ".join(
        str(source_row.get(key, ""))
        for key in (
            "title",
            "description",
            "specs",
            "details",
            "breadcrumbs",
            "price",
            "product_url",
            "url",
            "link",
        )
    )

    if has_memory_signal(source_blob):
        if record.get("ram") is None and re.search(r"\bram\b", source_blob, flags=re.IGNORECASE):
            return False
        if record.get("storage") is None and re.search(
            r"\b(ssd|hdd|rom|ufs|emmc|storage)\b", source_blob, flags=re.IGNORECASE
        ):
            return False

    return True


def normalize_record(row: dict[str, Any]) -> dict[str, Any]:
    title = clean_title(row.get("title"))
    details_text = " ".join(
        str(row.get(key, "")) for key in ("description", "specs", "details", "subtitle")
    )

    url_value = row.get("product_url") or row.get("url") or row.get("link")
    product_url = canonical_product_url(url_value)
    slug = extract_slug_from_url(product_url)

    brand = normalize_brand(row.get("brand"), title, slug)
    category = normalize_category(row.get("category"), title, slug, row.get("breadcrumbs"))
    variant = normalize_variant(row.get("variant"), title)
    processor = normalize_processor(row.get("processor"), title, details_text, slug)
    ram = normalize_ram(row.get("ram"), title, details_text, slug)
    storage = normalize_storage(row.get("storage"), title, details_text, slug)
    display = normalize_display(row.get("display"), title, details_text)
    os_name = normalize_os(row.get("os"), title, details_text)
    model = normalize_model(row.get("model"), title, brand)
    availability = normalize_availability(row.get("availability"), row.get("title"), details_text)

    price_value = row.get("price_inr") if row.get("price_inr") is not None else row.get("price")
    price_inr = parse_price_inr(price_value)

    rating = parse_rating(row.get("rating")) or infer_rating_from_text(
        row.get("title"),
        details_text,
        row.get("rating_text"),
    )
    review_count = parse_review_count(row.get("review_count")) or infer_review_count_from_text(
        row.get("title"),
        details_text,
        row.get("review_text"),
    )

    normalized = {
        "brand": brand,
        "category": category,
        "model": model,
        "variant": variant,
        "ram": ram,
        "storage": storage,
        "processor": processor,
        "display": display,
        "os": os_name,
        "price_inr": price_inr,
        "rating": rating,
        "review_count": review_count,
        "availability": availability,
        "product_url": product_url,
    }

    return replace_empty_with_none(normalized)


def record_completeness_score(record: dict[str, Any]) -> int:
    return sum(1 for key in OUTPUT_FIELDS if record.get(key) is not None)


def dedupe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}

    for row in records:
        key = (
            str(row.get("brand") or "").strip().lower(),
            str(row.get("model") or "").strip().lower(),
            str(row.get("ram") or "").strip().lower(),
            str(row.get("storage") or "").strip().lower(),
            str(row.get("processor") or "").strip().lower(),
        )

        existing = deduped.get(key)
        if existing is None:
            deduped[key] = row
            continue

        if record_completeness_score(row) > record_completeness_score(existing):
            deduped[key] = row

    return list(deduped.values())


def clean_products(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_records = [normalize_record(row) for row in records]
    validated_records = [
        row
        for row, source in zip(normalized_records, records, strict=False)
        if validate_normalized_record(row, source)
    ]
    deduped_records = dedupe_records(validated_records)
    return [{field: row.get(field) for field in OUTPUT_FIELDS} for row in deduped_records]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Normalize and validate product JSON records into training-ready schema."
    )
    parser.add_argument("input_json", nargs="?", default="products.json")
    parser.add_argument("--output", default="clean_products.json")
    args = parser.parse_args()

    input_path = Path(args.input_json).resolve()
    output_path = Path(args.output).resolve()

    records = load_records(input_path)
    validate_schema_consistency(records)
    cleaned = clean_products(records)

    output_path.write_text(json.dumps(cleaned, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
