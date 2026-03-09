from app.models import TemplateRecord
from app.services.template_engine import (
    apply_template_extract_rows,
    compute_page_fingerprint,
    fields_from_template,
    find_next_page_url_with_template,
    normalize_domain,
)


HTML = """
<html>
  <body>
    <div class="items">
      <article class="card">
        <a href="/p/1"><h2>Alpha</h2></a>
        <span class="price">$11.00</span>
      </article>
      <article class="card">
        <a href="/p/2"><h2>Beta</h2></a>
        <span class="price">$12.00</span>
      </article>
    </div>
    <a class="next" href="/search?page=2">Next</a>
  </body>
</html>
"""


def test_normalize_domain() -> None:
    assert normalize_domain("https://www.Example.com/path") == "example.com"


def test_compute_page_fingerprint_is_stable() -> None:
    fp1 = compute_page_fingerprint(HTML)
    fp2 = compute_page_fingerprint(HTML)
    assert fp1 == fp2
    assert len(fp1) == 40


def test_apply_template_extract_rows_and_pagination() -> None:
    template = TemplateRecord(
        template_id="tpl_1",
        domain="example.com",
        page_type="listing",
        template={
            "container_selector": "article.card",
            "fields": {
                "title": "h2",
                "price": ".price",
                "product_url": "a@href",
            },
            "pagination": {
                "type": "next_button",
                "selector": "a.next",
            },
        },
    )

    rows, warnings = apply_template_extract_rows(
        html=HTML,
        base_url="https://example.com/search?page=1",
        template=template,
        max_rows=10,
    )
    assert warnings == []
    assert len(rows) == 2
    assert rows[0]["product_url"] == "https://example.com/p/1"

    next_url, pagination_warnings = find_next_page_url_with_template(
        html=HTML,
        base_url="https://example.com/search?page=1",
        template=template,
    )
    assert next_url == "https://example.com/search?page=2"
    assert any("template_pagination" in warning for warning in pagination_warnings)

    fields = fields_from_template(template)
    assert len(fields) == 3
    assert fields[0].name == "title"
