"use client";

import { useEffect, useState } from "react";

import { listTemplates, type TemplateRecord } from "../../lib/api";
import { formatDateTime } from "../../lib/jobs";

const PAGE_TYPES = ["", "listing", "detail", "unknown"];

function toPercent(value: number): string {
  return `${Math.round(value * 100)}%`;
}

export default function TemplatesPage() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [templates, setTemplates] = useState<TemplateRecord[]>([]);
  const [domain, setDomain] = useState("");
  const [pageType, setPageType] = useState("");

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    void listTemplates({
      domain: domain || undefined,
      page_type: pageType || undefined,
    })
      .then((response) => {
        if (cancelled) {
          return;
        }
        setTemplates(response.templates);
        setError(null);
      })
      .catch((err) => {
        if (cancelled) {
          return;
        }
        setError(err instanceof Error ? err.message : "Failed to load templates");
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [domain, pageType]);

  return (
    <main>
      <section className="page-header">
        <h1>Template Library</h1>
        <p className="page-subtitle">
          Browse reusable extraction templates with success-rate health, versions, and invalidation signals.
        </p>
      </section>

      <section className="card">
        <div className="card-title-row">
          <h2>Filters</h2>
          <span className="meta-chip">Templates: {templates.length}</span>
        </div>
        <div className="form-grid-columns">
          <label>
            Domain
            <input
              value={domain}
              onChange={(event) => setDomain(event.target.value)}
              placeholder="example.com"
            />
          </label>
          <label>
            Page Type
            <select value={pageType} onChange={(event) => setPageType(event.target.value)}>
              {PAGE_TYPES.map((type) => (
                <option key={type || "all"} value={type}>
                  {type || "all"}
                </option>
              ))}
            </select>
          </label>
        </div>
      </section>

      {error ? <p className="text-danger">{error}</p> : null}

      <section className="card" style={{ marginTop: "1rem" }}>
        <h2>Templates</h2>
        {loading ? (
          <p>Loading templates...</p>
        ) : templates.length === 0 ? (
          <p className="empty-state">No templates found.</p>
        ) : (
          <div className="grid-cards">
            {templates.map((template) => (
              <article key={template.template_id} className="card card-muted">
                <div className="card-title-row">
                  <h3 className="mono">{template.template_id}</h3>
                  <span className={template.invalidated ? "status-chip status-failed" : "status-chip status-success"}>
                    {template.invalidated ? "invalidated" : "active"}
                  </span>
                </div>

                <div className="meta-row">
                  <span className="meta-chip">Domain: {template.domain}</span>
                  <span className="meta-chip">Page: {template.page_type}</span>
                  <span className="meta-chip">v{template.version}</span>
                </div>

                <p className="muted">Success Rate: {toPercent(template.success_rate)}</p>
                <p className="muted">
                  Success/Failure: {template.success_count}/{template.failure_count}
                </p>
                <p className="muted">Updated: {formatDateTime(template.updated_at)}</p>

                {template.invalidation_reason ? (
                  <p className="text-danger">Invalidation Reason: {template.invalidation_reason}</p>
                ) : null}

                {template.page_fingerprint ? <p className="mono">Fingerprint: {template.page_fingerprint}</p> : null}
              </article>
            ))}
          </div>
        )}
      </section>
    </main>
  );
}
