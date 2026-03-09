"use client";

import Link from "next/link";
import { FormEvent, useEffect, useMemo, useState } from "react";

import {
  getJobStatus,
  runPreview,
  runScrape,
  type JobStatusResponse,
  type PreviewResponse,
} from "../../lib/api";
import { formatDateTime, isTerminalStatus, statusClassName } from "../../lib/jobs";
import { getSession } from "../../lib/session";

export default function NewScrapePage() {
  const [projectId, setProjectId] = useState("proj_local");
  const [defaultProjectId, setDefaultProjectId] = useState<string | null>(null);
  const [canEditProjectId, setCanEditProjectId] = useState(false);
  const [hasSession, setHasSession] = useState<boolean | null>(null);
  const [url, setUrl] = useState("https://example.com/search?q=shoes");
  const [prompt, setPrompt] = useState(
    "Extract brand, category, product_family, model, parent_product_id, variant_id, canonical_product_id, cluster_id, cluster_confidence, global_entity_id, match_confidence, sku, sku_confidence, ram, storage, processor, display, os_family, os_version, os, is_canonical_name, name_source, price_inr, rating, review_count, review_scope, review_count_timestamp, availability, product_url",
  );
  const [maxRows, setMaxRows] = useState(20);
  const [maxPages, setMaxPages] = useState(5);
  const [loading, setLoading] = useState(false);
  const [runLoading, setRunLoading] = useState(false);
  const [result, setResult] = useState<PreviewResponse | null>(null);
  const [activeRun, setActiveRun] = useState<JobStatusResponse | null>(null);
  const [activeRunStartedAt, setActiveRunStartedAt] = useState<string | null>(null);
  const [editableFields, setEditableFields] = useState<Array<{ name: string; kind: string; confidence: number }>>([]);
  const [error, setError] = useState<string | null>(null);

  const synthesizedPrompt = useMemo(() => {
    if (editableFields.length === 0) {
      return prompt;
    }
    const edited = editableFields.map((field) => `${field.name} (${field.kind})`).join(", ");
    return `${prompt}. Preferred fields: ${edited}.`;
  }, [editableFields, prompt]);

  useEffect(() => {
    const session = getSession();
    if (session?.user.default_project_id) {
      const assignedProjectId = session.user.default_project_id;
      const hasWildcardProject = session.user.project_ids.includes("*");
      const hasManyProjects = session.user.project_ids.length > 1;
      const editable = hasWildcardProject || hasManyProjects;

      setDefaultProjectId(assignedProjectId);
      setCanEditProjectId(editable);
      setProjectId(assignedProjectId);
      setHasSession(true);
      return;
    }
    setDefaultProjectId(null);
    setCanEditProjectId(false);
    setHasSession(false);
  }, []);

  useEffect(() => {
    if (!activeRun || isTerminalStatus(activeRun.status)) {
      return;
    }

    const timer = window.setInterval(async () => {
      try {
        const latest = await getJobStatus(activeRun.job_id);
        setActiveRun(latest);
      } catch {
        // keep last state and retry on next interval
      }
    }, 2000);

    return () => window.clearInterval(timer);
  }, [activeRun]);

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoading(true);
    setError(null);
    const effectiveProjectId = canEditProjectId ? projectId : defaultProjectId || projectId;

    try {
      const data = await runPreview({
        project_id: effectiveProjectId,
        url,
        prompt: synthesizedPrompt,
        max_rows: maxRows,
      });
      setResult(data);
      setEditableFields(data.fields.map((field) => ({ ...field })));
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unexpected error";
      if (message.includes("No access to project") && defaultProjectId) {
        setProjectId(defaultProjectId);
        setError(`Project access mismatch. Reset to your assigned project: ${defaultProjectId}`);
      } else {
        setError(message);
      }
    } finally {
      setLoading(false);
    }
  }

  async function onRunFull() {
    setRunLoading(true);
    setError(null);
    const effectiveProjectId = canEditProjectId ? projectId : defaultProjectId || projectId;
    try {
      const data = await runScrape({
        project_id: effectiveProjectId,
        url,
        prompt: synthesizedPrompt,
        max_pages: maxPages,
        max_rows: Math.max(maxRows, 100),
      });
      setActiveRun({
        job_id: data.job_id,
        status: data.status,
        progress: { pages_processed: 0, rows_extracted: 0 },
        error: null,
      });
      setActiveRunStartedAt(new Date().toISOString());
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unexpected error";
      if (message.includes("No access to project") && defaultProjectId) {
        setProjectId(defaultProjectId);
        setError(`Project access mismatch. Reset to your assigned project: ${defaultProjectId}`);
      } else {
        setError(message);
      }
    } finally {
      setRunLoading(false);
    }
  }

  function updateField(index: number, key: "name" | "kind", value: string) {
    setEditableFields((current) =>
      current.map((field, fieldIndex) => (fieldIndex === index ? { ...field, [key]: value } : field)),
    );
  }

  function addFieldRow() {
    setEditableFields((current) => [...current, { name: "new_field", kind: "text", confidence: 0.5 }]);
  }

  return (
    <main>
      <section className="page-header">
        <h1>New Scrape</h1>
        <p className="page-subtitle">
          Preview extraction, tune field labels with confidence, then launch a full run with live status polling.
        </p>
      </section>

      <section className="grid-two">
        <form className="card form-grid" onSubmit={onSubmit}>
          <div className="card-title-row">
            <h2>Request</h2>
            <span className="meta-chip">Preview + Full Run</span>
          </div>

          {hasSession === false ? (
            <p className="text-danger">
              Login is required for user-scoped file management. Open <Link href="/auth">Auth</Link> first.
            </p>
          ) : null}

          <div className="form-grid-columns">
            <label>
              Project ID
              <input
                value={projectId}
                onChange={(event) => setProjectId(event.target.value)}
                readOnly={!canEditProjectId}
                title={canEditProjectId ? "Project ID" : "Locked to your assigned project"}
              />
              {!canEditProjectId && defaultProjectId ? (
                <small className="muted">Locked to your assigned project for this account.</small>
              ) : null}
            </label>
            <label>
              Max Preview Rows
              <input
                type="number"
                min={1}
                max={100}
                value={maxRows}
                onChange={(event) => setMaxRows(Number(event.target.value) || 1)}
              />
            </label>
          </div>

          <label>
            URL
            <input
              type="url"
              required
              value={url}
              onChange={(event) => setUrl(event.target.value)}
              placeholder="https://example.com"
            />
          </label>

          <label>
            Prompt
            <textarea
              rows={4}
              value={prompt}
              onChange={(event) => setPrompt(event.target.value)}
              placeholder="Extract brand, category, product_family, model, parent_product_id, variant_id, canonical_product_id, cluster_id, cluster_confidence, global_entity_id, match_confidence, sku, sku_confidence, ram, storage, processor, display, os_family, os_version, os, is_canonical_name, name_source, price_inr, rating, review_count, review_scope, review_count_timestamp, availability, product_url"
            />
          </label>

          <label>
            Max Full-Run Pages
            <input
              type="number"
              min={1}
              max={100}
              value={maxPages}
              onChange={(event) => setMaxPages(Number(event.target.value) || 1)}
            />
          </label>

          <div className="button-row">
            <button type="submit" disabled={loading || hasSession === false}>
              {loading ? "Running preview..." : "Run Preview"}
            </button>
            <button
              type="button"
              className="button-secondary"
              disabled={runLoading || hasSession === false}
              onClick={() => {
                void onRunFull();
              }}
            >
              {runLoading ? "Queueing..." : "Start Full Run"}
            </button>
          </div>
        </form>

        <section className="stack">
          <article className="card">
            <h2>Run Monitor</h2>
            {activeRun ? (
              <div className="stack">
                <p>
                  Job <span className="mono">{activeRun.job_id}</span>
                </p>
                <p>
                  Status <span className={statusClassName(activeRun.status)}>{activeRun.status}</span>
                </p>
                <p>
                  Progress: {activeRun.progress.pages_processed} pages, {activeRun.progress.rows_extracted} rows
                </p>
                {activeRunStartedAt ? <p className="muted">Started at {formatDateTime(activeRunStartedAt)}</p> : null}
                <div className="button-row">
                  <Link href={`/jobs/${encodeURIComponent(activeRun.job_id)}`}>Open Job Detail</Link>
                </div>
              </div>
            ) : (
              <p className="empty-state">No active run yet.</p>
            )}
          </article>

          <article className="card card-muted">
            <h2>Effective Prompt</h2>
            <p className="mono">{synthesizedPrompt}</p>
          </article>
        </section>
      </section>

      {error ? <p className="text-danger">{error}</p> : null}

      {result ? (
        <section className="card" style={{ marginTop: "1rem" }}>
          <div className="card-title-row">
            <h2>Preview Result</h2>
            <span className={statusClassName(result.status)}>{result.status}</span>
          </div>

          <div className="meta-row">
            <span className="meta-chip">Job: {result.job_id}</span>
            <span className="meta-chip">Page Type: {result.page_type}</span>
            <span className="meta-chip">Rows: {result.rows.length}</span>
          </div>

          {result.warnings.length > 0 ? (
            <>
              <hr className="section-divider" />
              <h3>Warnings</h3>
              <ul className="warning-list">
                {result.warnings.map((warning) => (
                  <li key={warning}>{warning}</li>
                ))}
              </ul>
            </>
          ) : null}

          <hr className="section-divider" />
          <div className="card-title-row">
            <h3>Field Editor</h3>
            <button type="button" className="button-secondary" onClick={addFieldRow}>
              Add Field
            </button>
          </div>

          <div className="stack">
            {editableFields.map((field, index) => (
              <div key={`${field.name}-${index}`} className="confidence-row">
                <input
                  value={field.name}
                  onChange={(event) => updateField(index, "name", event.target.value)}
                  aria-label={`Field name ${index + 1}`}
                />
                <select
                  value={field.kind}
                  onChange={(event) => updateField(index, "kind", event.target.value)}
                  aria-label={`Field kind ${index + 1}`}
                >
                  {["text", "money", "number", "rating", "url", "date"].map((kind) => (
                    <option key={kind} value={kind}>
                      {kind}
                    </option>
                  ))}
                </select>
                <div title={`confidence ${Math.round(field.confidence * 100)}%`}>
                  <div className="confidence-track">
                    <div className="confidence-bar" style={{ width: `${Math.round(field.confidence * 100)}%` }} />
                  </div>
                </div>
              </div>
            ))}
          </div>

          <hr className="section-divider" />
          <h3>Row Sample</h3>
          <div className="table-wrap">
            <pre style={{ margin: 0, padding: "0.75rem" }}>{JSON.stringify(result.rows.slice(0, 8), null, 2)}</pre>
          </div>
        </section>
      ) : null}
    </main>
  );
}
