"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";

import {
  cancelJob,
  createExport,
  getJobDetail,
  getJobInsights,
  getJobRows,
  getJobStatus,
  listExports,
  retryJob,
  type ExportRecord,
  type JobDetail,
  type JobInsightsResponse,
  type JobStatusResponse,
} from "../../../lib/api";
import { formatDateTime, isActiveStatus, isTerminalStatus, statusClassName } from "../../../lib/jobs";

type StatusLog = {
  at: string;
  status: string;
  note: string;
};

export default function JobDetailPage() {
  const params = useParams<{ jobId: string }>();
  const jobId = decodeURIComponent(params.jobId);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [detail, setDetail] = useState<JobDetail | null>(null);
  const [status, setStatus] = useState<JobStatusResponse | null>(null);
  const [rows, setRows] = useState<Array<Record<string, unknown>>>([]);
  const [totalRows, setTotalRows] = useState(0);
  const [exports, setExports] = useState<ExportRecord[]>([]);
  const [insights, setInsights] = useState<JobInsightsResponse | null>(null);
  const [logs, setLogs] = useState<StatusLog[]>([]);
  const [busyAction, setBusyAction] = useState<string | null>(null);

  const rowColumns = useMemo(() => {
    const first = rows[0];
    if (!first) {
      return [];
    }
    return Object.keys(first);
  }, [rows]);

  const addLog = useCallback((entry: StatusLog) => {
    setLogs((current) => [entry, ...current].slice(0, 50));
  }, []);

  const loadAll = useCallback(
    async (initial: boolean) => {
      if (initial) {
        setLoading(true);
      }
      try {
        const [detailResponse, statusResponse, rowsResponse, exportsResponse, insightsResponse] = await Promise.all([
          getJobDetail(jobId),
          getJobStatus(jobId),
          getJobRows(jobId, 0, 100),
          listExports({ job_id: jobId, offset: 0, limit: 50 }),
          getJobInsights(jobId, 50).catch(() => null),
        ]);

        setDetail(detailResponse);
        setStatus((current) => {
          if (!current || current.status !== statusResponse.status) {
            addLog({
              at: new Date().toISOString(),
              status: statusResponse.status,
              note: `Status changed to ${statusResponse.status}`,
            });
          }
          return statusResponse;
        });
        setRows(rowsResponse.rows);
        setTotalRows(rowsResponse.total_rows);
        setExports(exportsResponse.exports);
        setInsights(insightsResponse);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load job details");
      } finally {
        setLoading(false);
      }
    },
    [addLog, jobId],
  );

  useEffect(() => {
    void loadAll(true);
  }, [loadAll]);

  useEffect(() => {
    if (!status || isTerminalStatus(status.status)) {
      return;
    }
    const timer = window.setInterval(() => {
      void loadAll(false);
    }, 2000);
    return () => window.clearInterval(timer);
  }, [loadAll, status]);

  async function onCancel() {
    setBusyAction("cancel");
    try {
      await cancelJob(jobId);
      addLog({
        at: new Date().toISOString(),
        status: "cancelled",
        note: "Cancel requested from detail page",
      });
      await loadAll(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Cancel failed");
    } finally {
      setBusyAction(null);
    }
  }

  async function onRetry() {
    setBusyAction("retry");
    try {
      await retryJob(jobId);
      addLog({
        at: new Date().toISOString(),
        status: "queued",
        note: "Retry requested from detail page",
      });
      await loadAll(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Retry failed");
    } finally {
      setBusyAction(null);
    }
  }

  async function onExport(format: "csv" | "json") {
    setBusyAction(format);
    try {
      await createExport(jobId, { format, selected_columns: detail?.fields.map((field) => field.name) ?? [] });
      addLog({
        at: new Date().toISOString(),
        status: status?.status ?? "export_ready",
        note: `${format.toUpperCase()} export generated`,
      });
      await loadAll(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Export failed");
    } finally {
      setBusyAction(null);
    }
  }

  return (
    <main>
      <section className="page-header">
        <h1>Job Detail</h1>
        <p className="page-subtitle">
          Inspect job status transitions, extraction output rows, warnings, and export artifacts.
        </p>
      </section>

      <section className="card" style={{ marginTop: "1rem" }}>
        <div className="card-title-row">
          <h2>AI Insights</h2>
          {insights?.used_ai ? <span className="meta-chip">AI used</span> : <span className="meta-chip">Fallback</span>}
        </div>
        {insights ? (
          <div className="stack">
            <p>{insights.summary}</p>
            {Object.keys(insights.label_counts).length > 0 ? (
              <div className="meta-row">
                {Object.entries(insights.label_counts).map(([label, count]) => (
                  <span key={label} className="meta-chip">
                    {label}: {count}
                  </span>
                ))}
              </div>
            ) : (
              <p className="empty-state">No classifications returned.</p>
            )}
          </div>
        ) : (
          <p className="empty-state">Insights unavailable for this job.</p>
        )}
      </section>

      <section className="card">
        {loading ? (
          <p>Loading job {jobId}...</p>
        ) : detail && status ? (
          <div className="stack">
            <div className="card-title-row">
              <h2 className="mono">{detail.job_id}</h2>
              <span className={statusClassName(status.status)}>{status.status}</span>
            </div>
            <div className="meta-row">
              <span className="meta-chip">Project: {detail.project_id}</span>
              <span className="meta-chip">Mode: {detail.mode}</span>
              <span className="meta-chip">Created: {formatDateTime(detail.created_at)}</span>
              <span className="meta-chip">Page Type: {detail.page_type}</span>
            </div>
            <p className="mono">{detail.input_url}</p>
            <p>
              Progress: {status.progress.pages_processed} pages / {status.progress.rows_extracted} rows
            </p>

            <div className="button-row">
              {isActiveStatus(status.status) ? (
                <button type="button" className="button-danger" disabled={busyAction !== null} onClick={onCancel}>
                  Cancel Job
                </button>
              ) : null}
              {(status.status === "failed" || status.status === "cancelled") ? (
                <button type="button" className="button-secondary" disabled={busyAction !== null} onClick={onRetry}>
                  Retry Job
                </button>
              ) : null}
              <button
                type="button"
                className="button-secondary"
                disabled={busyAction !== null}
                onClick={() => {
                  void onExport("csv");
                }}
              >
                Export CSV
              </button>
              <button
                type="button"
                className="button-secondary"
                disabled={busyAction !== null}
                onClick={() => {
                  void onExport("json");
                }}
              >
                Export JSON
              </button>
            </div>

            {detail.warnings.length > 0 ? (
              <>
                <hr className="section-divider" />
                <h3>Warnings</h3>
                <ul className="warning-list">
                  {detail.warnings.map((warning) => (
                    <li key={warning}>{warning}</li>
                  ))}
                </ul>
              </>
            ) : null}
          </div>
        ) : (
          <p className="empty-state">No job details available.</p>
        )}
      </section>

      {error ? <p className="text-danger">{error}</p> : null}

      <section className="grid-two" style={{ marginTop: "1rem" }}>
        <article className="card">
          <div className="card-title-row">
            <h2>Status Logs</h2>
            <button
              type="button"
              className="button-secondary"
              onClick={() => {
                void loadAll(false);
              }}
            >
              Refresh
            </button>
          </div>
          {logs.length === 0 ? (
            <p className="empty-state">No status transitions recorded yet.</p>
          ) : (
            <ul className="timeline">
              {logs.map((log, index) => (
                <li key={`${log.at}-${index}`}>
                  <div>
                    <span className={statusClassName(log.status)}>{log.status}</span>
                  </div>
                  <div className="muted">{formatDateTime(log.at)}</div>
                  <div>{log.note}</div>
                </li>
              ))}
            </ul>
          )}
        </article>

        <article className="card">
          <h2>Fields</h2>
          {!detail || detail.fields.length === 0 ? (
            <p className="empty-state">No fields available.</p>
          ) : (
            <div className="stack">
              {detail.fields.map((field) => (
                <div key={field.name} className="confidence-row">
                  <div>{field.name}</div>
                  <div className="muted">{field.kind}</div>
                  <div title={`${Math.round(field.confidence * 100)}%`}>
                    <div className="confidence-track">
                      <div className="confidence-bar" style={{ width: `${Math.round(field.confidence * 100)}%` }} />
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </article>
      </section>

      <section className="card" style={{ marginTop: "1rem" }}>
        <h2>Rows ({totalRows})</h2>
        {rows.length === 0 ? (
          <p className="empty-state">No rows extracted yet.</p>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  {rowColumns.map((column) => (
                    <th key={column}>{column}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((row, index) => (
                  <tr key={index}>
                    {rowColumns.map((column) => (
                      <td key={`${index}-${column}`}>{String(row[column] ?? "")}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="card" style={{ marginTop: "1rem" }}>
        <div className="card-title-row">
          <h2>Exports</h2>
          <Link href={`/file-manager?job_id=${encodeURIComponent(jobId)}`}>Open File Manager</Link>
        </div>
        {exports.length === 0 ? (
          <p className="empty-state">No exports for this job yet.</p>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Export</th>
                  <th>Format</th>
                  <th>Status</th>
                  <th>Created</th>
                  <th>Download</th>
                </tr>
              </thead>
              <tbody>
                {exports.map((item) => (
                  <tr key={item.export_id}>
                    <td className="mono">{item.export_id}</td>
                    <td>{item.format}</td>
                    <td>{item.status}</td>
                    <td>{formatDateTime(item.created_at)}</td>
                    <td>
                      <a href={item.file_url} target="_blank" rel="noreferrer">
                        Download
                      </a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  );
}
