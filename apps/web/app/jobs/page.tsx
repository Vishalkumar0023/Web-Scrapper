"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import { cancelJob, listJobs, retryJob, type JobSummary } from "../../lib/api";
import { formatDateTime, isActiveStatus, statusClassName } from "../../lib/jobs";
import { getSession } from "../../lib/session";

const STATUS_OPTIONS = [
  "",
  "queued",
  "retrying",
  "extraction_running",
  "pagination_running",
  "success",
  "partial_success",
  "failed",
  "cancelled",
];

export default function JobsPage() {
  const [projectId, setProjectId] = useState("");
  const [hasSession, setHasSession] = useState<boolean | null>(null);
  const [statusFilter, setStatusFilter] = useState("");
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [totalJobs, setTotalJobs] = useState(0);

  const hasActiveJobs = useMemo(() => jobs.some((job) => isActiveStatus(job.status)), [jobs]);

  useEffect(() => {
    const session = getSession();
    if (session?.user.default_project_id) {
      setProjectId(session.user.default_project_id);
      setHasSession(true);
      return;
    }
    setHasSession(false);
  }, []);

  const loadJobs = useCallback(
    async (showLoading: boolean) => {
      if (hasSession === false) {
        setJobs([]);
        setTotalJobs(0);
        setLoading(false);
        setRefreshing(false);
        setError(null);
        return;
      }

      if (showLoading) {
        setLoading(true);
      } else {
        setRefreshing(true);
      }

      try {
        const response = await listJobs({
          project_id: projectId || undefined,
          status: statusFilter || undefined,
          offset: 0,
          limit: 100,
        });
        setJobs(response.jobs);
        setTotalJobs(response.total_jobs);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load jobs");
      } finally {
        setLoading(false);
        setRefreshing(false);
      }
    },
    [hasSession, projectId, statusFilter],
  );

  useEffect(() => {
    if (hasSession === null) {
      return;
    }
    void loadJobs(true);
  }, [hasSession, loadJobs]);

  useEffect(() => {
    if (hasSession !== true) {
      return;
    }
    const intervalMs = hasActiveJobs ? 2500 : 5000;
    const timer = window.setInterval(() => {
      void loadJobs(false);
    }, intervalMs);
    return () => window.clearInterval(timer);
  }, [hasActiveJobs, hasSession, loadJobs]);

  async function onCancel(jobId: string) {
    try {
      await cancelJob(jobId);
      await loadJobs(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to cancel job");
    }
  }

  async function onRetry(jobId: string) {
    try {
      await retryJob(jobId);
      await loadJobs(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to retry job");
    }
  }

  return (
    <main>
      <section className="page-header">
        <h1>Job History</h1>
        <p className="page-subtitle">
          Track queued and completed jobs with live polling. Open detail pages for per-job status logs and exports.
        </p>
      </section>

      {hasSession === false ? (
        <p className="text-danger">
          Login is required for user-scoped history. Open <Link href="/auth">Auth</Link> first.
        </p>
      ) : null}

      <section className="card">
        <div className="card-title-row">
          <h2>Filters</h2>
          <div className="meta-row">
            <span className="meta-chip">Total: {totalJobs}</span>
            {refreshing ? <span className="meta-chip">Refreshing...</span> : null}
          </div>
        </div>
        <div className="form-grid-columns">
          <label>
            Project ID
            <input
              value={projectId}
              onChange={(event) => setProjectId(event.target.value)}
              placeholder="proj_local"
            />
          </label>
          <label>
            Status
            <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
              {STATUS_OPTIONS.map((status) => (
                <option key={status || "all"} value={status}>
                  {status || "all"}
                </option>
              ))}
            </select>
          </label>
        </div>
      </section>

      {error ? <p className="text-danger">{error}</p> : null}

      <section className="card" style={{ marginTop: "1rem" }}>
        <div className="card-title-row">
          <h2>Jobs</h2>
          <button
            type="button"
            className="button-secondary"
            onClick={() => {
              void loadJobs(false);
            }}
          >
            Refresh
          </button>
        </div>
        {loading ? (
          <p>Loading jobs...</p>
        ) : jobs.length === 0 ? (
          <p className="empty-state">No jobs found for current filters.</p>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Job</th>
                  <th>Project</th>
                  <th>Mode</th>
                  <th>Status</th>
                  <th>Progress</th>
                  <th>Created</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <tr key={job.job_id}>
                    <td>
                      <div className="mono">{job.job_id}</div>
                      <Link href={`/jobs/${encodeURIComponent(job.job_id)}`}>Open detail</Link>
                    </td>
                    <td>{job.project_id}</td>
                    <td>{job.mode}</td>
                    <td>
                      <span className={statusClassName(job.status)}>{job.status}</span>
                    </td>
                    <td>
                      {job.progress.pages_processed} pages / {job.progress.rows_extracted} rows
                    </td>
                    <td>{formatDateTime(job.created_at)}</td>
                    <td>
                      <div className="button-row">
                        {isActiveStatus(job.status) ? (
                          <button
                            type="button"
                            className="button-danger"
                            onClick={() => {
                              void onCancel(job.job_id);
                            }}
                          >
                            Cancel
                          </button>
                        ) : null}
                        {(job.status === "failed" || job.status === "cancelled") ? (
                          <button
                            type="button"
                            className="button-secondary"
                            onClick={() => {
                              void onRetry(job.job_id);
                            }}
                          >
                            Retry
                          </button>
                        ) : null}
                      </div>
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
