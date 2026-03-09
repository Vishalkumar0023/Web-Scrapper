import { getSession, type SessionState, type SessionUser } from "./session";

export type PreviewRequest = {
  project_id: string;
  url: string;
  prompt?: string;
  max_rows?: number;
};

export type PreviewResponse = {
  job_id: string;
  status: string;
  page_type: string;
  fields: Array<{ name: string; kind: string; confidence: number }>;
  rows: Array<Record<string, unknown>>;
  warnings: string[];
};

export type RunRequest = {
  project_id: string;
  url: string;
  prompt?: string;
  max_pages?: number;
  max_rows?: number;
  template_id?: string;
};

export type RunResponse = {
  job_id: string;
  status: string;
};

export type JobProgress = {
  pages_processed: number;
  rows_extracted: number;
};

export type JobStatusResponse = {
  job_id: string;
  status: string;
  progress: JobProgress;
  error: { code: string; message: string } | null;
};

export type JobSummary = {
  job_id: string;
  project_id: string;
  mode: string;
  status: string;
  input_url: string;
  page_type: string;
  progress: JobProgress;
  created_at: string;
};

export type JobsListResponse = {
  total_jobs: number;
  jobs: JobSummary[];
};

export type JobDetail = {
  job_id: string;
  project_id: string;
  mode: string;
  status: string;
  input_url: string;
  prompt: string | null;
  max_pages: number;
  max_rows: number;
  page_type: string;
  fields: Array<{ name: string; kind: string; confidence: number }>;
  warnings: string[];
  progress: JobProgress;
  created_at: string;
};

export type JobRowsResponse = {
  job_id: string;
  total_rows: number;
  rows: Array<Record<string, unknown>>;
};

export type RowClassification = {
  row_index: number;
  label: string;
  confidence: number;
};

export type JobInsightsResponse = {
  job_id: string;
  summary: string;
  row_classifications: RowClassification[];
  label_counts: Record<string, number>;
  warnings: string[];
  used_ai: boolean;
};

export type TemplateRecord = {
  template_id: string;
  domain: string;
  page_type: string;
  page_fingerprint: string | null;
  template: Record<string, unknown>;
  version: number;
  parent_template_id: string | null;
  success_count: number;
  failure_count: number;
  success_rate: number;
  last_verified_at: string | null;
  invalidated: boolean;
  invalidation_reason: string | null;
  created_at: string;
  updated_at: string;
};

export type TemplatesListResponse = {
  templates: TemplateRecord[];
};

export type ExportRequest = {
  format: "csv" | "json";
  selected_columns?: string[];
};

export type ExportResponse = {
  export_id: string;
  status: string;
  file_url: string;
};

export type ExportRecord = {
  export_id: string;
  job_id: string;
  format: string;
  file_url: string;
  status: string;
  created_at: string;
  completed_at: string | null;
};

export type ExportsListResponse = {
  total_exports: number;
  exports: ExportRecord[];
};

export type AuthSignupRequest = {
  email: string;
  name: string;
  password: string;
};

export type AuthLoginRequest = {
  email: string;
  password: string;
};

export type AuthUserProfile = SessionUser;

export type AuthTokenResponse = SessionState;

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "";

function apiBaseCandidates(): string[] {
  const normalized = API_BASE.replace(/\/+$/, "");
  const out: string[] = [];

  // Prefer same-origin requests to avoid local CORS and extension fetch hooks.
  if (typeof window !== "undefined") {
    out.push("");
  }

  if (normalized) {
    out.push(normalized);
  } else {
    out.push("http://127.0.0.1:8000", "http://localhost:8000");
  }

  for (const base of [...out]) {
    if (base.includes("127.0.0.1")) {
      out.push(base.replace("127.0.0.1", "localhost"));
    } else if (base.includes("localhost")) {
      out.push(base.replace("localhost", "127.0.0.1"));
    } else if (base.includes("0.0.0.0")) {
      out.push(base.replace("0.0.0.0", "127.0.0.1"));
      out.push(base.replace("0.0.0.0", "localhost"));
    }
  }

  return [...new Set(out)];
}

function buildQuery(params: Record<string, string | number | undefined | null>): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    query.set(key, String(value));
  }
  const serialized = query.toString();
  return serialized ? `?${serialized}` : "";
}

function parseXhrHeaders(rawHeaders: string): Headers {
  const headers = new Headers();
  for (const line of rawHeaders.split(/\r?\n/)) {
    if (!line.trim()) {
      continue;
    }
    const index = line.indexOf(":");
    if (index <= 0) {
      continue;
    }
    const key = line.slice(0, index).trim();
    const value = line.slice(index + 1).trim();
    if (key) {
      headers.append(key, value);
    }
  }
  return headers;
}

async function requestWithXhr(url: string, init: RequestInit | undefined): Promise<Response> {
  if (typeof window === "undefined" || typeof XMLHttpRequest === "undefined") {
    throw new Error("xhr_unavailable");
  }

  return new Promise<Response>((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open(init?.method ?? "GET", url, true);

    const headers = new Headers(init?.headers);
    headers.forEach((value, key) => {
      xhr.setRequestHeader(key, value);
    });

    xhr.onload = () => {
      if (xhr.status === 0) {
        reject(new Error("xhr_network_error"));
        return;
      }

      const body = xhr.responseText ?? "";
      const response = new Response(body, {
        status: xhr.status,
        statusText: xhr.statusText,
        headers: parseXhrHeaders(xhr.getAllResponseHeaders()),
      });
      resolve(response);
    };

    xhr.onerror = () => reject(new Error("xhr_request_failed"));
    xhr.onabort = () => reject(new Error("xhr_request_aborted"));

    const body = init?.body;
    if (typeof body === "string" || body == null) {
      xhr.send(body ?? null);
      return;
    }
    reject(new Error("xhr_body_not_supported"));
  });
}

async function apiRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  const hasBody = init?.body !== undefined && init.body !== null;
  if (hasBody && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const session = getSession();
  if (session?.access_token && !headers.has("Authorization")) {
    headers.set("Authorization", `Bearer ${session.access_token}`);
  }
  const bases = apiBaseCandidates();
  let response: Response | null = null;
  const errors: string[] = [];

  for (const base of bases) {
    const url = `${base}${path}`;
    try {
      response = await fetch(url, {
        ...init,
        headers,
        cache: "no-store",
      });
      break;
    } catch (err) {
      errors.push(`fetch(${url}): ${err instanceof Error ? err.message : "unknown_error"}`);
      try {
        response = await requestWithXhr(url, {
          ...init,
          headers,
        });
        break;
      } catch (xhrErr) {
        errors.push(`xhr(${url}): ${xhrErr instanceof Error ? xhrErr.message : "unknown_error"}`);
      }
      response = null;
    }
  }

  if (!response) {
    const tried = bases.join(", ");
    const tail = errors.length ? ` Last errors: ${errors.slice(-2).join(" | ")}` : "";
    throw new Error(
      `Failed to reach API. Tried: ${tried}. Ensure backend is running and CORS allows your frontend origin.${tail}`,
    );
  }

  if (!response.ok) {
    let detail = `Request failed with status ${response.status}`;
    try {
      const payload = (await response.json()) as { detail?: { message?: string } | string };
      if (typeof payload.detail === "string") {
        detail = payload.detail;
      } else if (payload.detail && typeof payload.detail.message === "string") {
        detail = payload.detail.message;
      }
    } catch {
      // keep generic detail when response is not json
    }
    throw new Error(detail);
  }

  return (await response.json()) as T;
}

export async function runPreview(payload: PreviewRequest): Promise<PreviewResponse> {
  return apiRequest<PreviewResponse>("/api/v1/scrape/preview", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function runScrape(payload: RunRequest): Promise<RunResponse> {
  return apiRequest<RunResponse>("/api/v1/scrape/run", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function getJobStatus(jobId: string): Promise<JobStatusResponse> {
  return apiRequest<JobStatusResponse>(`/api/v1/jobs/${encodeURIComponent(jobId)}`);
}

export async function listJobs(params?: {
  project_id?: string;
  status?: string;
  offset?: number;
  limit?: number;
}): Promise<JobsListResponse> {
  const query = buildQuery({
    project_id: params?.project_id,
    status: params?.status,
    offset: params?.offset ?? 0,
    limit: params?.limit ?? 50,
  });
  return apiRequest<JobsListResponse>(`/api/v1/jobs${query}`);
}

export async function getJobDetail(jobId: string): Promise<JobDetail> {
  return apiRequest<JobDetail>(`/api/v1/jobs/${encodeURIComponent(jobId)}/detail`);
}

export async function getJobRows(jobId: string, offset = 0, limit = 100): Promise<JobRowsResponse> {
  const query = buildQuery({ offset, limit });
  return apiRequest<JobRowsResponse>(`/api/v1/jobs/${encodeURIComponent(jobId)}/rows${query}`);
}

export async function getJobInsights(jobId: string, maxRows = 30): Promise<JobInsightsResponse> {
  const query = buildQuery({ max_rows: maxRows });
  return apiRequest<JobInsightsResponse>(`/api/v1/jobs/${encodeURIComponent(jobId)}/insights${query}`);
}

export async function cancelJob(jobId: string): Promise<{ job_id: string; status: string }> {
  return apiRequest<{ job_id: string; status: string }>(`/api/v1/jobs/${encodeURIComponent(jobId)}/cancel`, {
    method: "POST",
  });
}

export async function retryJob(jobId: string): Promise<RunResponse> {
  return apiRequest<RunResponse>(`/api/v1/jobs/${encodeURIComponent(jobId)}/retry`, {
    method: "POST",
  });
}

export async function listTemplates(params?: {
  domain?: string;
  page_type?: string;
}): Promise<TemplatesListResponse> {
  const query = buildQuery({
    domain: params?.domain,
    page_type: params?.page_type,
  });
  return apiRequest<TemplatesListResponse>(`/api/v1/templates${query}`);
}

export async function createExport(jobId: string, payload: ExportRequest): Promise<ExportResponse> {
  return apiRequest<ExportResponse>(`/api/v1/export/${encodeURIComponent(jobId)}`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function listExports(params?: {
  job_id?: string;
  offset?: number;
  limit?: number;
}): Promise<ExportsListResponse> {
  const query = buildQuery({
    job_id: params?.job_id,
    offset: params?.offset ?? 0,
    limit: params?.limit ?? 100,
  });
  return apiRequest<ExportsListResponse>(`/api/v1/exports${query}`);
}

export async function signup(payload: AuthSignupRequest): Promise<AuthTokenResponse> {
  return apiRequest<AuthTokenResponse>("/api/v1/auth/signup", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function login(payload: AuthLoginRequest): Promise<AuthTokenResponse> {
  return apiRequest<AuthTokenResponse>("/api/v1/auth/login", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function getAuthMe(): Promise<AuthUserProfile> {
  return apiRequest<AuthUserProfile>("/api/v1/auth/me");
}
