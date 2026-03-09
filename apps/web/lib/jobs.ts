export const TERMINAL_STATUSES = new Set(["success", "partial_success", "failed", "cancelled"]);

export const ACTIVE_STATUSES = new Set([
  "queued",
  "retrying",
  "request_received",
  "page_loaded",
  "structure_detected",
  "fields_planned",
  "extraction_running",
  "pagination_running",
  "normalization_complete",
  "export_ready",
]);

export function isTerminalStatus(status: string): boolean {
  return TERMINAL_STATUSES.has(status);
}

export function isActiveStatus(status: string): boolean {
  return ACTIVE_STATUSES.has(status);
}

export function formatDateTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

export function statusClassName(status: string): string {
  if (status === "success" || status === "partial_success") {
    return "status-chip status-success";
  }
  if (status === "failed") {
    return "status-chip status-failed";
  }
  if (status === "cancelled") {
    return "status-chip status-cancelled";
  }
  if (status === "retrying") {
    return "status-chip status-retrying";
  }
  return "status-chip status-running";
}
