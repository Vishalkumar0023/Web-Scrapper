export type SessionUser = {
  user_id: string;
  email: string;
  name: string;
  workspace_id: string;
  project_ids: string[];
  default_project_id: string;
  role: string;
};

export type SessionState = {
  access_token: string;
  token_type: string;
  expires_in: number;
  user: SessionUser;
};

const SESSION_STORAGE_KEY = "webscrapper_session_v1";
const SESSION_EVENT_NAME = "webscrapper:session_changed";

export function getSession(): SessionState | null {
  if (typeof window === "undefined") {
    return null;
  }
  const raw = window.localStorage.getItem(SESSION_STORAGE_KEY);
  if (!raw) {
    return null;
  }
  try {
    const parsed = JSON.parse(raw) as SessionState;
    if (!parsed.access_token || !parsed.user?.user_id) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

export function setSession(session: SessionState): void {
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(session));
  window.dispatchEvent(new Event(SESSION_EVENT_NAME));
}

export function clearSession(): void {
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.removeItem(SESSION_STORAGE_KEY);
  window.dispatchEvent(new Event(SESSION_EVENT_NAME));
}

export function sessionEventName(): string {
  return SESSION_EVENT_NAME;
}
