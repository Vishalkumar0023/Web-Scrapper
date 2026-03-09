from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from threading import Lock


logger = logging.getLogger("webscrapper.api")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True)
class RequestObservation:
    method: str
    path: str
    status_code: int
    duration_ms: float


class MetricsCollector:
    def __init__(self) -> None:
        self._lock = Lock()
        self._requests_total: dict[tuple[str, str, int], int] = defaultdict(int)
        self._request_duration_ms_sum: dict[tuple[str, str], float] = defaultdict(float)
        self._request_duration_ms_count: dict[tuple[str, str], int] = defaultdict(int)
        self._rate_limit_blocked_total: dict[str, int] = defaultdict(int)
        self._auth_failed_total: dict[str, int] = defaultdict(int)

    def observe_request(self, observation: RequestObservation) -> None:
        key = (observation.method, observation.path, observation.status_code)
        duration_key = (observation.method, observation.path)
        with self._lock:
            self._requests_total[key] += 1
            self._request_duration_ms_sum[duration_key] += observation.duration_ms
            self._request_duration_ms_count[duration_key] += 1

    def observe_rate_limit_block(self, path: str) -> None:
        with self._lock:
            self._rate_limit_blocked_total[path] += 1

    def observe_auth_failed(self, path: str) -> None:
        with self._lock:
            self._auth_failed_total[path] += 1

    def render_prometheus(self) -> str:
        lines: list[str] = [
            "# HELP api_requests_total Total HTTP requests.",
            "# TYPE api_requests_total counter",
        ]
        with self._lock:
            for (method, path, status_code), value in sorted(self._requests_total.items()):
                lines.append(
                    f'api_requests_total{{method="{method}",path="{path}",status_code="{status_code}"}} {value}'
                )

            lines.extend(
                [
                    "# HELP api_request_duration_ms_sum Request duration sum in milliseconds.",
                    "# TYPE api_request_duration_ms_sum counter",
                ]
            )
            for (method, path), value in sorted(self._request_duration_ms_sum.items()):
                lines.append(f'api_request_duration_ms_sum{{method="{method}",path="{path}"}} {value:.6f}')

            lines.extend(
                [
                    "# HELP api_request_duration_ms_count Request duration observation count.",
                    "# TYPE api_request_duration_ms_count counter",
                ]
            )
            for (method, path), value in sorted(self._request_duration_ms_count.items()):
                lines.append(f'api_request_duration_ms_count{{method="{method}",path="{path}"}} {value}')

            lines.extend(
                [
                    "# HELP api_rate_limit_blocked_total Requests blocked by rate limiting.",
                    "# TYPE api_rate_limit_blocked_total counter",
                ]
            )
            for path, value in sorted(self._rate_limit_blocked_total.items()):
                lines.append(f'api_rate_limit_blocked_total{{path="{path}"}} {value}')

            lines.extend(
                [
                    "# HELP api_auth_failed_total Requests denied by authentication.",
                    "# TYPE api_auth_failed_total counter",
                ]
            )
            for path, value in sorted(self._auth_failed_total.items()):
                lines.append(f'api_auth_failed_total{{path="{path}"}} {value}')

        return "\n".join(lines) + "\n"


def log_request_event(
    request_id: str,
    method: str,
    path: str,
    status_code: int,
    duration_ms: float,
    trace_id: str | None = None,
    workspace_id: str | None = None,
    user_id: str | None = None,
) -> None:
    payload: dict[str, object] = {
        "event": "http_request",
        "request_id": request_id,
        "method": method,
        "path": path,
        "status_code": status_code,
        "duration_ms": round(duration_ms, 3),
    }
    if trace_id:
        payload["trace_id"] = trace_id
    if workspace_id:
        payload["workspace_id"] = workspace_id
    if user_id:
        payload["user_id"] = user_id
    logger.info(json.dumps(payload, separators=(",", ":")))
