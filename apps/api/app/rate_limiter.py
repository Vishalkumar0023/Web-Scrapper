from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass
from threading import Lock
from typing import Protocol

try:
    import redis
except Exception:  # pragma: no cover - optional dependency fallback
    redis = None


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    remaining: int
    reset_seconds: int


class RateLimiter(Protocol):
    backend: str

    def check(self, key: str, limit: int, window_seconds: int) -> RateLimitResult: ...


class InMemoryRateLimiter:
    backend = "memory"

    def __init__(self) -> None:
        self._entries: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def check(self, key: str, limit: int, window_seconds: int) -> RateLimitResult:
        now = time.time()
        bounded_limit = max(1, int(limit))
        bounded_window = max(1, int(window_seconds))
        cutoff = now - bounded_window

        with self._lock:
            bucket = self._entries[key]
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            if len(bucket) >= bounded_limit:
                reset_seconds = int(max(1, round(bucket[0] + bounded_window - now)))
                return RateLimitResult(allowed=False, remaining=0, reset_seconds=reset_seconds)

            bucket.append(now)
            remaining = max(0, bounded_limit - len(bucket))
            return RateLimitResult(allowed=True, remaining=remaining, reset_seconds=bounded_window)


class RedisRateLimiter:
    backend = "redis"

    def __init__(self, redis_url: str, key_prefix: str) -> None:
        if redis is None:
            raise RuntimeError("redis dependency is not installed")
        self._key_prefix = key_prefix
        self._client = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_timeout=0.5,
            socket_connect_timeout=0.5,
        )

    def ping(self) -> bool:
        return bool(self._client.ping())

    def check(self, key: str, limit: int, window_seconds: int) -> RateLimitResult:
        bounded_limit = max(1, int(limit))
        bounded_window = max(1, int(window_seconds))
        full_key = f"{self._key_prefix}:{key}"

        pipe = self._client.pipeline()
        pipe.incr(full_key, amount=1)
        pipe.ttl(full_key)
        count, ttl = pipe.execute()

        if count == 1:
            self._client.expire(full_key, bounded_window)
            ttl = bounded_window
        elif ttl is None or int(ttl) <= 0:
            self._client.expire(full_key, bounded_window)
            ttl = bounded_window

        count_int = int(count)
        ttl_int = max(1, int(ttl))

        if count_int > bounded_limit:
            return RateLimitResult(allowed=False, remaining=0, reset_seconds=ttl_int)

        remaining = max(0, bounded_limit - count_int)
        return RateLimitResult(allowed=True, remaining=remaining, reset_seconds=ttl_int)


def create_rate_limiter(rate_limit_backend: str, redis_url: str, key_prefix: str) -> RateLimiter:
    backend = rate_limit_backend.strip().lower()
    if backend == "memory":
        return InMemoryRateLimiter()

    if backend == "redis" and not redis_url:
        raise RuntimeError("APP_RATE_LIMIT_BACKEND is set to 'redis' but REDIS_URL is empty")

    if redis_url:
        try:
            limiter = RedisRateLimiter(redis_url=redis_url, key_prefix=key_prefix)
            limiter.ping()
            return limiter
        except Exception:
            if backend == "redis":
                raise

    return InMemoryRateLimiter()
