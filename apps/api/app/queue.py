from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, replace
from itertools import count
from queue import Empty, PriorityQueue
from threading import Lock
from typing import Protocol

try:
    import redis
except Exception:  # pragma: no cover - optional dependency fallback
    redis = None


@dataclass(frozen=True)
class RunJobMessage:
    job_id: str
    project_id: str
    url: str
    prompt: str | None
    max_pages: int
    max_rows: int
    template_id: str | None = None
    attempt: int = 0
    max_attempts: int = 3
    available_at: float = 0.0
    force: bool = False
    idempotency_key: str | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, payload: str) -> "RunJobMessage":
        return cls(**json.loads(payload))

    def with_retry(self, delay_seconds: float) -> "RunJobMessage":
        return replace(
            self,
            attempt=self.attempt + 1,
            available_at=time.time() + max(0.0, delay_seconds),
        )


class JobQueue(Protocol):
    backend: str

    def enqueue(self, message: RunJobMessage) -> None: ...

    def dequeue(self, timeout_seconds: int = 1) -> RunJobMessage | None: ...

    def enqueue_dead_letter(self, message: RunJobMessage, reason: str) -> None: ...

    def list_dead_letters(self, limit: int = 100) -> list[dict[str, object]]: ...

    def acquire_job_lock(self, job_id: str, owner_id: str, ttl_seconds: int) -> bool: ...

    def release_job_lock(self, job_id: str, owner_id: str) -> None: ...


class InMemoryJobQueue:
    backend = "memory"

    def __init__(self) -> None:
        self._queue: PriorityQueue[tuple[float, int, str]] = PriorityQueue()
        self._dead_letters: list[dict[str, object]] = []
        self._job_locks: dict[str, tuple[str, float]] = {}
        self._sequence = count()
        self._lock = Lock()

    def enqueue(self, message: RunJobMessage) -> None:
        self._queue.put((float(message.available_at), next(self._sequence), message.to_json()))

    def dequeue(self, timeout_seconds: int = 1) -> RunJobMessage | None:
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                available_at, sequence, payload = self._queue.get(timeout=min(0.1, remaining))
            except Empty:
                continue

            now = time.time()
            if available_at > now:
                self._queue.put((available_at, sequence, payload))
                sleep_for = min(available_at - now, remaining, 0.1)
                if sleep_for > 0:
                    time.sleep(sleep_for)
                continue

            return RunJobMessage.from_json(payload)

    def enqueue_dead_letter(self, message: RunJobMessage, reason: str) -> None:
        with self._lock:
            self._dead_letters.append(
                {
                    "message": asdict(message),
                    "reason": reason,
                    "created_at": time.time(),
                }
            )

    def list_dead_letters(self, limit: int = 100) -> list[dict[str, object]]:
        with self._lock:
            if limit <= 0:
                return []
            return list(reversed(self._dead_letters[-limit:]))

    def acquire_job_lock(self, job_id: str, owner_id: str, ttl_seconds: int) -> bool:
        now = time.time()
        expiry = now + max(1, int(ttl_seconds))
        with self._lock:
            existing = self._job_locks.get(job_id)
            if existing and existing[1] > now and existing[0] != owner_id:
                return False

            self._job_locks[job_id] = (owner_id, expiry)
            return True

    def release_job_lock(self, job_id: str, owner_id: str) -> None:
        with self._lock:
            existing = self._job_locks.get(job_id)
            if existing and existing[0] == owner_id:
                del self._job_locks[job_id]


class RedisJobQueue:
    backend = "redis"

    def __init__(self, redis_url: str, queue_key: str) -> None:
        if redis is None:
            raise RuntimeError("redis dependency is not installed")

        self._queue_key = queue_key
        self._dead_letter_key = f"{queue_key}:dead"
        self._lock_prefix = f"{queue_key}:lock:"
        self._client = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_timeout=0.5,
            socket_connect_timeout=0.5,
        )

    def ping(self) -> bool:
        return bool(self._client.ping())

    def enqueue(self, message: RunJobMessage) -> None:
        self._client.rpush(self._queue_key, message.to_json())

    def dequeue(self, timeout_seconds: int = 1) -> RunJobMessage | None:
        try:
            value = self._client.blpop(self._queue_key, timeout=timeout_seconds)
        except Exception as exc:
            # Redis socket read timeouts should not crash worker threads.
            if redis is not None and isinstance(exc, redis.exceptions.RedisError):
                return None
            raise
        if value is None:
            return None

        _key, payload = value
        message = RunJobMessage.from_json(payload)
        if message.available_at > time.time():
            # Delayed retry message, move it back to the tail.
            self._client.rpush(self._queue_key, message.to_json())
            sleep_for = min(max(0.0, message.available_at - time.time()), max(0.0, float(timeout_seconds)), 0.5)
            if sleep_for > 0:
                time.sleep(sleep_for)
            return None
        return message

    def enqueue_dead_letter(self, message: RunJobMessage, reason: str) -> None:
        payload = {
            "message": asdict(message),
            "reason": reason,
            "created_at": time.time(),
        }
        self._client.rpush(self._dead_letter_key, json.dumps(payload))

    def list_dead_letters(self, limit: int = 100) -> list[dict[str, object]]:
        if limit <= 0:
            return []
        payloads = self._client.lrange(self._dead_letter_key, -limit, -1)
        return [json.loads(item) for item in reversed(payloads)]

    def acquire_job_lock(self, job_id: str, owner_id: str, ttl_seconds: int) -> bool:
        key = f"{self._lock_prefix}{job_id}"
        ttl = max(1, int(ttl_seconds))
        acquired = self._client.set(key, owner_id, nx=True, ex=ttl)
        if acquired:
            return True

        current_owner = self._client.get(key)
        if current_owner == owner_id:
            self._client.expire(key, ttl)
            return True

        return False

    def release_job_lock(self, job_id: str, owner_id: str) -> None:
        key = f"{self._lock_prefix}{job_id}"
        self._client.eval(
            """
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            end
            return 0
            """,
            1,
            key,
            owner_id,
        )


def create_queue(queue_backend: str, redis_url: str, queue_key: str) -> JobQueue:
    backend = queue_backend.strip().lower()

    if backend == "memory":
        return InMemoryJobQueue()

    if backend == "redis" and not redis_url:
        raise RuntimeError("APP_QUEUE_BACKEND is set to 'redis' but REDIS_URL is empty")

    if redis_url:
        try:
            redis_queue = RedisJobQueue(redis_url=redis_url, queue_key=queue_key)
            redis_queue.ping()
            return redis_queue
        except Exception:
            if backend == "redis":
                raise

    return InMemoryJobQueue()
