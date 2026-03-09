# WebScrapper Monorepo

This repository contains the MVP foundation for an AI-assisted web scraper based on the product and architecture documents.

## Current Scope
- FastAPI backend with API contracts, real HTML scraping, and scoring
- Next.js web app scaffold with New Scrape flow
- Monorepo structure for future services and shared packages
- Local dev infra for PostgreSQL and Redis-backed persistence

## Repository Layout
- `apps/api` - FastAPI API and orchestration entrypoint
- `apps/web` - Next.js frontend shell
- `services/*` - future extraction domain services
- `packages/*` - shared contracts and utilities
- `infra/*` - docker and scripts
- `*.md` - product/architecture docs

## Quick Start
1. Copy env file:
   - `cp .env.example .env`
2. Start dependencies:
   - `docker compose up -d`
3. Run API:
   - `cd apps/api && pip install -e . && python -m uvicorn app.main:app --reload --port 8000`
   - Optional for JS-heavy pages: `pip install -e ".[browser]" && playwright install chromium`
   - Shortcut from repo root: `python3 app.py --reload`
   - Run API + frontend together from repo root: `python3 app.py --reload --with-web` (uses `pnpm`, falls back to `npm`)
4. Run Worker (optional if `APP_EMBEDDED_WORKER=true`, recommended for production-like setup):
   - `cd apps/api && python -m app.worker_runner`
5. Run Web:
   - `cd apps/web && pnpm install && pnpm dev`
6. Apply DB migrations:
   - `cd apps/api && alembic -c alembic.ini upgrade head`
7. Open auth UI:
   - `http://localhost:3000/auth`
   - Sign up/login, then use `/new-scrape`, `/jobs`, and `/file-manager` with per-user project scope.

To force in-memory storage for quick local testing:
- `APP_STORE_BACKEND=memory`

To use PostgreSQL persistence:
- `APP_STORE_BACKEND=postgres` and valid `DATABASE_URL`

To use Redis queue:
- `APP_QUEUE_BACKEND=redis` and valid `REDIS_URL`
- If Redis is unavailable and `APP_QUEUE_BACKEND=auto`, queue falls back to memory.
- `APP_EMBEDDED_WORKER_CONCURRENCY=1`
- `APP_WORKER_MAX_RETRIES=3`
- `APP_WORKER_RETRY_BACKOFF_INITIAL_SECONDS=2`
- `APP_WORKER_RETRY_BACKOFF_MAX_SECONDS=30`
- `APP_WORKER_JOB_LOCK_TTL_SECONDS=300`

Auth and authorization controls:
- `APP_AUTH_ENABLED=true|false`
- `APP_AUTH_TOKENS_JSON={"token":{"user_id":"user_1","workspace_id":"ws_1","project_ids":["proj_1"],"role":"member"}}`
- `APP_AUTH_DEV_TOKEN=dev-token`
- `APP_AUTH_SIGNING_SECRET=<jwt-like-signing-secret>`
- `APP_AUTH_TOKEN_TTL_SECONDS=86400`
- `APP_AUTH_DEFAULT_WORKSPACE_ID=ws_default`
- `APP_AUTH_DEFAULT_USER_ID=user_local`

Rate limiting controls:
- `APP_RATE_LIMIT_ENABLED=true|false`
- `APP_RATE_LIMIT_BACKEND=memory|redis|auto`
- `APP_RATE_LIMIT_KEY_PREFIX=ratelimit:workspace`
- `APP_RATE_LIMIT_REQUESTS_PER_WINDOW=120`
- `APP_RATE_LIMIT_WINDOW_SECONDS=60`

Export pipeline controls:
- `APP_EXPORT_STORAGE_BACKEND=local|s3`
- `APP_EXPORT_LOCAL_DIR=./exports`
- `APP_EXPORT_SIGNED_URL_TTL_SECONDS=900`
- `APP_EXPORT_SIGNING_SECRET=<secret>`
- `APP_BASE_URL=http://localhost:8000`
- `APP_S3_BUCKET=<bucket>`
- `APP_S3_REGION=<region>`
- `APP_S3_ENDPOINT_URL=<s3-compatible-endpoint>`
- `APP_S3_ACCESS_KEY_ID=<access-key>`
- `APP_S3_SECRET_ACCESS_KEY=<secret-key>`

Playwright fallback controls:
- `APP_PLAYWRIGHT_FALLBACK=true|false`
- `APP_PLAYWRIGHT_TIMEOUT_SECONDS=20`

Scraping quality controls:
- `APP_DUPLICATE_ROW_CUTOFF_RATIO=0.8`
- `APP_MAX_CONSECUTIVE_LOW_YIELD_PAGES=1`

AI planner controls (labeling only):
- `APP_AI_PLANNER_ENABLED=true`
- `APP_AI_PROVIDER=gemini`
- `AI_API_KEY=<your-key>`
- `APP_AI_MODEL=gemini-2.0-flash`
- `APP_AI_MAX_SAMPLE_ROWS=8`
- `APP_AI_MAX_INPUT_CHARS=4000`
- `APP_AI_MAX_ESTIMATED_INPUT_TOKENS=1200`
- `APP_AI_LABELING_PROMPT=<optional override for Gemini field-labeling prompt>`

Data retention controls:
- `APP_DATA_RETENTION_DAYS=30`
- `APP_STARTUP_CLEANUP_ENABLED=true`

## Implemented API Routes
- `GET /health`
- `GET /metrics` (admin-only)
- `POST /api/v1/auth/signup`
- `POST /api/v1/auth/login`
- `GET /api/v1/auth/me`
- `POST /api/v1/scrape/preview`
- `POST /api/v1/scrape/run`
- `GET /api/v1/jobs/{job_id}`
- `GET /api/v1/jobs`
- `GET /api/v1/jobs/{job_id}/detail`
- `GET /api/v1/jobs/{job_id}/rows`
- `GET /api/v1/jobs/{job_id}/insights` (AI summarization + classification)
- `POST /api/v1/jobs/{job_id}/cancel`
- `POST /api/v1/jobs/{job_id}/retry`
- `POST /api/v1/templates`
- `GET /api/v1/templates`
- `POST /api/v1/export/{job_id}`
- `GET /api/v1/exports`
- `GET /api/v1/exports/{export_id}/download`
- `POST /api/v1/maintenance/cleanup`

## Notes
- Store backend is selected by `APP_STORE_BACKEND` (`memory`, `postgres`, or `auto`).
- In `auto` mode, the API uses PostgreSQL when `DATABASE_URL` is valid and falls back to memory otherwise.
- Queue backend is selected by `APP_QUEUE_BACKEND` (`memory`, `redis`, or `auto`).
- `/api/v1/scrape/run` is asynchronous and processed by queue worker(s).
- HTTP fetch is primary; Playwright is used as fallback for JS-heavy or HTTP-failed pages when enabled.
- Full scraping includes duplicate-page/row cutoffs and stronger pagination heuristics.
- Template engine includes domain+fingerprint auto-match, versioning, metrics, and invalidation.
- AI planner only labels/renames fields and kinds; extraction remains deterministic.
- Persistence includes migration-managed schema for users/workspaces/projects/jobs/templates/exports/usage events.
- Security middleware provides bearer auth, project scoping, request IDs, and rate-limit headers.
- Signup/login issue signed bearer tokens and auto-create a default workspace/project per user.
- Response tracing includes `X-Trace-Id` propagation for request correlation.
- Signed export download URLs are public but token-protected (`expires` + `token`).

## CI/CD Scaffolding
- CI workflow: `.github/workflows/ci.yml`
- Manual deploy workflows:
  - `.github/workflows/deploy-staging.yml`
  - `.github/workflows/deploy-production.yml`
- Deployment env templates:
  - `infra/environments/staging.env.example`
  - `infra/environments/production.env.example`
- Monitoring templates:
  - `infra/monitoring/alerts/prometheus-rules.yml`

## Quality Gates
- Fixture regression tests:
  - `cd apps/api && python -m pytest tests/test_regression_fixtures.py`
- Infra integration test (requires Postgres + Redis):
  - `cd apps/api && RUN_INFRA_INTEGRATION_TESTS=1 python -m pytest tests/test_integration_postgres_redis_worker.py`
- API end-to-end flow test:
  - `cd apps/api && python -m pytest tests/test_e2e_api_queue_export.py`
- Worker load baseline (opt-in):
  - `cd apps/api && RUN_LOAD_TESTS=1 python -m pytest tests/test_worker_load.py`
- Flaky monitoring:
  - `./infra/scripts/flaky-check.sh 3 tests`
  - scheduled workflow: `.github/workflows/flaky-check.yml`
