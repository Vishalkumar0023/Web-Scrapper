.PHONY: api worker web infra-up infra-down db-migrate db-revision cleanup

api:
	cd apps/api && python -m uvicorn app.main:app --reload --port 8000

worker:
	cd apps/api && python -m app.worker_runner

web:
	cd apps/web && pnpm dev

infra-up:
	docker compose up -d

infra-down:
	docker compose down

db-migrate:
	cd apps/api && alembic -c alembic.ini upgrade head

db-revision:
	cd apps/api && alembic -c alembic.ini revision --autogenerate -m \"schema update\"

cleanup:
	cd apps/api && python -m app.maintenance --retention-days 30
