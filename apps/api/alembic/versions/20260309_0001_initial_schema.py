"""initial schema

Revision ID: 20260309_0001
Revises: 
Create Date: 2026-03-09 18:30:00

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20260309_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("email"),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=False)

    op.create_table(
        "workspaces",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("owner_user_id", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_workspaces_owner_user_id", "workspaces", ["owner_user_id"], unique=False)

    op.create_table(
        "projects",
        sa.Column("id", sa.String(length=128), nullable=False),
        sa.Column("workspace_id", sa.String(length=64), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["workspace_id"], ["workspaces.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_projects_workspace_id", "projects", ["workspace_id"], unique=False)

    op.create_table(
        "scrape_jobs",
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("project_id", sa.String(length=128), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("input_url", sa.Text(), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=True),
        sa.Column("max_pages", sa.Integer(), nullable=False),
        sa.Column("max_rows", sa.Integer(), nullable=False),
        sa.Column("fields_json", sa.JSON(), nullable=False),
        sa.Column("page_type", sa.String(length=32), nullable=False),
        sa.Column("warnings_json", sa.JSON(), nullable=False),
        sa.Column("pages_processed", sa.Integer(), nullable=False),
        sa.Column("rows_extracted", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("job_id"),
    )
    op.create_index("ix_scrape_jobs_project_id", "scrape_jobs", ["project_id"], unique=False)
    op.create_index("ix_scrape_jobs_status", "scrape_jobs", ["status"], unique=False)
    op.create_index("ix_scrape_jobs_project_created", "scrape_jobs", ["project_id", "created_at"], unique=False)
    op.create_index("ix_scrape_jobs_status_created", "scrape_jobs", ["status", "created_at"], unique=False)

    op.create_table(
        "result_rows",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("row_index", sa.Integer(), nullable=False),
        sa.Column("row_json", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["scrape_jobs.job_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_result_rows_job_id", "result_rows", ["job_id"], unique=False)
    op.create_index("ix_result_rows_job_rowindex", "result_rows", ["job_id", "row_index"], unique=False)

    op.create_table(
        "templates",
        sa.Column("template_id", sa.String(length=64), nullable=False),
        sa.Column("domain", sa.String(length=255), nullable=False),
        sa.Column("page_type", sa.String(length=64), nullable=False),
        sa.Column("page_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("template_json", sa.JSON(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("parent_template_id", sa.String(length=64), nullable=True),
        sa.Column("success_count", sa.Integer(), nullable=False),
        sa.Column("failure_count", sa.Integer(), nullable=False),
        sa.Column("success_rate", sa.Float(), nullable=False),
        sa.Column("last_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("invalidated", sa.Boolean(), nullable=False),
        sa.Column("invalidation_reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("template_id"),
    )
    op.create_index("ix_templates_domain", "templates", ["domain"], unique=False)
    op.create_index("ix_templates_page_type", "templates", ["page_type"], unique=False)
    op.create_index("ix_templates_page_fingerprint", "templates", ["page_fingerprint"], unique=False)
    op.create_index("ix_templates_domain_fingerprint", "templates", ["domain", "page_fingerprint"], unique=False)
    op.create_index("ix_templates_workspace_like", "templates", ["domain", "page_type", "invalidated"], unique=False)

    op.create_table(
        "exports",
        sa.Column("export_id", sa.String(length=64), nullable=False),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("format", sa.String(length=16), nullable=False),
        sa.Column("file_url", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["job_id"], ["scrape_jobs.job_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("export_id"),
    )
    op.create_index("ix_exports_job_id", "exports", ["job_id"], unique=False)
    op.create_index("ix_exports_job_created", "exports", ["job_id", "created_at"], unique=False)

    op.create_table(
        "usage_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("workspace_id", sa.String(length=64), nullable=False),
        sa.Column("user_id", sa.String(length=64), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("event_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_usage_events_workspace_id", "usage_events", ["workspace_id"], unique=False)
    op.create_index("ix_usage_events_user_id", "usage_events", ["user_id"], unique=False)
    op.create_index("ix_usage_events_event_type", "usage_events", ["event_type"], unique=False)
    op.create_index("ix_usage_events_workspace_created", "usage_events", ["workspace_id", "created_at"], unique=False)
    op.create_index("ix_usage_events_user_created", "usage_events", ["user_id", "created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_usage_events_user_created", table_name="usage_events")
    op.drop_index("ix_usage_events_workspace_created", table_name="usage_events")
    op.drop_index("ix_usage_events_event_type", table_name="usage_events")
    op.drop_index("ix_usage_events_user_id", table_name="usage_events")
    op.drop_index("ix_usage_events_workspace_id", table_name="usage_events")
    op.drop_table("usage_events")

    op.drop_index("ix_exports_job_created", table_name="exports")
    op.drop_index("ix_exports_job_id", table_name="exports")
    op.drop_table("exports")

    op.drop_index("ix_templates_workspace_like", table_name="templates")
    op.drop_index("ix_templates_domain_fingerprint", table_name="templates")
    op.drop_index("ix_templates_page_fingerprint", table_name="templates")
    op.drop_index("ix_templates_page_type", table_name="templates")
    op.drop_index("ix_templates_domain", table_name="templates")
    op.drop_table("templates")

    op.drop_index("ix_result_rows_job_rowindex", table_name="result_rows")
    op.drop_index("ix_result_rows_job_id", table_name="result_rows")
    op.drop_table("result_rows")

    op.drop_index("ix_scrape_jobs_status_created", table_name="scrape_jobs")
    op.drop_index("ix_scrape_jobs_project_created", table_name="scrape_jobs")
    op.drop_index("ix_scrape_jobs_status", table_name="scrape_jobs")
    op.drop_index("ix_scrape_jobs_project_id", table_name="scrape_jobs")
    op.drop_table("scrape_jobs")

    op.drop_index("ix_projects_workspace_id", table_name="projects")
    op.drop_table("projects")

    op.drop_index("ix_workspaces_owner_user_id", table_name="workspaces")
    op.drop_table("workspaces")

    op.drop_index("ix_users_email", table_name="users")
    op.drop_table("users")
