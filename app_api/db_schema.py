"""SQLAlchemy table metadata matching the canonical Alembic migration."""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = MetaData()

projects = Table(
    "projects",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("name", String(100), nullable=False),
    Column("api_key_hash", String(64), nullable=False),
    Column("active_baseline_set", String(100)),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

telemetry_runs = Table(
    "telemetry_runs",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("project_id", UUID(as_uuid=True), nullable=False),
    Column("session_id", String(100), nullable=False),
    Column("prompt_text", Text, nullable=False),
    Column("output_text", Text, nullable=False),
    Column("raw_metadata", JSONB, nullable=False),
    Column("status", String(50), nullable=False),
    Column("ingested_at", DateTime(timezone=True), nullable=False),
)

evaluations = Table(
    "evaluations",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("run_id", UUID(as_uuid=True), nullable=False),
    Column("drift_distance", Float),
    Column("matched_baseline_id", UUID(as_uuid=True)),
    Column("evaluation_latency_ms", Integer),
    Column("is_anomaly", Boolean, nullable=False),
    Column("evaluated_at", DateTime(timezone=True), nullable=False),
)

alert_rules = Table(
    "alert_rules",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("project_id", UUID(as_uuid=True), nullable=False),
    Column("rule_name", String(100), nullable=False),
    Column("threshold", Float, nullable=False),
    Column("action_type", String(50), nullable=False),
    Column("notification_target", String(255), nullable=False),
    Column("is_active", Boolean, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

alerts = Table(
    "alerts",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("evaluation_id", UUID(as_uuid=True), nullable=False),
    Column("rule_id", Integer, nullable=False),
    Column("alert_status", String(50), nullable=False),
    Column("notified_at", DateTime(timezone=True)),
    Column("route_status", String(20), nullable=False),
    Column("route_due_at", DateTime(timezone=True)),
    Column("delivery_lease_until", DateTime(timezone=True)),
    Column("delivery_lease_token", UUID(as_uuid=True)),
    Column("delivery_attempts", Integer, nullable=False),
    Column("last_delivery_error", Text),
)

telemetry_outbox = Table(
    "telemetry_outbox",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("run_id", UUID(as_uuid=True), nullable=False),
    Column("event_type", String(100), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("status", String(50), nullable=False),
    Column("retry_count", Integer, nullable=False),
    Column("next_attempt_at", DateTime(timezone=True), nullable=False),
    Column("dispatch_time", DateTime(timezone=True)),
    Column("last_error", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

legal_holds = Table(
    "legal_holds",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("project_id", UUID(as_uuid=True), nullable=False),
    Column("starts_at", DateTime(timezone=True), nullable=False),
    Column("ends_at", DateTime(timezone=True)),
    Column("reason", String(500), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("released_at", DateTime(timezone=True)),
)

retention_vector_outbox = Table(
    "retention_vector_outbox",
    metadata,
    Column("run_id", UUID(as_uuid=True), primary_key=True),
    Column("status", String(20), nullable=False),
    Column("attempts", Integer, nullable=False),
    Column("next_attempt_at", DateTime(timezone=True), nullable=False),
    Column("last_error", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True)),
)
