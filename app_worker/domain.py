"""Domain records shared by worker adapters and orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from uuid import UUID

UTC = getattr(__import__("datetime"), "UTC", timezone.utc)  # noqa: UP017


class ActionType(Enum):
    NOTIFY = "NOTIFY"
    DIGEST = "DIGEST"
    MUTE = "MUTE"


class AlertStatus(Enum):
    TRIGGERED = "TRIGGERED"
    RESOLVED = "RESOLVED"
    SNOOZED = "SNOOZED"


class RouteStatus(Enum):
    PENDING = "PENDING"
    DELIVERED = "DELIVERED"
    SUPPRESSED = "SUPPRESSED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class TelemetryRun:
    id: UUID
    project_id: UUID
    output_text: str
    ingested_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    active_baseline_set: str | None = None
    prompt_text: str = ""


@dataclass(frozen=True, slots=True)
class Evaluation:
    id: UUID
    run_id: UUID
    drift_distance: float | None
    matched_baseline_id: UUID | None
    evaluation_latency_ms: int
    is_anomaly: bool


@dataclass(frozen=True, slots=True)
class AlertRule:
    id: int
    project_id: UUID
    rule_name: str
    threshold: float
    action_type: ActionType
    notification_target: str


@dataclass(frozen=True, slots=True)
class Alert:
    id: UUID
    evaluation_id: UUID
    rule: AlertRule
    status: AlertStatus
    created: bool
    route_status: RouteStatus = RouteStatus.PENDING
    delivery_attempts: int = 0
    delivery_lease_token: UUID | None = None


@dataclass(frozen=True, slots=True)
class DeliveryItem:
    alert: Alert
    evaluation: Evaluation
    run: TelemetryRun


@dataclass(frozen=True, slots=True)
class DigestDelivery:
    lease_token: UUID
    project_id: UUID
    rule: AlertRule
    digest_day: date
    total_count: int
    evidence: tuple[DeliveryItem, ...]


@dataclass(frozen=True, slots=True)
class DeliveryBatch:
    items: tuple[DeliveryItem, ...]
    digest: DigestDelivery | None

    @property
    def claimed_count(self) -> int:
        return len(self.items) + (self.digest.total_count if self.digest else 0)


@dataclass(frozen=True, slots=True)
class BaselineMatch:
    id: UUID
    similarity: float


@dataclass(frozen=True, slots=True)
class BaselineSeed:
    id: UUID
    project_id: UUID
    baseline_set: str
    embedding_model_revision: str
    text: str
    vector: list[float]


@dataclass(frozen=True, slots=True)
class BaselineManifest:
    manifest_hash: str
    point_count: int


@dataclass(frozen=True, slots=True)
class Job:
    run_id: UUID
    event_id: UUID
    attempt: int = 1


@dataclass(frozen=True, slots=True)
class ProcessingResult:
    evaluation: Evaluation
    created: bool
    notification_failures: int = 0
