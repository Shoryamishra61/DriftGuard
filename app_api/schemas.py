"""Public request and response schemas for the DriftGuard API."""

from __future__ import annotations

import ipaddress
import json
import re
from datetime import datetime
from typing import Annotated, Any
from urllib.parse import unquote, urlsplit
from uuid import UUID

try:
    from enum import StrEnum
except ImportError:  # pragma: no cover - production runs Python 3.12
    from enum import Enum

    class StrEnum(str, Enum):  # noqa: UP042 -- Python 3.10 test support
        pass

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from common_utils.network import is_public_unicast_address

MAX_TEXT_BYTES = 50 * 1024
MAX_METADATA_BYTES = 64 * 1024
MAX_METADATA_DEPTH = 8
EMAIL_LOCAL_PATTERN = re.compile(r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+$")
SLACK_WEBHOOK_PATH = re.compile(
    r"^/services/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+$"
)
DISCORD_WEBHOOK_PATH = re.compile(
    r"^/api/webhooks/[0-9]{5,32}/[A-Za-z0-9._-]+$"
)
PAGERDUTY_TARGET = re.compile(r"^[A-Za-z0-9]{32}$")


def _validate_mailto_target(target: str) -> None:
    parsed = urlsplit(target)
    if (
        parsed.scheme.lower() != "mailto"
        or not parsed.path
        or parsed.netloc
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("DIGEST email targets must be one absolute mailto address")
    address = unquote(parsed.path)
    if "\r" in address or "\n" in address or address.count("@") != 1:
        raise ValueError("DIGEST email target is invalid")
    local, domain = address.rsplit("@", 1)
    if (
        not local
        or len(local) > 64
        or local.startswith(".")
        or local.endswith(".")
        or ".." in local
        or EMAIL_LOCAL_PATTERN.fullmatch(local) is None
    ):
        raise ValueError("DIGEST email target is invalid")
    try:
        ascii_domain = domain.encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise ValueError("DIGEST email target domain is invalid") from exc
    if (
        not ascii_domain
        or len(ascii_domain) > 253
        or "." not in ascii_domain
        or any(
            not label
            or len(label) > 63
            or label.startswith("-")
            or label.endswith("-")
            or not label.replace("-", "").isalnum()
            for label in ascii_domain.split(".")
        )
    ):
        raise ValueError("DIGEST email target domain is invalid")


def _normalized_url_hostname(target: str) -> tuple[Any, str]:
    parsed = urlsplit(target)
    if not parsed.hostname:
        raise ValueError("webhook target hostname is required")
    try:
        hostname = parsed.hostname.encode("idna").decode("ascii").lower().rstrip(".")
    except UnicodeError as exc:
        raise ValueError("webhook target hostname is invalid") from exc
    return parsed, hostname


def _validate_pagerduty_target(target: str) -> None:
    parsed = urlsplit(target)
    if (
        parsed.scheme.lower() != "pagerduty"
        or parsed.path
        or parsed.query
        or parsed.fragment
        or not PAGERDUTY_TARGET.fullmatch(parsed.netloc)
    ):
        raise ValueError(
            "PagerDuty targets require pagerduty:// followed by one 32-character routing key"
        )


def _validate_known_webhook_path(parsed: Any, hostname: str) -> None:
    if hostname == "hooks.slack.com":
        if (
            parsed.query
            or parsed.fragment
            or SLACK_WEBHOOK_PATH.fullmatch(parsed.path) is None
        ):
            raise ValueError("Slack webhook URL has an invalid path")
    elif hostname == "discord.com" and (
        parsed.query
        or parsed.fragment
        or DISCORD_WEBHOOK_PATH.fullmatch(parsed.path) is None
    ):
        raise ValueError("Discord webhook URL has an invalid path")
    elif hostname == "discordapp.com":
        raise ValueError("legacy Discord webhook host is not supported")


def enforce_delivery_target_policy(
    rule: AlertRuleUpdate,
    allowed_webhook_hosts: tuple[str, ...],
) -> None:
    """Enforce deployment-specific generic-webhook allowlisting."""

    if rule.action_type is AlertAction.MUTE:
        return
    parsed = urlsplit(rule.notification_target)
    scheme = parsed.scheme.lower()
    if rule.action_type is AlertAction.DIGEST and scheme == "mailto":
        return
    if rule.action_type is AlertAction.NOTIFY and scheme == "pagerduty":
        return

    _parsed, hostname = _normalized_url_hostname(rule.notification_target)
    if hostname in {"hooks.slack.com", "discord.com"}:
        return
    if not allowed_webhook_hosts or not any(
        hostname == allowed or hostname.endswith(f".{allowed}")
        for allowed in allowed_webhook_hosts
    ):
        raise ValueError("generic webhook hostname is not allowlisted")


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TelemetryPayload(StrictModel):
    session_id: Annotated[str, Field(min_length=1, max_length=100)]
    prompt_text: str
    output_text: str
    metadata: dict[str, JsonValue]

    @field_validator("session_id")
    @classmethod
    def validate_session_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("session_id must not be blank")
        return value

    @field_validator("prompt_text", "output_text")
    @classmethod
    def validate_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("text must not be blank")
        if len(value.encode("utf-8")) > MAX_TEXT_BYTES:
            raise ValueError("text exceeds the 50 KiB UTF-8 limit")
        return value

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(encoded) > MAX_METADATA_BYTES:
            raise ValueError("metadata exceeds the 64 KiB UTF-8 limit")

        stack: list[tuple[JsonValue, int]] = [(value, 1)]
        while stack:
            item, depth = stack.pop()
            if depth > MAX_METADATA_DEPTH:
                raise ValueError("metadata exceeds the maximum nesting depth")
            if isinstance(item, dict):
                stack.extend((child, depth + 1) for child in item.values())
            elif isinstance(item, list):
                stack.extend((child, depth + 1) for child in item)
        return value


class IngestResponse(StrictModel):
    status: str = "accepted"
    run_id: UUID


class ServiceStatus(StrEnum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class AlertAction(StrEnum):
    NOTIFY = "NOTIFY"
    DIGEST = "DIGEST"
    MUTE = "MUTE"


class AlertState(StrEnum):
    TRIGGERED = "TRIGGERED"
    RESOLVED = "RESOLVED"
    SNOOZED = "SNOOZED"


class AlertRouteStatus(StrEnum):
    PENDING = "PENDING"
    DELIVERED = "DELIVERED"
    SUPPRESSED = "SUPPRESSED"
    FAILED = "FAILED"


class TrendWindow(StrEnum):
    DAY = "24h"
    WEEK = "7d"
    MONTH = "30d"

    @property
    def hours(self) -> int:
        return {self.DAY: 24, self.WEEK: 168, self.MONTH: 720}[self]

    @property
    def bucket(self) -> str:
        return {
            self.DAY: "hour",
            self.WEEK: "day",
            self.MONTH: "week",
        }[self]


class TrendPoint(StrictModel):
    timestamp: datetime
    average_drift: float
    p95_latency_ms: float | None
    evaluations: int
    anomalies: int


class ThresholdLine(StrictModel):
    rule_name: str
    action_type: AlertAction
    threshold: float


class TrendSummary(StrictModel):
    weighted_average_drift: float | None
    evaluated_run_count: int
    active_alert_count: int
    p95_evaluation_latency_ms: float | None
    average_end_to_end_latency_ms: float | None
    p95_end_to_end_latency_ms: float | None


class TrendResponse(StrictModel):
    window: TrendWindow
    points: list[TrendPoint]
    thresholds: list[ThresholdLine]
    summary: TrendSummary


class AlertItem(StrictModel):
    id: UUID
    evaluation_id: UUID
    rule_id: int
    rule_name: str
    action_type: AlertAction
    status: AlertState
    notified_at: datetime | None
    route_status: AlertRouteStatus
    route_due_at: datetime | None
    delivery_lease_until: datetime | None
    delivery_attempts: int
    run_id: UUID
    session_id: str
    prompt_text: str
    output_text: str
    drift_distance: float | None
    matched_baseline_id: UUID | None
    matched_baseline_text: str | None = None
    evaluated_at: datetime


class AlertListResponse(StrictModel):
    items: list[AlertItem]
    limit: int
    offset: int
    has_more: bool


class VectorPointType(StrEnum):
    BASELINE = "baseline"
    EVALUATION = "evaluation"


class VectorProjectionPoint(StrictModel):
    id: UUID
    point_type: VectorPointType
    x: float
    y: float
    run_id: UUID | None = None
    baseline_set: Annotated[str | None, Field(max_length=100)] = None
    drift_distance: Annotated[float | None, Field(ge=0.0, le=2.0)] = None
    matched_baseline_id: UUID | None = None


class VectorProjectionResponse(StrictModel):
    points: list[VectorProjectionPoint]
    count: int
    limit: int
    has_more: bool


class AlertRule(StrictModel):
    id: int
    rule_name: str
    threshold: float
    action_type: AlertAction
    notification_target: str
    is_active: bool
    created_at: datetime


class AlertRuleUpdate(StrictModel):
    rule_name: Annotated[str, Field(min_length=1, max_length=100)]
    threshold: Annotated[float, Field(ge=0.0, le=2.0)]
    action_type: AlertAction
    notification_target: Annotated[str, Field(min_length=1, max_length=255)]
    is_active: bool

    @field_validator("rule_name", "notification_target")
    @classmethod
    def reject_blank_strings(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value must not be blank")
        return value

    @model_validator(mode="after")
    def validate_delivery_target(self) -> AlertRuleUpdate:
        if self.action_type is AlertAction.MUTE:
            return self

        parsed = urlsplit(self.notification_target)
        if self.action_type is AlertAction.DIGEST and parsed.scheme.lower() == "mailto":
            _validate_mailto_target(self.notification_target)
            return self
        if self.action_type is AlertAction.NOTIFY and parsed.scheme.lower() == "pagerduty":
            _validate_pagerduty_target(self.notification_target)
            return self
        if parsed.scheme.lower() != "https" or not parsed.hostname:
            raise ValueError(
                "NOTIFY targets require HTTPS or pagerduty; "
                "DIGEST targets require HTTPS or mailto"
            )
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("notification targets may not contain URL credentials")
        try:
            port = parsed.port or 443
        except ValueError as exc:
            raise ValueError("notification target port is invalid") from exc
        if not 10 <= port <= 65435:
            raise ValueError("notification target port is invalid")

        parsed, hostname = _normalized_url_hostname(self.notification_target)
        _validate_known_webhook_path(parsed, hostname)
        if hostname == "localhost" or hostname.endswith(
            (".localhost", ".local", ".invalid", ".test")
        ):
            raise ValueError("notification target hostname is not publicly routable")
        try:
            address = ipaddress.ip_address(hostname)
        except ValueError:
            return self
        if not is_public_unicast_address(address):
            raise ValueError("notification target IP is not publicly routable")
        return self


class AlertRuleCreate(AlertRuleUpdate):
    """Fields accepted when creating a tenant-owned alert rule."""


class PulseResponse(StrictModel):
    timestamp: datetime
    status: ServiceStatus
    services: dict[str, dict[str, Any]]
