"""Authenticated dashboard queries scoped to the resolved project tenant."""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import Select, func, insert, or_, select, text, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app_api.auth import ProjectDependency
from app_api.config import Settings
from app_api.database import get_session
from app_api.db_schema import alert_rules, alerts, evaluations, telemetry_runs
from app_api.qdrant import BaselineTextResolver, get_baseline_text_resolver
from app_api.schemas import (
    AlertItem,
    AlertListResponse,
    AlertRule,
    AlertRuleCreate,
    AlertRuleUpdate,
    AlertState,
    ThresholdLine,
    TrendPoint,
    TrendResponse,
    TrendSummary,
    TrendWindow,
    enforce_delivery_target_policy,
)
from app_api.security import require_admin_token

logger = logging.getLogger("driftguard.analytics")
router = APIRouter(
    prefix="/api/v1",
    tags=["dashboard"],
    dependencies=[Depends(require_admin_token)],
)
SessionDependency = Annotated[AsyncSession, Depends(get_session)]


def get_request_settings(request: Request) -> Settings:
    runtime = getattr(request.app.state, "runtime", None)
    return runtime.settings if runtime is not None else request.app.state.settings


SettingsDependency = Annotated[Settings, Depends(get_request_settings)]


def _require_allowed_delivery_target(
    payload: AlertRuleCreate | AlertRuleUpdate,
    settings: Settings,
) -> None:
    try:
        enforce_delivery_target_policy(payload, settings.webhook_allowed_hosts)
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail="notification target is not an approved delivery destination",
        ) from None


def _trend_statement(window: TrendWindow, project_id) -> Select:
    bucket = func.date_trunc(window.bucket, evaluations.c.evaluated_at).label("timestamp")
    return (
        select(
            bucket,
            func.avg(evaluations.c.drift_distance).label("average_drift"),
            func.percentile_cont(0.95)
            .within_group(evaluations.c.evaluation_latency_ms)
            .label("p95_latency_ms"),
            func.count(evaluations.c.id).label("evaluations"),
            func.count(evaluations.c.id)
            .filter(evaluations.c.is_anomaly.is_(True))
            .label("anomalies"),
        )
        .select_from(
            evaluations.join(telemetry_runs, evaluations.c.run_id == telemetry_runs.c.id)
        )
        .where(
            telemetry_runs.c.project_id == project_id,
            evaluations.c.evaluated_at
            >= text("NOW() - (:window_hours * INTERVAL '1 hour')"),
            evaluations.c.drift_distance.is_not(None),
        )
        .group_by(bucket)
        .order_by(bucket)
        .params(window_hours=window.hours)
    )


def _trend_summary_statement(window: TrendWindow, project_id) -> Select:
    end_to_end_latency_ms = (
        func.extract(
            "epoch",
            evaluations.c.evaluated_at - telemetry_runs.c.ingested_at,
        )
        * 1000.0
    )
    return (
        select(
            func.avg(evaluations.c.drift_distance).label("weighted_average_drift"),
            func.count(evaluations.c.id).label("evaluated_run_count"),
            func.percentile_cont(0.95)
            .within_group(evaluations.c.evaluation_latency_ms)
            .label("p95_evaluation_latency_ms"),
            func.avg(end_to_end_latency_ms).label("average_end_to_end_latency_ms"),
            func.percentile_cont(0.95)
            .within_group(end_to_end_latency_ms)
            .label("p95_end_to_end_latency_ms"),
        )
        .select_from(
            evaluations.join(telemetry_runs, evaluations.c.run_id == telemetry_runs.c.id)
        )
        .where(
            telemetry_runs.c.project_id == project_id,
            evaluations.c.evaluated_at
            >= text("NOW() - (:summary_window_hours * INTERVAL '1 hour')"),
        )
        .params(summary_window_hours=window.hours)
    )


def _active_alert_count_statement(project_id) -> Select:
    return (
        select(func.count(alerts.c.id))
        .select_from(
            alerts.join(evaluations, alerts.c.evaluation_id == evaluations.c.id).join(
                telemetry_runs,
                evaluations.c.run_id == telemetry_runs.c.id,
            )
        )
        .where(
            telemetry_runs.c.project_id == project_id,
            alerts.c.alert_status == "TRIGGERED",
        )
    )


@router.get("/metrics/trends", response_model=TrendResponse)
async def get_trends(
    project: ProjectDependency,
    session: SessionDependency,
    window: Annotated[TrendWindow, Query()] = TrendWindow.DAY,
) -> TrendResponse:
    try:
        trend_result = await session.execute(_trend_statement(window, project.id))
        threshold_result = await session.execute(
            select(
                alert_rules.c.rule_name,
                alert_rules.c.action_type,
                alert_rules.c.threshold,
            )
            .where(
                alert_rules.c.project_id == project.id,
                alert_rules.c.is_active.is_(True),
            )
            .order_by(alert_rules.c.threshold)
        )
        summary_result = await session.execute(_trend_summary_statement(window, project.id))
        active_alert_result = await session.execute(
            _active_alert_count_statement(project.id)
        )
    except SQLAlchemyError as exc:
        logger.error("trend query failed (%s)", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="metrics temporarily unavailable",
        ) from None

    points = [
        TrendPoint(
            timestamp=row["timestamp"],
            average_drift=float(row["average_drift"]),
            p95_latency_ms=(
                float(row["p95_latency_ms"])
                if row["p95_latency_ms"] is not None
                else None
            ),
            evaluations=int(row["evaluations"]),
            anomalies=int(row["anomalies"]),
        )
        for row in trend_result.mappings()
    ]
    thresholds = [
        ThresholdLine(
            rule_name=row["rule_name"],
            action_type=row["action_type"],
            threshold=float(row["threshold"]),
        )
        for row in threshold_result.mappings()
    ]
    summary_row = summary_result.mappings().one()

    def optional_float(value):
        return float(value) if value is not None else None

    summary = TrendSummary(
        weighted_average_drift=optional_float(summary_row["weighted_average_drift"]),
        evaluated_run_count=int(summary_row["evaluated_run_count"]),
        active_alert_count=int(active_alert_result.scalar_one()),
        p95_evaluation_latency_ms=optional_float(
            summary_row["p95_evaluation_latency_ms"]
        ),
        average_end_to_end_latency_ms=optional_float(
            summary_row["average_end_to_end_latency_ms"]
        ),
        p95_end_to_end_latency_ms=optional_float(
            summary_row["p95_end_to_end_latency_ms"]
        ),
    )
    return TrendResponse(
        window=window,
        points=points,
        thresholds=thresholds,
        summary=summary,
    )


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _alerts_statement(
    project_id,
    alert_state: AlertState | None,
    search_query: str | None,
) -> Select:
    statement = (
        select(
            alerts.c.id,
            alerts.c.evaluation_id,
            alerts.c.rule_id,
            alert_rules.c.rule_name,
            alert_rules.c.action_type,
            alerts.c.alert_status.label("status"),
            alerts.c.notified_at,
            alerts.c.route_status,
            alerts.c.route_due_at,
            alerts.c.delivery_lease_until,
            alerts.c.delivery_attempts,
            telemetry_runs.c.id.label("run_id"),
            telemetry_runs.c.session_id,
            telemetry_runs.c.prompt_text,
            telemetry_runs.c.output_text,
            evaluations.c.drift_distance,
            evaluations.c.matched_baseline_id,
            evaluations.c.evaluated_at,
        )
        .select_from(
            alerts.join(evaluations, alerts.c.evaluation_id == evaluations.c.id)
            .join(telemetry_runs, evaluations.c.run_id == telemetry_runs.c.id)
            .join(alert_rules, alerts.c.rule_id == alert_rules.c.id)
        )
        .where(telemetry_runs.c.project_id == project_id)
        .order_by(evaluations.c.evaluated_at.desc(), alerts.c.id)
    )
    if alert_state is not None:
        statement = statement.where(alerts.c.alert_status == alert_state.value)
    if search_query is not None:
        pattern = f"%{_escape_like(search_query)}%"
        statement = statement.where(
            or_(
                telemetry_runs.c.prompt_text.ilike(pattern, escape="\\"),
                telemetry_runs.c.output_text.ilike(pattern, escape="\\"),
                alert_rules.c.rule_name.ilike(pattern, escape="\\"),
                alerts.c.alert_status.ilike(pattern, escape="\\"),
                alerts.c.route_status.ilike(pattern, escape="\\"),
            )
        )
    return statement


@router.get("/alerts", response_model=AlertListResponse)
async def get_alerts(
    project: ProjectDependency,
    session: SessionDependency,
    baseline_resolver: Annotated[
        BaselineTextResolver,
        Depends(get_baseline_text_resolver),
    ],
    alert_state: Annotated[AlertState | None, Query(alias="status")] = None,
    q: Annotated[str | None, Query(min_length=1, max_length=200)] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0, le=1_000_000)] = 0,
) -> AlertListResponse:
    normalized_query = q.strip() if q is not None else None
    if q is not None and not normalized_query:
        raise HTTPException(
            status_code=422,
            detail="search query must not be blank",
        )
    try:
        result = await session.execute(
            _alerts_statement(project.id, alert_state, normalized_query)
            .limit(limit + 1)
            .offset(offset)
        )
    except SQLAlchemyError as exc:
        logger.error("alert feed query failed (%s)", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="alerts temporarily unavailable",
        ) from None

    rows = list(result.mappings())
    has_more = len(rows) > limit
    items = [AlertItem.model_validate(dict(row)) for row in rows[:limit]]
    baseline_ids = [
        item.matched_baseline_id
        for item in items
        if item.matched_baseline_id is not None
    ]
    baseline_texts = await baseline_resolver.resolve(project.id, baseline_ids)
    for item in items:
        if item.matched_baseline_id is not None:
            item.matched_baseline_text = baseline_texts.get(item.matched_baseline_id)
    return AlertListResponse(
        items=items,
        limit=limit,
        offset=offset,
        has_more=has_more,
    )


def _alert_rule_from_row(row) -> AlertRule:
    return AlertRule.model_validate(dict(row))


def _alert_rule_returning_columns():
    return (
        alert_rules.c.id,
        alert_rules.c.rule_name,
        alert_rules.c.threshold,
        alert_rules.c.action_type,
        alert_rules.c.notification_target,
        alert_rules.c.is_active,
        alert_rules.c.created_at,
    )


@router.get("/alert-rules", response_model=list[AlertRule])
async def get_alert_rules(
    project: ProjectDependency,
    session: SessionDependency,
) -> list[AlertRule]:
    try:
        result = await session.execute(
            select(
                alert_rules.c.id,
                alert_rules.c.rule_name,
                alert_rules.c.threshold,
                alert_rules.c.action_type,
                alert_rules.c.notification_target,
                alert_rules.c.is_active,
                alert_rules.c.created_at,
            )
            .where(alert_rules.c.project_id == project.id)
            .order_by(alert_rules.c.threshold, alert_rules.c.id)
        )
    except SQLAlchemyError as exc:
        logger.error("alert-rule query failed (%s)", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="alert rules temporarily unavailable",
        ) from None
    return [_alert_rule_from_row(row) for row in result.mappings()]


@router.post(
    "/alert-rules",
    response_model=AlertRule,
    status_code=status.HTTP_201_CREATED,
)
async def create_alert_rule(
    payload: AlertRuleCreate,
    project: ProjectDependency,
    session: SessionDependency,
    settings: SettingsDependency,
) -> AlertRule:
    _require_allowed_delivery_target(payload, settings)
    try:
        async with session.begin():
            result = await session.execute(
                insert(alert_rules)
                .values(project_id=project.id, **payload.model_dump())
                .returning(*_alert_rule_returning_columns())
            )
            row = result.mappings().one_or_none()
    except SQLAlchemyError as exc:
        logger.error("alert-rule creation failed (%s)", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="alert rule creation temporarily unavailable",
        ) from None

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="alert rule creation failed",
        )
    return _alert_rule_from_row(row)


@router.put("/alert-rules/{rule_id}", response_model=AlertRule)
async def update_alert_rule(
    rule_id: int,
    payload: AlertRuleUpdate,
    project: ProjectDependency,
    session: SessionDependency,
    settings: SettingsDependency,
) -> AlertRule:
    _require_allowed_delivery_target(payload, settings)
    try:
        async with session.begin():
            current_result = await session.execute(
                select(
                    alert_rules.c.action_type,
                    alert_rules.c.is_active,
                )
                .where(
                    alert_rules.c.id == rule_id,
                    alert_rules.c.project_id == project.id,
                )
                .with_for_update()
            )
            current = current_result.mappings().one_or_none()
            if current is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="alert rule not found",
                )

            result = await session.execute(
                update(alert_rules)
                .where(
                    alert_rules.c.id == rule_id,
                    alert_rules.c.project_id == project.id,
                )
                .values(**payload.model_dump())
                .returning(*_alert_rule_returning_columns())
            )
            row = result.mappings().one_or_none()

            pending_routes = alerts.c.rule_id == rule_id, alerts.c.route_status == "PENDING"
            if payload.action_type.value == "MUTE":
                await session.execute(
                    update(alerts)
                    .where(*pending_routes)
                    .values(
                        alert_status="SNOOZED",
                        route_status="SUPPRESSED",
                        route_due_at=None,
                        delivery_lease_until=None,
                        delivery_lease_token=None,
                    )
                )
            elif not payload.is_active:
                await session.execute(
                    update(alerts)
                    .where(*pending_routes)
                    .values(
                        route_status="SUPPRESSED",
                        route_due_at=None,
                        delivery_lease_until=None,
                        delivery_lease_token=None,
                    )
                )
            elif {
                str(current["action_type"]),
                payload.action_type.value,
            } == {"NOTIFY", "DIGEST"} and bool(current["is_active"]):
                due_at = (
                    func.now()
                    if payload.action_type.value == "NOTIFY"
                    else text(
                        "(date_trunc('day', NOW() AT TIME ZONE 'UTC') "
                        "+ INTERVAL '1 day') AT TIME ZONE 'UTC'"
                    )
                )
                await session.execute(
                    update(alerts)
                    .where(*pending_routes)
                    .values(
                        route_due_at=due_at,
                        delivery_lease_until=None,
                        delivery_lease_token=None,
                    )
                )
    except SQLAlchemyError as exc:
        logger.error("alert-rule update failed (%s)", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="alert rule update temporarily unavailable",
        ) from None

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="alert rule update failed",
        )
    return _alert_rule_from_row(row)
