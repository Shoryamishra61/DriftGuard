from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import SQLAlchemyError

from app_api.analytics import (
    create_alert_rule,
    get_alert_rules,
    get_alerts,
    get_trends,
    update_alert_rule,
)
from app_api.auth import AuthenticatedProject
from app_api.config import Settings
from app_api.schemas import AlertAction, AlertRuleCreate, AlertRuleUpdate, TrendWindow

from .fakes import FakeResult, FakeSession

NOW = datetime(2026, 8, 9, tzinfo=timezone.utc)  # noqa: UP017 -- Python 3.10
SETTINGS = Settings(webhook_allowed_hosts_csv="example.com")


class BaselineResolver:
    async def resolve(self, project_id, baseline_ids):
        del project_id
        return {baseline_id: "Expected baseline answer" for baseline_id in baseline_ids}


@pytest.mark.asyncio
async def test_trends_are_project_scoped_and_include_real_thresholds() -> None:
    project = AuthenticatedProject(id=uuid4())
    session = FakeSession(
        results=[
            FakeResult(
                rows=[
                    {
                        "timestamp": NOW,
                        "average_drift": 0.27,
                        "p95_latency_ms": 44.5,
                        "evaluations": 12,
                        "anomalies": 3,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "rule_name": "Warning",
                        "action_type": "DIGEST",
                        "threshold": 0.3,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "weighted_average_drift": 0.24,
                        "evaluated_run_count": 42,
                        "p95_evaluation_latency_ms": 51.0,
                        "average_end_to_end_latency_ms": 72.0,
                        "p95_end_to_end_latency_ms": 110.0,
                    }
                ]
            ),
            FakeResult(scalar=5),
        ]
    )

    response = await get_trends(
        project=project,
        session=session,
        window=TrendWindow.DAY,
    )

    assert response.points[0].average_drift == 0.27
    assert response.thresholds[0].action_type is AlertAction.DIGEST
    assert response.summary.weighted_average_drift == 0.24
    assert response.summary.evaluated_run_count == 42
    assert response.summary.active_alert_count == 5
    assert response.summary.p95_evaluation_latency_ms == 51.0
    assert response.summary.average_end_to_end_latency_ms == 72.0
    assert response.summary.p95_end_to_end_latency_ms == 110.0
    compiled = session.statements[0].compile(dialect=postgresql.dialect())
    assert project.id in compiled.params.values()
    assert compiled.params["window_hours"] == 24
    summary_sql = str(session.statements[2].compile(dialect=postgresql.dialect()))
    assert "percentile_cont" in summary_sql
    assert "evaluations.evaluated_at - telemetry_runs.ingested_at" in summary_sql
    active_count_sql = str(session.statements[3].compile(dialect=postgresql.dialect()))
    assert "alerts.alert_status =" in active_count_sql
    assert "TRIGGERED" in session.statements[3].compile(
        dialect=postgresql.dialect()
    ).params.values()
    assert "LIMIT" not in active_count_sql


def test_trend_windows_use_hour_day_and_week_granularity() -> None:
    assert TrendWindow.DAY.bucket == "hour"
    assert TrendWindow.WEEK.bucket == "day"
    assert TrendWindow.MONTH.bucket == "week"


@pytest.mark.asyncio
async def test_alert_feed_returns_only_requested_page_shape() -> None:
    project = AuthenticatedProject(id=uuid4())
    row = {
        "id": uuid4(),
        "evaluation_id": uuid4(),
        "rule_id": 7,
        "rule_name": "Critical",
        "action_type": "NOTIFY",
        "status": "TRIGGERED",
        "notified_at": None,
        "route_status": "PENDING",
        "route_due_at": None,
        "delivery_lease_until": None,
        "delivery_attempts": 1,
        "run_id": uuid4(),
        "session_id": "sess-1",
        "prompt_text": "prompt",
        "output_text": "output",
        "drift_distance": 0.6,
        "matched_baseline_id": uuid4(),
        "evaluated_at": NOW,
    }
    session = FakeSession(results=[FakeResult(rows=[row])])

    response = await get_alerts(
        project=project,
        session=session,
        baseline_resolver=BaselineResolver(),
        alert_state=None,
        q=None,
        limit=10,
        offset=0,
    )

    assert response.items[0].run_id == row["run_id"]
    assert response.items[0].notified_at is None
    assert response.items[0].route_status == "PENDING"
    assert response.items[0].matched_baseline_text == "Expected baseline answer"
    assert response.has_more is False
    compiled = session.statements[0].compile(dialect=postgresql.dialect())
    assert project.id in compiled.params.values()
    compiled_sql = str(compiled)
    assert "evaluations.evaluated_at DESC" in compiled_sql
    assert "alerts.notified_at DESC" not in compiled_sql


@pytest.mark.asyncio
async def test_alert_search_is_tenant_scoped_bounded_and_parameterized() -> None:
    project = AuthenticatedProject(id=uuid4())
    session = FakeSession(results=[FakeResult()])
    untrusted_query = "incident%_') OR TRUE --"

    response = await get_alerts(
        project=project,
        session=session,
        baseline_resolver=BaselineResolver(),
        alert_state=None,
        q=untrusted_query,
        limit=20,
        offset=40,
    )

    assert response.items == []
    assert response.offset == 40
    compiled = session.statements[0].compile(dialect=postgresql.dialect())
    compiled_sql = str(compiled)
    assert untrusted_query not in compiled_sql
    assert project.id in compiled.params.values()
    assert "%incident\\%\\_') OR TRUE --%" in compiled.params.values()
    assert compiled_sql.count("ILIKE") == 5
    assert "telemetry_runs.prompt_text" in compiled_sql
    assert "telemetry_runs.output_text" in compiled_sql
    assert "alert_rules.rule_name" in compiled_sql
    assert "alerts.alert_status" in compiled_sql
    assert "alerts.route_status" in compiled_sql


@pytest.mark.asyncio
async def test_alert_search_database_failure_returns_safe_service_error() -> None:
    project = AuthenticatedProject(id=uuid4())
    session = FakeSession(
        failure=SQLAlchemyError("bound parameter must not leak"),
        fail_on_execute=1,
    )

    with pytest.raises(HTTPException) as captured:
        await get_alerts(
            project=project,
            session=session,
            baseline_resolver=BaselineResolver(),
            alert_state=None,
            q="incident",
            limit=20,
            offset=0,
        )

    assert getattr(captured.value, "status_code", None) == 503
    assert getattr(captured.value, "detail", None) == "alerts temporarily unavailable"
    assert "bound parameter" not in str(getattr(captured.value, "detail", ""))


@pytest.mark.asyncio
async def test_rule_update_is_tenant_scoped_and_transactional() -> None:
    project = AuthenticatedProject(id=uuid4())
    result_row = {
        "id": 4,
        "rule_name": "Critical",
        "threshold": 0.45,
        "action_type": "NOTIFY",
        "notification_target": "https://example.com/drift",
        "is_active": True,
        "created_at": NOW,
    }
    session = FakeSession(
        results=[
            FakeResult(rows=[{"action_type": "NOTIFY", "is_active": True}]),
            FakeResult(rows=[result_row]),
        ]
    )
    payload = AlertRuleUpdate(
        rule_name="Critical",
        threshold=0.45,
        action_type="NOTIFY",
        notification_target="https://example.com/drift",
        is_active=True,
    )

    response = await update_alert_rule(
        rule_id=4,
        payload=payload,
        project=project,
        session=session,
        settings=SETTINGS,
    )

    assert response.id == 4
    assert session.events[-1] == "transaction.commit"
    compiled = session.statements[1].compile(dialect=postgresql.dialect())
    assert project.id in compiled.params.values()
    assert 4 in compiled.params.values()


@pytest.mark.asyncio
async def test_rule_deactivation_suppresses_only_pending_routes_in_same_transaction() -> None:
    project = AuthenticatedProject(id=uuid4())
    result_row = {
        "id": 4,
        "rule_name": "Critical",
        "threshold": 0.45,
        "action_type": "NOTIFY",
        "notification_target": "https://example.com/drift",
        "is_active": False,
        "created_at": NOW,
    }
    session = FakeSession(
        results=[
            FakeResult(rows=[{"action_type": "NOTIFY", "is_active": True}]),
            FakeResult(rows=[result_row]),
            FakeResult(),
        ]
    )
    payload = AlertRuleUpdate(
        rule_name="Critical",
        threshold=0.45,
        action_type="NOTIFY",
        notification_target="https://example.com/drift",
        is_active=False,
    )

    await update_alert_rule(
        rule_id=4,
        payload=payload,
        project=project,
        session=session,
        settings=SETTINGS,
    )

    assert session.events[0] == "transaction.begin"
    assert session.events[-1] == "transaction.commit"
    route_update = session.statements[2].compile(dialect=postgresql.dialect())
    assert route_update.params["route_status"] == "SUPPRESSED"
    assert "SNOOZED" not in route_update.params.values()
    assert "PENDING" in route_update.params.values()
    assert route_update.params["route_due_at"] is None
    assert route_update.params["delivery_lease_until"] is None
    assert route_update.params["delivery_lease_token"] is None


@pytest.mark.asyncio
async def test_rule_digest_to_notify_reschedules_only_pending_routes() -> None:
    project = AuthenticatedProject(id=uuid4())
    result_row = {
        "id": 4,
        "rule_name": "Immediate",
        "threshold": 0.45,
        "action_type": "NOTIFY",
        "notification_target": "https://example.com/drift",
        "is_active": True,
        "created_at": NOW,
    }
    session = FakeSession(
        results=[
            FakeResult(rows=[{"action_type": "DIGEST", "is_active": True}]),
            FakeResult(rows=[result_row]),
            FakeResult(),
        ]
    )
    payload = AlertRuleUpdate(
        rule_name="Immediate",
        threshold=0.45,
        action_type="NOTIFY",
        notification_target="https://example.com/drift",
        is_active=True,
    )

    await update_alert_rule(
        rule_id=4,
        payload=payload,
        project=project,
        session=session,
        settings=SETTINGS,
    )

    route_update = session.statements[2].compile(dialect=postgresql.dialect())
    assert "now()" in str(route_update).lower()
    assert "PENDING" in route_update.params.values()
    assert "SUPPRESSED" not in route_update.params.values()
    assert "FAILED" not in route_update.params.values()
    assert "DELIVERED" not in route_update.params.values()
    assert route_update.params["delivery_lease_until"] is None
    assert route_update.params["delivery_lease_token"] is None


@pytest.mark.asyncio
async def test_rule_action_change_to_mute_suppresses_pending_routes() -> None:
    project = AuthenticatedProject(id=uuid4())
    result_row = {
        "id": 4,
        "rule_name": "Silent",
        "threshold": 0.45,
        "action_type": "MUTE",
        "notification_target": "muted",
        "is_active": True,
        "created_at": NOW,
    }
    session = FakeSession(
        results=[
            FakeResult(rows=[{"action_type": "NOTIFY", "is_active": True}]),
            FakeResult(rows=[result_row]),
            FakeResult(),
        ]
    )
    payload = AlertRuleUpdate(
        rule_name="Silent",
        threshold=0.45,
        action_type="MUTE",
        notification_target="muted",
        is_active=True,
    )

    await update_alert_rule(
        rule_id=4,
        payload=payload,
        project=project,
        session=session,
        settings=SETTINGS,
    )

    route_update = session.statements[2].compile(dialect=postgresql.dialect())
    assert route_update.params["alert_status"] == "SNOOZED"
    assert route_update.params["route_status"] == "SUPPRESSED"
    assert route_update.params["delivery_lease_token"] is None
    assert "PENDING" in route_update.params.values()
    assert route_update.params["route_due_at"] is None


@pytest.mark.asyncio
async def test_rule_notify_to_digest_reschedules_for_next_utc_day() -> None:
    project = AuthenticatedProject(id=uuid4())
    result_row = {
        "id": 4,
        "rule_name": "Daily",
        "threshold": 0.45,
        "action_type": "DIGEST",
        "notification_target": "mailto:alerts@example.com",
        "is_active": True,
        "created_at": NOW,
    }
    session = FakeSession(
        results=[
            FakeResult(rows=[{"action_type": "NOTIFY", "is_active": True}]),
            FakeResult(rows=[result_row]),
            FakeResult(),
        ]
    )
    payload = AlertRuleUpdate(
        rule_name="Daily",
        threshold=0.45,
        action_type="DIGEST",
        notification_target="mailto:alerts@example.com",
        is_active=True,
    )

    await update_alert_rule(
        rule_id=4,
        payload=payload,
        project=project,
        session=session,
        settings=SETTINGS,
    )

    route_update_sql = str(
        session.statements[2].compile(dialect=postgresql.dialect())
    )
    assert "date_trunc('day', NOW() AT TIME ZONE 'UTC')" in route_update_sql
    assert "INTERVAL '1 day'" in route_update_sql


@pytest.mark.asyncio
async def test_rule_creation_assigns_authenticated_project_and_commits() -> None:
    project = AuthenticatedProject(id=uuid4())
    result_row = {
        "id": 9,
        "rule_name": "Warning digest",
        "threshold": 0.3,
        "action_type": "DIGEST",
        "notification_target": "https://example.com/digest",
        "is_active": True,
        "created_at": NOW,
    }
    session = FakeSession(results=[FakeResult(rows=[result_row])])
    payload = AlertRuleCreate(
        rule_name="Warning digest",
        threshold=0.3,
        action_type="DIGEST",
        notification_target="https://example.com/digest",
        is_active=True,
    )

    response = await create_alert_rule(
        payload=payload,
        project=project,
        session=session,
        settings=SETTINGS,
    )

    assert response.id == 9
    assert session.events[-1] == "transaction.commit"
    params = session.statements[0].compile(dialect=postgresql.dialect()).params
    assert params["project_id"] == project.id
    assert params["action_type"] == "DIGEST"


@pytest.mark.asyncio
async def test_rule_creation_rejects_unapproved_generic_webhook_before_database_write() -> None:
    project = AuthenticatedProject(id=uuid4())
    session = FakeSession()
    payload = AlertRuleCreate(
        rule_name="Critical",
        threshold=0.45,
        action_type="NOTIFY",
        notification_target="https://unapproved.example.net/drift",
        is_active=True,
    )

    with pytest.raises(HTTPException) as captured:
        await create_alert_rule(
            payload=payload,
            project=project,
            session=session,
            settings=Settings(),
        )

    assert captured.value.status_code == 422
    assert captured.value.detail == (
        "notification target is not an approved delivery destination"
    )
    assert session.statements == []


@pytest.mark.asyncio
async def test_rule_list_maps_database_rows_without_mock_defaults() -> None:
    project = AuthenticatedProject(id=uuid4())
    session = FakeSession(
        results=[
            FakeResult(
                rows=[
                    {
                        "id": 1,
                        "rule_name": "Digest",
                        "threshold": 0.3,
                        "action_type": "DIGEST",
                        "notification_target": "https://example.com/digest",
                        "is_active": True,
                        "created_at": NOW,
                    }
                ]
            )
        ]
    )

    rules = await get_alert_rules(project=project, session=session)

    assert len(rules) == 1
    assert rules[0].rule_name == "Digest"
