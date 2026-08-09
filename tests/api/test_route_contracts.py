from __future__ import annotations

from app_api.main import create_app


def test_dashboard_and_ingestion_routes_have_frozen_http_contracts() -> None:
    schema = create_app().openapi()
    paths = schema["paths"]

    assert paths["/api/v1/logs"]["post"]["responses"].keys() >= {"202", "422"}
    assert "get" in paths["/api/v1/metrics/trends"]
    assert "get" in paths["/api/v1/alerts"]
    assert paths["/api/v1/alert-rules"]["post"]["responses"].keys() >= {
        "201",
        "422",
    }
    assert "get" in paths["/api/v1/alert-rules"]
    assert "put" in paths["/api/v1/alert-rules/{rule_id}"]
    assert "get" in paths["/api/v1/diagnostics/pulse"]
    assert "get" in paths["/api/v1/vectors/projection"]
    assert paths["/api/v1/dashboard/session"]["get"]["responses"].keys() >= {"204"}
    assert "get" in paths["/api/v1/retention/legal-holds"]
    assert paths["/api/v1/retention/legal-holds"]["post"]["responses"].keys() >= {"201"}
    assert paths["/api/v1/retention/legal-holds/{hold_id}"]["delete"][
        "responses"
    ].keys() >= {"204"}

    protected_operations = [
        paths["/api/v1/logs"]["post"],
        paths["/api/v1/metrics/trends"]["get"],
        paths["/api/v1/alerts"]["get"],
        paths["/api/v1/alert-rules"]["get"],
        paths["/api/v1/alert-rules"]["post"],
        paths["/api/v1/alert-rules/{rule_id}"]["put"],
        paths["/api/v1/diagnostics/pulse"]["get"],
        paths["/api/v1/vectors/projection"]["get"],
        paths["/api/v1/dashboard/session"]["get"],
        paths["/api/v1/retention/legal-holds"]["get"],
        paths["/api/v1/retention/legal-holds"]["post"],
        paths["/api/v1/retention/legal-holds/{hold_id}"]["delete"],
    ]
    assert all(
        operation["security"] == [{"APIKeyHeader": []}]
        for operation in protected_operations
    )
    admin_operations = protected_operations[1:]
    assert all(
        any(
            parameter.get("name") == "X-DriftGuard-Admin-Token"
            and parameter.get("in") == "header"
            for parameter in operation.get("parameters", [])
        )
        for operation in admin_operations
    )
    assert "security" not in paths["/healthz"]["get"]
    assert "security" not in paths["/status"]["get"]

    alert_parameters = {
        parameter["name"]: parameter
        for parameter in paths["/api/v1/alerts"]["get"]["parameters"]
    }
    q_string_schema = next(
        option
        for option in alert_parameters["q"]["schema"]["anyOf"]
        if option.get("type") == "string"
    )
    assert q_string_schema["maxLength"] == 200
    projection_parameters = {
        parameter["name"]: parameter
        for parameter in paths["/api/v1/vectors/projection"]["get"]["parameters"]
    }
    assert projection_parameters["limit"]["schema"]["maximum"] == 500


def test_response_models_expose_dashboard_field_names() -> None:
    schemas = create_app().openapi()["components"]["schemas"]

    assert schemas["TrendResponse"]["properties"].keys() >= {
        "window",
        "points",
        "thresholds",
        "summary",
    }
    assert schemas["TrendPoint"]["properties"].keys() >= {
        "timestamp",
        "average_drift",
        "p95_latency_ms",
        "evaluations",
        "anomalies",
    }
    assert schemas["AlertListResponse"]["properties"].keys() >= {
        "items",
        "limit",
        "offset",
        "has_more",
    }
    assert schemas["PulseResponse"]["properties"].keys() >= {
        "timestamp",
        "status",
        "services",
    }
    assert schemas["TrendSummary"]["properties"].keys() >= {
        "weighted_average_drift",
        "evaluated_run_count",
        "active_alert_count",
        "p95_evaluation_latency_ms",
        "average_end_to_end_latency_ms",
        "p95_end_to_end_latency_ms",
    }
    assert schemas["VectorProjectionResponse"]["properties"].keys() >= {
        "points",
        "count",
        "limit",
        "has_more",
    }
    assert schemas["VectorProjectionPoint"]["properties"].keys() == {
        "id",
        "point_type",
        "x",
        "y",
        "run_id",
        "baseline_set",
        "drift_distance",
        "matched_baseline_id",
    }
