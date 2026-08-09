from __future__ import annotations

from dataclasses import dataclass

import telemetry_simulator


@dataclass
class FakeResponse:
    status_code: int
    text: str = "{}"


def sample_payload() -> dict[str, object]:
    return {
        "session_id": "sess-test",
        "prompt_text": "prompt",
        "output_text": "output",
        "metadata": {"source": "test"},
    }


def test_send_payload_authenticates_without_logging_key(monkeypatch, capsys) -> None:
    calls: list[tuple[str, dict[str, object], dict[str, str], int]] = []

    def fake_post(url, json, headers, timeout):
        calls.append((url, json, headers, timeout))
        return FakeResponse(202, '{"run_id":"test"}')

    monkeypatch.setattr("requests.post", fake_post)

    assert telemetry_simulator.send_payload(
        "http://api:8000/api/v1/logs",
        sample_payload(),
        "dg_live_secret",
    )
    assert calls[0][2]["X-API-Key"] == "dg_live_secret"
    assert calls[0][3] == 5
    assert "dg_live_secret" not in capsys.readouterr().out


def test_send_payload_retries_retryable_responses_with_exponential_delays(monkeypatch) -> None:
    responses = iter([FakeResponse(503), FakeResponse(429), FakeResponse(202)])
    delays: list[int] = []

    monkeypatch.setattr("requests.post", lambda *args, **kwargs: next(responses))
    monkeypatch.setattr(telemetry_simulator.time, "sleep", delays.append)

    assert telemetry_simulator.send_payload(
        "http://api:8000/api/v1/logs",
        sample_payload(),
        "dg_live_secret",
        verbose=False,
    )
    assert delays == [2, 4]


def test_send_payload_does_not_retry_client_contract_errors(monkeypatch) -> None:
    attempts = 0

    def fake_post(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        return FakeResponse(422, "invalid payload")

    monkeypatch.setattr("requests.post", fake_post)

    assert not telemetry_simulator.send_payload(
        "http://api:8000/api/v1/logs",
        sample_payload(),
        "dg_live_secret",
        verbose=False,
    )
    assert attempts == 1
