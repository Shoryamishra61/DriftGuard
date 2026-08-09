import json
from uuid import uuid4

import httpx
import pytest

from app_worker.domain import (
    ActionType,
    Alert,
    AlertRule,
    AlertStatus,
    DeliveryItem,
    Evaluation,
    TelemetryRun,
)
from app_worker.notifications import WebhookDeliveryError, WebhookSender


def notification_records(target: str):
    project_id = uuid4()
    run = TelemetryRun(
        uuid4(),
        project_id,
        "private output evidence",
        prompt_text="private prompt evidence",
    )
    rule = AlertRule(1, project_id, "critical", 0.3, ActionType.NOTIFY, target)
    evaluation = Evaluation(uuid4(), run.id, 0.6, uuid4(), 42, True)
    alert = Alert(uuid4(), evaluation.id, rule, AlertStatus.SNOOZED, True)
    return run, evaluation, alert


@pytest.mark.asyncio
async def test_notify_posts_bounded_evidence_with_idempotency_key() -> None:
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sender = WebhookSender(client=client, allowed_hosts=("8.8.8.8",))
    run, evaluation, alert = notification_records("https://8.8.8.8/driftguard")

    await sender.send(alert=alert, evaluation=evaluation, run=run)
    await client.aclose()

    assert len(requests) == 1
    assert requests[0].headers["Idempotency-Key"] == str(alert.id)
    payload = json.loads(requests[0].content)
    assert payload["run_id"] == str(run.id)
    assert payload["project_id"] == str(run.project_id)
    assert payload["evaluation"]["drift_distance"] == pytest.approx(0.6)
    assert payload["evidence"] == {
        "prompt_excerpt": "private prompt evidence",
        "output_excerpt": "private output evidence",
    }
    assert "prompt_text" not in payload
    assert "output_text" not in payload


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target",
    [
        "http://hooks.example.com/notify",
        "https://127.0.0.1/notify",
        "https://10.1.2.3/notify",
        "https://user:password@8.8.8.8/notify",
        "https://8.8.8.8:invalid/notify",
    ],
)
async def test_notify_rejects_unsafe_webhook_targets(target: str) -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: None))
    sender = WebhookSender(client=client)
    run, evaluation, alert = notification_records(target)

    with pytest.raises(WebhookDeliveryError):
        await sender.send(alert=alert, evaluation=evaluation, run=run)
    await client.aclose()


@pytest.mark.asyncio
async def test_notify_retries_transient_status_with_bounded_backoff() -> None:
    attempts = 0
    sleeps = []

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(503 if attempts == 1 else 202)

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sender = WebhookSender(
        client=client,
        max_attempts=3,
        sleep=record_sleep,
        allowed_hosts=("8.8.8.8",),
    )
    run, evaluation, alert = notification_records("https://8.8.8.8/driftguard")

    await sender.send(alert=alert, evaluation=evaluation, run=run)
    await client.aclose()

    assert attempts == 2
    assert sleeps == [1.0]


def digest_records(target: str, *, count: int = 2):
    project_id = uuid4()
    rule = AlertRule(2, project_id, "daily", 0.3, ActionType.DIGEST, target)
    items = []
    for _index in range(count):
        run = TelemetryRun(
            uuid4(),
            project_id,
            "private output",
            prompt_text="private prompt",
        )
        evaluation = Evaluation(uuid4(), run.id, 0.6, uuid4(), 42, True)
        alert = Alert(uuid4(), evaluation.id, rule, AlertStatus.TRIGGERED, True)
        items.append(DeliveryItem(alert, evaluation, run))
    return items


@pytest.mark.asyncio
async def test_digest_mailto_contains_bounded_prompt_and_output_evidence() -> None:
    deliveries = []

    def capture(message, recipient):
        deliveries.append((message, recipient))

    sender = WebhookSender(
        smtp_host="smtp.example.com",
        smtp_from_address="alerts@example.com",
        smtp_send=capture,
    )
    items = digest_records("mailto:ops@example.com")

    await sender.send_digest(items)
    await sender.close()

    assert len(deliveries) == 1
    message, recipient = deliveries[0]
    assert recipient == "ops@example.com"
    assert message["To"] == "ops@example.com"
    assert message["X-DriftGuard-Idempotency-Key"]
    body = message.get_content()
    assert '"count": 2' in body
    assert '"prompt_excerpt": "private prompt"' in body
    assert '"output_excerpt": "private output"' in body


@pytest.mark.asyncio
async def test_generic_webhook_requires_explicit_allowlist() -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: httpx.Response(204)))
    sender = WebhookSender(client=client)
    run, evaluation, alert = notification_records("https://8.8.8.8/driftguard")

    with pytest.raises(WebhookDeliveryError, match="WEBHOOK_ALLOWED_HOSTS"):
        await sender.send(alert=alert, evaluation=evaluation, run=run)
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("resolved_address", ["224.0.0.1", "ff02::1", "fec0::1"])
async def test_generic_webhook_rejects_non_unicast_resolution(
    resolved_address: str,
    monkeypatch,
) -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: None))
    sender = WebhookSender(client=client, allowed_hosts=("hooks.example.com",))

    async def unsafe_address(_hostname, _port):
        import ipaddress

        return [ipaddress.ip_address(resolved_address)]

    monkeypatch.setattr(sender, "_resolve_addresses", unsafe_address)

    with pytest.raises(WebhookDeliveryError, match="private or non-routable"):
        await sender._validate_and_bind_target(
            "https://hooks.example.com/driftguard",
            official_provider=False,
        )
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target", "provider_field"),
    [
        ("https://hooks.slack.com/services/T123/B456/secret", "blocks"),
        ("https://discord.com/api/webhooks/123456/secret", "allowed_mentions"),
    ],
)
async def test_official_provider_urls_get_provider_payloads_and_dns_pinning(
    target,
    provider_field,
    monkeypatch,
) -> None:
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(204)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sender = WebhookSender(client=client)

    async def public_address(_hostname, _port):
        import ipaddress

        return [ipaddress.ip_address("8.8.8.8")]

    monkeypatch.setattr(sender, "_resolve_addresses", public_address)
    run, evaluation, alert = notification_records(target)

    await sender.send(alert=alert, evaluation=evaluation, run=run)
    await client.aclose()

    assert len(requests) == 1
    assert requests[0].url.host == "8.8.8.8"
    assert requests[0].headers["Host"] == url_host(target)
    assert requests[0].extensions["sni_hostname"] == url_host(target)
    assert provider_field in json.loads(requests[0].content)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target", "content_key", "limit"),
    [
        ("https://hooks.slack.com/services/T123/B456/secret", "text", 2900),
        ("https://discord.com/api/webhooks/123456/secret", "content", 1900),
    ],
)
async def test_provider_digest_contains_bounded_drift_entries(
    target,
    content_key,
    limit,
    monkeypatch,
) -> None:
    requests = []
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: requests.append(request) or httpx.Response(204)
        )
    )
    sender = WebhookSender(client=client)

    async def public_address(_hostname, _port):
        import ipaddress

        return [ipaddress.ip_address("8.8.8.8")]

    monkeypatch.setattr(sender, "_resolve_addresses", public_address)
    items = digest_records(target, count=30)

    await sender.send_digest(items)
    await client.aclose()

    content = json.loads(requests[0].content)[content_key]
    provider_payload = json.loads(requests[0].content)
    assert any(str(item.run.id) in content for item in items)
    assert "distance=0.6" in content
    assert "input: private prompt" in content
    assert "output: private output" in content
    assert "additional alerts" in content
    assert len(content) <= limit
    if content_key == "text":
        assert provider_payload["blocks"]


@pytest.mark.asyncio
async def test_generic_digest_caps_count_and_excerpt_lengths() -> None:
    requests = []
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: requests.append(request) or httpx.Response(204)
        )
    )
    sender = WebhookSender(client=client, allowed_hosts=("8.8.8.8",))
    items = digest_records("https://8.8.8.8/digest", count=30)
    long_prompt = "  prompt\n" + ("p" * 500)
    long_output = " output\t" + ("o" * 500)
    items[0] = DeliveryItem(
        items[0].alert,
        items[0].evaluation,
        TelemetryRun(
            items[0].run.id,
            items[0].run.project_id,
            long_output,
            prompt_text=long_prompt,
        ),
    )

    await sender.send_digest(items, total_count=30)
    await client.aclose()

    payload = json.loads(requests[0].content)
    assert payload["count"] == 30
    assert payload["evidence_count"] == 20
    assert payload["omitted_count"] == 10
    assert len(payload["evaluations"]) == 20
    first = payload["evaluations"][0]
    assert len(first["prompt_excerpt"]) == 240
    assert len(first["output_excerpt"]) == 240
    assert "\n" not in first["prompt_excerpt"]
    assert "\t" not in first["output_excerpt"]
    assert first["prompt_excerpt"].endswith("…")
    assert first["output_excerpt"].endswith("…")


@pytest.mark.asyncio
async def test_pagerduty_target_uses_official_events_v2_payload(monkeypatch) -> None:
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(202)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sender = WebhookSender(client=client)

    async def public_address(_hostname, _port):
        import ipaddress

        return [ipaddress.ip_address("8.8.8.8")]

    monkeypatch.setattr(sender, "_resolve_addresses", public_address)
    routing_key = "A" * 32
    run, evaluation, alert = notification_records(f"pagerduty://{routing_key}")

    await sender.send(alert=alert, evaluation=evaluation, run=run)
    await client.aclose()

    payload = json.loads(requests[0].content)
    assert requests[0].headers["Host"] == "events.pagerduty.com"
    assert payload["routing_key"] == routing_key
    assert payload["event_action"] == "trigger"
    assert payload["dedup_key"] == str(alert.id)
    assert payload["payload"]["custom_details"]["prompt_excerpt"] == ("private prompt evidence")
    assert payload["payload"]["custom_details"]["output_excerpt"] == ("private output evidence")


def url_host(target: str) -> str:
    from urllib.parse import urlsplit

    return urlsplit(target).hostname
