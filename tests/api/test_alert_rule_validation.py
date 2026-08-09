from __future__ import annotations

import pytest
from pydantic import ValidationError

from app_api.schemas import AlertRuleCreate, enforce_delivery_target_policy


@pytest.mark.parametrize(
    "target",
    [
        "http://example.com/hook",
        "https://user:password@example.com/hook",
        "https://127.0.0.1/hook",
        "https://10.0.0.4/hook",
        "https://224.0.0.1/hook",
        "https://[ff02::1]/hook",
        "https://hooks.example.invalid/hook",
        "not-a-url",
    ],
)
def test_notify_rejects_targets_worker_cannot_deliver_safely(target: str) -> None:
    with pytest.raises(ValidationError):
        AlertRuleCreate(
            rule_name="critical",
            threshold=0.45,
            action_type="NOTIFY",
            notification_target=target,
            is_active=True,
        )


def test_https_webhook_and_mute_target_are_accepted() -> None:
    notify = AlertRuleCreate(
        rule_name="critical",
        threshold=0.45,
        action_type="NOTIFY",
        notification_target="https://example.com/driftguard",
        is_active=True,
    )
    mute = AlertRuleCreate(
        rule_name="silent",
        threshold=0.15,
        action_type="MUTE",
        notification_target="muted",
        is_active=True,
    )
    assert notify.notification_target.startswith("https://")
    assert mute.notification_target == "muted"


def test_digest_accepts_one_strict_mailto_recipient() -> None:
    digest = AlertRuleCreate(
        rule_name="daily digest",
        threshold=0.2,
        action_type="DIGEST",
        notification_target="mailto:alerts+drift@example.com",
        is_active=True,
    )

    assert digest.notification_target == "mailto:alerts+drift@example.com"


@pytest.mark.parametrize(
    "target",
    [
        "mailto:one@example.com,two@example.com",
        "mailto:one@example.com?subject=Injected",
        "mailto:one@example.com%0ABcc:other@example.com",
        "mailto://one@example.com",
        "mailto:missing-domain@localhost",
        "mailto:.bad@example.com",
    ],
)
def test_digest_rejects_ambiguous_or_header_injecting_mailto_targets(target: str) -> None:
    with pytest.raises(ValidationError):
        AlertRuleCreate(
            rule_name="daily digest",
            threshold=0.2,
            action_type="DIGEST",
            notification_target=target,
            is_active=True,
        )


def test_notify_does_not_accept_mailto_target() -> None:
    with pytest.raises(ValidationError):
        AlertRuleCreate(
            rule_name="immediate",
            threshold=0.4,
            action_type="NOTIFY",
            notification_target="mailto:alerts@example.com",
            is_active=True,
        )


@pytest.mark.parametrize(
    "target",
    [
        "https://hooks.slack.com/services/T123/B456/secret-token",
        "https://discord.com/api/webhooks/1234567890/secret.token-value",
        "pagerduty://0123456789abcdef0123456789ABCDEF",
    ],
)
def test_notify_accepts_strict_native_adapter_targets(target: str) -> None:
    rule = AlertRuleCreate(
        rule_name="critical",
        threshold=0.45,
        action_type="NOTIFY",
        notification_target=target,
        is_active=True,
    )

    enforce_delivery_target_policy(rule, ())


@pytest.mark.parametrize(
    "target",
    [
        "https://hooks.slack.com/not-services/T123/B456/token",
        "https://hooks.slack.com/services/T123/B456/token?redirect=true",
        "https://discord.com/api/webhooks/not-numeric/token",
        "https://discordapp.com/api/webhooks/123456/token",
        "pagerduty://too-short",
        "pagerduty://0123456789abcdef0123456789abcdef/path",
    ],
)
def test_native_adapter_targets_reject_ambiguous_formats(target: str) -> None:
    with pytest.raises(ValidationError):
        AlertRuleCreate(
            rule_name="critical",
            threshold=0.45,
            action_type="NOTIFY",
            notification_target=target,
            is_active=True,
        )


def test_pagerduty_target_is_not_valid_for_digest() -> None:
    with pytest.raises(ValidationError):
        AlertRuleCreate(
            rule_name="daily",
            threshold=0.3,
            action_type="DIGEST",
            notification_target="pagerduty://0123456789abcdef0123456789abcdef",
            is_active=True,
        )


def test_generic_https_requires_configured_host_and_allows_its_subdomains() -> None:
    rule = AlertRuleCreate(
        rule_name="critical",
        threshold=0.45,
        action_type="NOTIFY",
        notification_target="https://events.alerts.example.com/driftguard",
        is_active=True,
    )

    enforce_delivery_target_policy(rule, ("example.com",))
    with pytest.raises(ValueError):
        enforce_delivery_target_policy(rule, ())
    with pytest.raises(ValueError):
        enforce_delivery_target_policy(rule, ("example.com.attacker.invalid",))
