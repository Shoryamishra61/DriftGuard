"""SSRF-resistant, bounded webhook delivery for immediate alerts."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import re
import smtplib
import socket
import ssl
import unicodedata
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from email.message import EmailMessage
from urllib.parse import unquote, urlsplit

import httpx

from common_utils.network import is_public_unicast_address

from .domain import Alert, DeliveryItem, Evaluation, TelemetryRun

UTC = getattr(__import__("datetime"), "UTC", timezone.utc)  # noqa: UP017
EMAIL_LOCAL_PATTERN = re.compile(r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+$")
PAGERDUTY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9]{32}$")
PAGERDUTY_EVENTS_URL = "https://events.pagerduty.com/v2/enqueue"
SLACK_HOST = "hooks.slack.com"
DISCORD_HOST = "discord.com"
MAX_DIGEST_EVIDENCE = 20
MAX_EVIDENCE_EXCERPT_CHARACTERS = 240


class WebhookDeliveryError(RuntimeError):
    """Raised when an immediate notification cannot be delivered safely."""


class WebhookSender:
    def __init__(
        self,
        *,
        timeout_seconds: float = 5.0,
        max_attempts: int = 3,
        allowed_hosts: tuple[str, ...] = (),
        smtp_host: str = "",
        smtp_port: int = 587,
        smtp_username: str = "",
        smtp_password: str = "",
        smtp_from_address: str = "",
        smtp_security: str = "starttls",
        smtp_timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        smtp_send: Callable[[EmailMessage, str], None] | None = None,
    ) -> None:
        self._allowed_hosts = tuple(host.lower().rstrip(".") for host in allowed_hosts)
        self._max_attempts = max_attempts
        self._sleep = sleep
        self._smtp_host = smtp_host.strip()
        self._smtp_port = smtp_port
        self._smtp_username = smtp_username
        self._smtp_password = smtp_password
        self._smtp_from_address = smtp_from_address.strip()
        self._smtp_security = smtp_security
        self._smtp_timeout_seconds = smtp_timeout_seconds
        self._smtp_send = smtp_send or self._send_smtp_message
        if smtp_security not in {"starttls", "tls"}:
            raise ValueError("SMTP security must be starttls or tls")
        if not 10 <= smtp_port <= 65435:
            raise ValueError("SMTP port must be between 10 and 65435")
        if bool(smtp_username) != bool(smtp_password):
            raise ValueError("SMTP username and password must be configured together")
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=False,
            trust_env=False,
        )

    async def send(
        self,
        *,
        alert: Alert,
        evaluation: Evaluation,
        run: TelemetryRun,
        idempotency_key: str | None = None,
    ) -> None:
        target = alert.rule.notification_target
        payload = {
            "event": "driftguard.semantic_drift",
            "alert_id": str(alert.id),
            "project_id": str(run.project_id),
            "run_id": str(run.id),
            "rule": {
                "id": alert.rule.id,
                "name": alert.rule.rule_name,
                "threshold": alert.rule.threshold,
            },
            "evaluation": {
                "drift_distance": evaluation.drift_distance,
                "matched_baseline_id": (
                    str(evaluation.matched_baseline_id)
                    if evaluation.matched_baseline_id is not None
                    else None
                ),
                "latency_ms": evaluation.evaluation_latency_ms,
            },
            "evidence": {
                "prompt_excerpt": self._evidence_excerpt(run.prompt_text),
                "output_excerpt": self._evidence_excerpt(run.output_text),
            },
            "observed_at": datetime.now(UTC).isoformat(),
        }
        await self._post(
            target=target,
            payload=payload,
            event_name="semantic_drift",
            idempotency_key=idempotency_key or str(alert.id),
        )

    async def send_digest(
        self,
        items: list[DeliveryItem],
        *,
        total_count: int | None = None,
        digest_day: str | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        if not items:
            raise ValueError("digest delivery requires at least one alert")
        all_items = list(items)
        first = all_items[0]
        rule = first.alert.rule
        if any(
            item.alert.rule.id != rule.id
            or item.alert.rule.notification_target != rule.notification_target
            or item.run.project_id != first.run.project_id
            for item in all_items
        ):
            raise ValueError("digest items must share one project, rule, and target")

        ordered = all_items[:MAX_DIGEST_EVIDENCE]
        digest_identity = ":".join(sorted(str(item.alert.id) for item in all_items))
        idempotency_key = (
            idempotency_key or hashlib.sha256(digest_identity.encode("ascii")).hexdigest()
        )
        total_count = len(all_items) if total_count is None else total_count
        if total_count < len(all_items):
            raise ValueError("digest total_count cannot be smaller than its evidence")
        payload = {
            "event": "driftguard.semantic_drift_digest",
            "project_id": str(first.run.project_id),
            "rule": {
                "id": rule.id,
                "name": rule.rule_name,
                "threshold": rule.threshold,
            },
            "count": total_count,
            "evidence_count": len(ordered),
            "omitted_count": total_count - len(ordered),
            "digest_day": digest_day,
            "evaluations": [
                {
                    "alert_id": str(item.alert.id),
                    "run_id": str(item.run.id),
                    "drift_distance": item.evaluation.drift_distance,
                    "matched_baseline_id": (
                        str(item.evaluation.matched_baseline_id)
                        if item.evaluation.matched_baseline_id is not None
                        else None
                    ),
                    "prompt_excerpt": self._evidence_excerpt(item.run.prompt_text),
                    "output_excerpt": self._evidence_excerpt(item.run.output_text),
                }
                for item in ordered
            ],
            "generated_at": datetime.now(UTC).isoformat(),
            "digest_id": idempotency_key,
        }
        target = rule.notification_target
        if urlsplit(target).scheme.lower() == "mailto":
            await self._email_digest(
                target=target,
                payload=payload,
                idempotency_key=idempotency_key,
            )
        else:
            if urlsplit(target).scheme.lower() == "pagerduty":
                raise WebhookDeliveryError("PagerDuty targets are only valid for NOTIFY")
            await self._post(
                target=target,
                payload=payload,
                event_name="semantic_drift_digest",
                idempotency_key=idempotency_key,
            )

    async def _email_digest(
        self,
        *,
        target: str,
        payload: dict,
        idempotency_key: str,
    ) -> None:
        recipient = self._mailto_recipient(target)
        if not self._smtp_host or not self._smtp_from_address:
            raise WebhookDeliveryError("SMTP delivery is not configured")
        sender = self._email_address(self._smtp_from_address)
        message = EmailMessage()
        message["From"] = sender
        message["To"] = recipient
        message["Subject"] = f"DriftGuard daily semantic drift digest ({payload['count']} alerts)"
        message["Message-ID"] = f"<{idempotency_key}@driftguard.local>"
        message["X-DriftGuard-Idempotency-Key"] = idempotency_key
        message.set_content(json.dumps(payload, indent=2, sort_keys=True))

        last_failure = "unknown SMTP failure"
        for attempt in range(1, self._max_attempts + 1):
            try:
                await asyncio.to_thread(self._smtp_send, message, recipient)
                return
            except (OSError, smtplib.SMTPException, ssl.SSLError) as exc:
                last_failure = f"SMTP transport failed with {type(exc).__name__}"
            if attempt < self._max_attempts:
                await self._sleep(float(2 ** (attempt - 1)))
        raise WebhookDeliveryError(last_failure)

    def _send_smtp_message(self, message: EmailMessage, recipient: str) -> None:
        context = ssl.create_default_context()
        if self._smtp_security == "tls":
            client = smtplib.SMTP_SSL(
                self._smtp_host,
                self._smtp_port,
                timeout=self._smtp_timeout_seconds,
                context=context,
            )
        else:
            client = smtplib.SMTP(
                self._smtp_host,
                self._smtp_port,
                timeout=self._smtp_timeout_seconds,
            )
        try:
            client.ehlo()
            if self._smtp_security == "starttls":
                client.starttls(context=context)
                client.ehlo()
            if self._smtp_username:
                client.login(self._smtp_username, self._smtp_password)
            client.send_message(
                message,
                from_addr=self._smtp_from_address,
                to_addrs=[recipient],
            )
        finally:
            try:
                client.quit()
            except (OSError, smtplib.SMTPException):
                client.close()

    async def _post(
        self,
        *,
        target: str,
        payload: dict,
        event_name: str,
        idempotency_key: str,
    ) -> None:
        target, payload, provider = self._provider_request(
            target=target,
            payload=payload,
            event_name=event_name,
            idempotency_key=idempotency_key,
        )
        bound_target, host_header, sni_hostname = await self._validate_and_bind_target(
            target,
            official_provider=provider != "generic",
        )
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "DriftGuard-Worker/1.0",
            "X-DriftGuard-Event": event_name,
            "Idempotency-Key": idempotency_key,
            "Host": host_header,
        }

        last_failure = "unknown webhook failure"
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = await self._client.request(
                    "POST",
                    bound_target,
                    json=payload,
                    headers=headers,
                    extensions={"sni_hostname": sni_hostname},
                )
                if 200 <= response.status_code < 300:
                    return
                last_failure = f"webhook returned HTTP {response.status_code}"
                retryable = response.status_code in {408, 425, 429} or response.status_code >= 500
                if not retryable:
                    raise WebhookDeliveryError(last_failure)
            except WebhookDeliveryError:
                raise
            except (httpx.HTTPError, OSError) as exc:
                last_failure = f"webhook transport failed with {type(exc).__name__}"

            if attempt < self._max_attempts:
                await self._sleep(float(2 ** (attempt - 1)))

        raise WebhookDeliveryError(last_failure)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def _provider_request(
        self,
        *,
        target: str,
        payload: dict,
        event_name: str,
        idempotency_key: str,
    ) -> tuple[str, dict, str]:
        parsed = urlsplit(target)
        if parsed.scheme.lower() == "pagerduty":
            routing_key = parsed.netloc
            if (
                parsed.path
                or parsed.query
                or parsed.fragment
                or PAGERDUTY_KEY_PATTERN.fullmatch(routing_key) is None
            ):
                raise WebhookDeliveryError(
                    "PagerDuty targets must be pagerduty:// plus a 32-character routing key"
                )
            distance = payload.get("evaluation", {}).get("drift_distance")
            pagerduty_payload = {
                "routing_key": routing_key,
                "event_action": "trigger",
                "dedup_key": idempotency_key,
                "payload": {
                    "summary": "DriftGuard detected critical semantic drift",
                    "source": "driftguard",
                    "severity": "critical",
                    "timestamp": payload.get("observed_at"),
                    "custom_details": {
                        "alert_id": payload.get("alert_id"),
                        "project_id": payload.get("project_id"),
                        "run_id": payload.get("run_id"),
                        "drift_distance": distance,
                        "prompt_excerpt": payload.get("evidence", {}).get("prompt_excerpt"),
                        "output_excerpt": payload.get("evidence", {}).get("output_excerpt"),
                    },
                },
            }
            return PAGERDUTY_EVENTS_URL, pagerduty_payload, "pagerduty"

        hostname = (parsed.hostname or "").lower().rstrip(".")
        if hostname == SLACK_HOST:
            segments = [segment for segment in parsed.path.split("/") if segment]
            if (
                parsed.scheme.lower() != "https"
                or len(segments) != 4
                or segments[0] != "services"
                or parsed.query
                or parsed.fragment
            ):
                raise WebhookDeliveryError("Slack webhook URL has an invalid official path")
            summary = self._summary(payload, event_name, max_characters=2900)
            return (
                target,
                {
                    "text": summary,
                    "blocks": [
                        {
                            "type": "section",
                            "text": {"type": "mrkdwn", "text": summary},
                        }
                    ],
                },
                "slack",
            )

        if hostname == DISCORD_HOST:
            segments = [segment for segment in parsed.path.split("/") if segment]
            if (
                parsed.scheme.lower() != "https"
                or len(segments) != 4
                or segments[:2] != ["api", "webhooks"]
                or not segments[2].isdigit()
                or parsed.query
                or parsed.fragment
            ):
                raise WebhookDeliveryError("Discord webhook URL has an invalid official path")
            summary = self._summary(payload, event_name, max_characters=1900)
            return (
                target,
                {
                    "content": summary,
                    "allowed_mentions": {"parse": []},
                },
                "discord",
            )
        return target, payload, "generic"

    @staticmethod
    def _summary(payload: dict, event_name: str, *, max_characters: int) -> str:
        if event_name == "semantic_drift_digest":
            header = (
                "DriftGuard daily semantic drift digest: "
                f"{payload.get('count', 0)} alerts for project "
                f"{payload.get('project_id', 'unknown')}"
            )
            lines = [header]
            evaluations = payload.get("evaluations", [])
            included = 0
            for evaluation in evaluations[:20]:
                prompt = WebhookSender._provider_safe_excerpt(evaluation.get("prompt_excerpt"))
                output = WebhookSender._provider_safe_excerpt(evaluation.get("output_excerpt"))
                evidence_lines = [
                    f"- run {evaluation.get('run_id', 'unknown')}: "
                    f"distance={evaluation.get('drift_distance')}"
                ]
                if prompt:
                    evidence_lines.append(f"  input: {prompt}")
                if output:
                    evidence_lines.append(f"  output: {output}")
                candidate = "\n".join(evidence_lines)
                if len("\n".join([*lines, candidate])) > max_characters - 80:
                    break
                lines.append(candidate)
                included += 1
            omitted = max(0, int(payload.get("count", 0)) - included)
            if omitted:
                suffix = f"- +{omitted} additional alerts in this daily digest"
                if len("\n".join([*lines, suffix])) <= max_characters:
                    lines.append(suffix)
            return "\n".join(lines)[:max_characters]
        distance = payload.get("evaluation", {}).get("drift_distance")
        lines = [
            "DriftGuard semantic drift alert for project "
            f"{payload.get('project_id', 'unknown')} "
            f"(distance={distance})"
        ]
        evidence = payload.get("evidence", {})
        prompt = WebhookSender._provider_safe_excerpt(evidence.get("prompt_excerpt"))
        output = WebhookSender._provider_safe_excerpt(evidence.get("output_excerpt"))
        for label, excerpt in (("Input", prompt), ("Output", output)):
            if not excerpt:
                continue
            candidate = f"{label}: {excerpt}"
            if len("\n".join([*lines, candidate])) <= max_characters:
                lines.append(candidate)
        return "\n".join(lines)[:max_characters]

    @staticmethod
    def _evidence_excerpt(value: str) -> str:
        if not isinstance(value, str):
            return ""
        normalized = unicodedata.normalize("NFC", value)
        compact = " ".join(normalized.split())
        if len(compact) <= MAX_EVIDENCE_EXCERPT_CHARACTERS:
            return compact
        return compact[: MAX_EVIDENCE_EXCERPT_CHARACTERS - 1] + "…"

    @staticmethod
    def _provider_safe_excerpt(value: object) -> str:
        if not isinstance(value, str):
            return ""
        return value.replace("<", "‹").replace(">", "›")

    async def _validate_and_bind_target(
        self,
        target: str,
        *,
        official_provider: bool,
    ) -> tuple[str, str, str]:
        parsed = urlsplit(target)
        if parsed.scheme.lower() != "https" or not parsed.hostname:
            raise WebhookDeliveryError("NOTIFY targets must be absolute HTTPS webhook URLs")
        if parsed.username or parsed.password:
            raise WebhookDeliveryError("webhook URLs may not contain credentials")
        try:
            port = parsed.port
        except ValueError as exc:
            raise WebhookDeliveryError("webhook port is invalid") from exc
        if port is not None and not 10 <= port <= 65435:
            raise WebhookDeliveryError("webhook port is outside the allowed range")

        try:
            hostname = parsed.hostname.encode("idna").decode("ascii").lower().rstrip(".")
        except UnicodeError as exc:
            raise WebhookDeliveryError("webhook hostname is invalid") from exc
        if not official_provider:
            if not self._allowed_hosts:
                raise WebhookDeliveryError("generic webhooks require WEBHOOK_ALLOWED_HOSTS")
            if not any(
                hostname == allowed or hostname.endswith(f".{allowed}")
                for allowed in self._allowed_hosts
            ):
                raise WebhookDeliveryError("webhook hostname is not allowlisted")

        addresses = await self._resolve_addresses(hostname, port or 443)

        if not addresses or any(
            not is_public_unicast_address(address) for address in addresses
        ):
            raise WebhookDeliveryError(
                "webhook target resolves to a private or non-routable address"
            )
        selected = sorted(addresses, key=lambda address: (address.version, int(address)))[0]
        bound_target = str(httpx.URL(target).copy_with(host=str(selected)))
        host_header = hostname
        if port is not None and port != 443:
            host_header = f"{hostname}:{port}"
        return bound_target, host_header, hostname

    @staticmethod
    async def _resolve_addresses(
        hostname: str,
        port: int,
    ) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
        try:
            literal = ipaddress.ip_address(hostname)
            return [literal]
        except ValueError:
            try:
                records = await asyncio.get_running_loop().getaddrinfo(
                    hostname,
                    port,
                    family=socket.AF_UNSPEC,
                    type=socket.SOCK_STREAM,
                )
            except OSError as exc:
                raise WebhookDeliveryError("webhook hostname could not be resolved") from exc
            return list({ipaddress.ip_address(record[4][0]) for record in records})

    @classmethod
    def _mailto_recipient(cls, target: str) -> str:
        parsed = urlsplit(target)
        if (
            parsed.scheme.lower() != "mailto"
            or not parsed.path
            or parsed.netloc
            or parsed.query
            or parsed.fragment
        ):
            raise WebhookDeliveryError("email targets must be a single absolute mailto address")
        return cls._email_address(unquote(parsed.path))

    @staticmethod
    def _email_address(value: str) -> str:
        if "\r" in value or "\n" in value or value.count("@") != 1:
            raise WebhookDeliveryError("email target is invalid")
        local, domain = value.rsplit("@", 1)
        if (
            not local
            or len(local) > 64
            or local.startswith(".")
            or local.endswith(".")
            or ".." in local
            or EMAIL_LOCAL_PATTERN.fullmatch(local) is None
        ):
            raise WebhookDeliveryError("email target is invalid")
        try:
            ascii_domain = domain.encode("idna").decode("ascii").lower()
        except UnicodeError as exc:
            raise WebhookDeliveryError("email target domain is invalid") from exc
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
            raise WebhookDeliveryError("email target domain is invalid")
        return f"{local}@{ascii_domain}"
