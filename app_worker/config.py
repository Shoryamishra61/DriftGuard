"""Environment-backed configuration for the DriftGuard worker."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from os import environ
from urllib.parse import quote

DEFAULT_EMBEDDING_REVISION = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"


class ConfigurationError(ValueError):
    """Raised when required runtime configuration is absent or unsafe."""


def _required(source: Mapping[str, str], name: str) -> str:
    value = source.get(name, "").strip()
    if not value:
        raise ConfigurationError(f"{name} must be provided as a secret environment variable")
    return value


def _integer(
    source: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    raw = source.get(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return value


def _number(
    source: Mapping[str, str],
    name: str,
    default: float,
    *,
    minimum: float,
    maximum: float,
) -> float:
    raw = source.get(name, str(default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be numeric") from exc
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return value


def _choice(
    source: Mapping[str, str],
    name: str,
    default: str,
    choices: set[str],
) -> str:
    value = source.get(name, default).strip().lower()
    if value not in choices:
        allowed = ", ".join(sorted(choices))
        raise ConfigurationError(f"{name} must be one of: {allowed}")
    return value


def _boolean(source: Mapping[str, str], name: str, default: bool) -> bool:
    raw = source.get(name, "true" if default else "false").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"{name} must be a boolean")


def _database_url(source: Mapping[str, str]) -> str:
    explicit = source.get("DATABASE_URL", "").strip()
    if explicit:
        if not explicit.startswith(("postgresql://", "postgres://")):
            raise ConfigurationError("DATABASE_URL must use the PostgreSQL protocol")
        return explicit

    host = source.get("DB_HOST", "db").strip() or "db"
    port = _integer(source, "DB_PORT", 5432, minimum=10, maximum=65435)
    database = _required(source, "DB_NAME")
    user = _required(source, "DB_USER")
    password = _required(source, "DB_PASSWORD")
    return (
        f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}@"
        f"{host}:{port}/{quote(database, safe='')}"
    )


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    """Validated worker runtime settings.

    Hostname defaults are Zerops private service names. Credentials never have
    defaults and must be injected by Zerops secret variables.
    """

    database_url: str
    valkey_host: str
    valkey_port: int
    valkey_password: str
    qdrant_host: str
    qdrant_port: int
    qdrant_api_key: str
    queue_name: str = "drift_eval_queue"
    dead_letter_queue: str = "drift_eval_dead_letter"
    qdrant_collection: str = "drift_baselines"
    qdrant_circuit_failure_threshold: int = 3
    qdrant_circuit_reset_seconds: float = 30.0
    embedding_model: str = "models/all-MiniLM-L6-v2"
    embedding_model_revision: str = DEFAULT_EMBEDDING_REVISION
    embedding_dimension: int = 384
    embedding_cache_ttl_seconds: int = 3600
    baseline_cache_ttl_seconds: int = 86400
    max_text_characters: int = 2048
    heartbeat_interval_seconds: int = 5
    heartbeat_ttl_seconds: int = 15
    job_health_timeout_seconds: int = 60
    failure_dedupe_ttl_seconds: int = 60
    delivery_receipt_ttl_seconds: int = 86400
    max_job_attempts: int = 3
    webhook_timeout_seconds: float = 5.0
    webhook_max_attempts: int = 3
    webhook_allowed_hosts: tuple[str, ...] = ()
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_from_address: str = ""
    smtp_security: str = "starttls"
    smtp_timeout_seconds: float = 10.0
    db_pool_max_size: int = 5
    delivery_poll_interval_seconds: float = 1.0
    delivery_batch_size: int = 100
    delivery_lease_seconds: int = 60
    delivery_max_attempts: int = 5
    worker_concurrency: int = 4
    retention_enabled: bool = False
    retention_interval_seconds: int = 60
    retention_raw_text_days: int = 30
    retention_telemetry_days: int = 90
    retention_outbox_days: int = 7
    retention_batch_size: int = 5000
    retention_max_batches: int = 10

    def __post_init__(self) -> None:
        if self.db_pool_max_size <= self.worker_concurrency:
            raise ConfigurationError("DB_POOL_MAX_SIZE must be greater than WORKER_CONCURRENCY")
        if self.retention_telemetry_days < self.retention_raw_text_days:
            raise ConfigurationError(
                "RETENTION_TELEMETRY_DAYS must be at least RETENTION_RAW_TEXT_DAYS"
            )

    @property
    def qdrant_url(self) -> str:
        return f"http://{self.qdrant_host}:{self.qdrant_port}"

    @classmethod
    def from_env(cls, source: Mapping[str, str] | None = None) -> WorkerConfig:
        values = environ if source is None else source
        valkey_host = values.get("VALKEY_HOST", "cache").strip() or "cache"
        qdrant_host = values.get("QDRANT_HOST", "qdrant").strip() or "qdrant"
        if "://" in valkey_host or "://" in qdrant_host:
            raise ConfigurationError("private service hosts must be bare Zerops hostnames")

        allowed_hosts = tuple(
            host.strip().lower().rstrip(".")
            for host in values.get("WEBHOOK_ALLOWED_HOSTS", "").split(",")
            if host.strip()
        )
        smtp_host = values.get("SMTP_HOST", "").strip()
        smtp_username = values.get("SMTP_USERNAME", "").strip()
        smtp_password = values.get("SMTP_PASSWORD", "").strip()
        smtp_from_address = values.get("SMTP_FROM_ADDRESS", "").strip()
        if smtp_host and "://" in smtp_host:
            raise ConfigurationError("SMTP_HOST must be a bare hostname")
        if bool(smtp_username) != bool(smtp_password):
            raise ConfigurationError("SMTP_USERNAME and SMTP_PASSWORD must be configured together")
        if smtp_host and not smtp_from_address:
            raise ConfigurationError("SMTP_FROM_ADDRESS is required when SMTP_HOST is configured")

        return cls(
            database_url=_database_url(values),
            valkey_host=valkey_host,
            valkey_port=_integer(values, "VALKEY_PORT", 6379, minimum=10, maximum=65435),
            valkey_password=_required(values, "VALKEY_PASSWORD"),
            qdrant_host=qdrant_host,
            qdrant_port=_integer(values, "QDRANT_PORT", 6333, minimum=10, maximum=65435),
            qdrant_api_key=_required(values, "QDRANT_API_KEY"),
            queue_name=values.get("DRIFT_QUEUE_NAME", "drift_eval_queue").strip()
            or "drift_eval_queue",
            dead_letter_queue=values.get(
                "DRIFT_DEAD_LETTER_QUEUE", "drift_eval_dead_letter"
            ).strip()
            or "drift_eval_dead_letter",
            qdrant_collection=values.get("QDRANT_COLLECTION", "drift_baselines").strip()
            or "drift_baselines",
            qdrant_circuit_failure_threshold=_integer(
                values,
                "QDRANT_CIRCUIT_FAILURE_THRESHOLD",
                3,
                minimum=1,
                maximum=100,
            ),
            qdrant_circuit_reset_seconds=_number(
                values,
                "QDRANT_CIRCUIT_RESET_SECONDS",
                30.0,
                minimum=0.1,
                maximum=3600.0,
            ),
            embedding_model=values.get("EMBEDDING_MODEL", "models/all-MiniLM-L6-v2").strip()
            or "models/all-MiniLM-L6-v2",
            embedding_model_revision=values.get(
                "EMBEDDING_MODEL_REVISION",
                DEFAULT_EMBEDDING_REVISION,
            ).strip()
            or DEFAULT_EMBEDDING_REVISION,
            embedding_dimension=_integer(
                values, "EMBEDDING_DIMENSION", 384, minimum=384, maximum=384
            ),
            embedding_cache_ttl_seconds=_integer(
                values,
                "EMBEDDING_CACHE_TTL_SECONDS",
                3600,
                minimum=60,
                maximum=86400,
            ),
            baseline_cache_ttl_seconds=_integer(
                values,
                "BASELINE_CACHE_TTL_SECONDS",
                86400,
                minimum=300,
                maximum=604800,
            ),
            max_text_characters=_integer(
                values, "MAX_TEXT_CHARACTERS", 2048, minimum=1, maximum=2048
            ),
            heartbeat_interval_seconds=_integer(
                values, "WORKER_HEARTBEAT_INTERVAL_SECONDS", 5, minimum=1, maximum=30
            ),
            heartbeat_ttl_seconds=_integer(
                values, "WORKER_HEARTBEAT_TTL_SECONDS", 15, minimum=3, maximum=120
            ),
            job_health_timeout_seconds=_integer(
                values,
                "WORKER_JOB_HEALTH_TIMEOUT_SECONDS",
                60,
                minimum=30,
                maximum=300,
            ),
            failure_dedupe_ttl_seconds=_integer(
                values, "FAILURE_DEDUPE_TTL_SECONDS", 60, minimum=1, maximum=3600
            ),
            delivery_receipt_ttl_seconds=_integer(
                values,
                "DELIVERY_RECEIPT_TTL_SECONDS",
                86400,
                minimum=300,
                maximum=604800,
            ),
            max_job_attempts=_integer(values, "WORKER_MAX_JOB_ATTEMPTS", 3, minimum=1, maximum=10),
            webhook_timeout_seconds=_number(
                values, "WEBHOOK_TIMEOUT_SECONDS", 5.0, minimum=0.25, maximum=30.0
            ),
            webhook_max_attempts=_integer(values, "WEBHOOK_MAX_ATTEMPTS", 3, minimum=1, maximum=5),
            webhook_allowed_hosts=allowed_hosts,
            smtp_host=smtp_host,
            smtp_port=_integer(values, "SMTP_PORT", 587, minimum=10, maximum=65435),
            smtp_username=smtp_username,
            smtp_password=smtp_password,
            smtp_from_address=smtp_from_address,
            smtp_security=_choice(
                values,
                "SMTP_SECURITY",
                "starttls",
                {"starttls", "tls"},
            ),
            smtp_timeout_seconds=_number(
                values,
                "SMTP_TIMEOUT_SECONDS",
                10.0,
                minimum=1.0,
                maximum=30.0,
            ),
            db_pool_max_size=_integer(values, "DB_POOL_MAX_SIZE", 5, minimum=1, maximum=20),
            delivery_poll_interval_seconds=_number(
                values,
                "DELIVERY_POLL_INTERVAL_SECONDS",
                1.0,
                minimum=0.25,
                maximum=30.0,
            ),
            delivery_batch_size=_integer(
                values, "DELIVERY_BATCH_SIZE", 100, minimum=1, maximum=500
            ),
            delivery_lease_seconds=_integer(
                values, "DELIVERY_LEASE_SECONDS", 60, minimum=30, maximum=300
            ),
            delivery_max_attempts=_integer(
                values, "DELIVERY_MAX_ATTEMPTS", 5, minimum=1, maximum=20
            ),
            worker_concurrency=_integer(values, "WORKER_CONCURRENCY", 4, minimum=1, maximum=32),
            retention_enabled=_boolean(values, "RETENTION_ENABLED", True),
            retention_interval_seconds=_integer(
                values, "RETENTION_INTERVAL_SECONDS", 60, minimum=60, maximum=86400
            ),
            retention_raw_text_days=_integer(
                values, "RETENTION_RAW_TEXT_DAYS", 30, minimum=1, maximum=3650
            ),
            retention_telemetry_days=_integer(
                values, "RETENTION_TELEMETRY_DAYS", 90, minimum=1, maximum=3650
            ),
            retention_outbox_days=_integer(
                values, "RETENTION_OUTBOX_DAYS", 7, minimum=1, maximum=3650
            ),
            retention_batch_size=_integer(
                values, "RETENTION_BATCH_SIZE", 5000, minimum=100, maximum=10000
            ),
            retention_max_batches=_integer(
                values, "RETENTION_MAX_BATCHES", 10, minimum=1, maximum=100
            ),
        )
