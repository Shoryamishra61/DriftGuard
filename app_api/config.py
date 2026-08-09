"""Runtime configuration loaded exclusively from environment variables."""

from __future__ import annotations

import ipaddress
from functools import cached_property

from pydantic import AliasChoices, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL, make_url

from common_utils.network import is_public_unicast_address


class ConfigurationError(RuntimeError):
    """Raised when a required dependency cannot be configured safely."""


class Settings(BaseSettings):
    """Validated API configuration.

    Credentials remain ``SecretStr`` values until a client is constructed and
    are never rendered into logs or API responses.
    """

    model_config = SettingsConfigDict(
        env_file=None,
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )

    app_name: str = "DriftGuard API"
    environment: str = "production"
    admin_token: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("DRIFTGUARD_ADMIN_TOKEN"),
    )
    max_request_bytes: int = Field(
        default=256 * 1024,
        ge=128 * 1024,
        le=50 * 1024 * 1024,
        validation_alias=AliasChoices("MAX_REQUEST_BYTES"),
    )

    database_url: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("DATABASE_URL"),
    )
    db_host: str | None = Field(default=None, validation_alias=AliasChoices("DB_HOST"))
    db_port: int = Field(default=5432, ge=10, le=65435, validation_alias=AliasChoices("DB_PORT"))
    db_name: str | None = Field(
        default=None,
        validation_alias=AliasChoices("DB_NAME", "DB_DATABASE"),
    )
    db_user: str | None = Field(
        default=None,
        validation_alias=AliasChoices("DB_USER", "DB_USERNAME"),
    )
    db_password: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("DB_PASSWORD", "DB_PASS"),
    )

    valkey_url: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("VALKEY_URL"),
    )
    valkey_host: str = Field(
        default="cache",
        validation_alias=AliasChoices("VALKEY_HOST"),
    )
    valkey_port: int = Field(
        default=6379,
        ge=10,
        le=65435,
        validation_alias=AliasChoices("VALKEY_PORT"),
    )
    valkey_password: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("VALKEY_PASSWORD"),
    )
    valkey_database: int = Field(
        default=0,
        ge=0,
        le=15,
        validation_alias=AliasChoices("VALKEY_DATABASE"),
    )

    qdrant_url: str | None = Field(default=None, validation_alias=AliasChoices("QDRANT_URL"))
    qdrant_host: str | None = Field(default=None, validation_alias=AliasChoices("QDRANT_HOST"))
    qdrant_port: int = Field(
        default=6333,
        ge=10,
        le=65435,
        validation_alias=AliasChoices("QDRANT_PORT"),
    )
    qdrant_api_key: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("QDRANT_API_KEY"),
    )
    qdrant_collection: str = Field(
        default="drift_baselines",
        validation_alias=AliasChoices("QDRANT_COLLECTION"),
    )

    queue_name: str = Field(
        default="drift_eval_queue",
        validation_alias=AliasChoices("DRIFT_QUEUE_NAME", "QUEUE_NAME"),
    )
    dependency_timeout_seconds: float = Field(default=1.5, gt=0, le=10)
    startup_max_attempts: int = Field(
        default=5,
        ge=1,
        le=8,
        validation_alias=AliasChoices("STARTUP_MAX_ATTEMPTS"),
    )
    startup_backoff_seconds: float = Field(
        default=2.0,
        ge=0.1,
        le=60.0,
        validation_alias=AliasChoices("STARTUP_BACKOFF_SECONDS"),
    )
    ingest_rate_limit_requests: int = Field(
        default=1000,
        ge=1,
        le=100_000,
        validation_alias=AliasChoices("INGEST_RATE_LIMIT_REQUESTS"),
    )
    ingest_rate_limit_window_seconds: int = Field(
        default=1,
        ge=1,
        le=3600,
        validation_alias=AliasChoices("INGEST_RATE_LIMIT_WINDOW_SECONDS"),
    )
    rate_limit_fail_open: bool = Field(
        default=True,
        validation_alias=AliasChoices("RATE_LIMIT_FAIL_OPEN"),
    )
    webhook_allowed_hosts_csv: str = Field(
        default="",
        validation_alias=AliasChoices("WEBHOOK_ALLOWED_HOSTS"),
    )
    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0, le=60)
    outbox_batch_size: int = Field(default=50, ge=1, le=500)
    outbox_max_attempts: int = Field(default=20, ge=1, le=100)
    outbox_retry_base_seconds: int = Field(default=2, ge=1, le=60)
    outbox_retry_max_seconds: int = Field(default=300, ge=1, le=3600)
    outbox_dispatch_lease_seconds: int = Field(default=60, ge=15, le=3600)
    outbox_dedupe_ttl_seconds: int = Field(default=45, ge=10, le=3590)

    database_pool_size: int = Field(default=10, ge=1, le=100)
    database_max_overflow: int = Field(default=20, ge=0, le=200)

    @field_validator("queue_name", "qdrant_collection")
    @classmethod
    def validate_redis_and_collection_names(cls, value: str) -> str:
        if not value.strip() or any(character.isspace() for character in value):
            raise ValueError("name must be non-empty and contain no whitespace")
        return value

    @field_validator("admin_token")
    @classmethod
    def validate_admin_token(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None and len(value.get_secret_value().encode("utf-8")) < 32:
            raise ValueError("DRIFTGUARD_ADMIN_TOKEN must contain at least 32 UTF-8 bytes")
        return value

    @field_validator("webhook_allowed_hosts_csv")
    @classmethod
    def validate_webhook_allowed_hosts(cls, value: str) -> str:
        normalized_hosts: list[str] = []
        for raw_host in value.split(","):
            candidate = raw_host.strip().lower().rstrip(".")
            if not candidate:
                continue
            if any(character in candidate for character in "/:@?#[]") or any(
                character.isspace() for character in candidate
            ):
                raise ValueError("WEBHOOK_ALLOWED_HOSTS entries must be bare hostnames")
            try:
                hostname = candidate.encode("idna").decode("ascii")
            except UnicodeError as exc:
                raise ValueError("WEBHOOK_ALLOWED_HOSTS contains an invalid hostname") from exc
            try:
                address = ipaddress.ip_address(hostname)
            except ValueError:
                labels = hostname.split(".")
                if (
                    len(labels) < 2
                    or any(
                        not label
                        or len(label) > 63
                        or label.startswith("-")
                        or label.endswith("-")
                        or not label.replace("-", "").isalnum()
                        for label in labels
                    )
                ):
                    raise ValueError(
                        "WEBHOOK_ALLOWED_HOSTS contains an invalid hostname"
                    ) from None
            else:
                if not is_public_unicast_address(address):
                    raise ValueError(
                        "WEBHOOK_ALLOWED_HOSTS may contain only public unicast addresses"
                    )
            normalized_hosts.append(hostname)
        return ",".join(dict.fromkeys(normalized_hosts))

    @field_validator("qdrant_url")
    @classmethod
    def validate_qdrant_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.rstrip("/")
        if not normalized.startswith("http://"):
            raise ValueError("Qdrant private-network URL must use http://")
        return normalized

    @model_validator(mode="after")
    def validate_dispatch_marker_window(self) -> Settings:
        if self.outbox_dedupe_ttl_seconds >= self.outbox_dispatch_lease_seconds:
            raise ValueError(
                "outbox dedupe TTL must be shorter than the stale dispatch lease"
            )
        return self

    @cached_property
    def database_dsn(self) -> URL:
        if self.database_url is not None:
            raw_url = self.database_url.get_secret_value()
            parsed = make_url(raw_url)
            if parsed.get_backend_name() != "postgresql":
                raise ConfigurationError("DATABASE_URL must target PostgreSQL")
            return parsed.set(drivername="postgresql+asyncpg")

        required = {
            "DB_HOST": self.db_host,
            "DB_NAME": self.db_name,
            "DB_USER": self.db_user,
            "DB_PASSWORD": self.db_password,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            raise ConfigurationError(
                "PostgreSQL configuration is incomplete; set DATABASE_URL or all DB_* variables"
            )

        assert self.db_host is not None
        assert self.db_name is not None
        assert self.db_user is not None
        assert self.db_password is not None
        return URL.create(
            drivername="postgresql+asyncpg",
            username=self.db_user,
            password=self.db_password.get_secret_value(),
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
        )

    @cached_property
    def qdrant_base_url(self) -> str | None:
        if self.qdrant_url is not None:
            return self.qdrant_url
        if self.qdrant_host is None:
            return None
        return f"http://{self.qdrant_host}:{self.qdrant_port}"

    @cached_property
    def startup_backoff_schedule(self) -> tuple[float, ...]:
        """Five configured retries yield the canonical 2/4/8/16/32 schedule."""

        return tuple(
            self.startup_backoff_seconds * (2**retry_index)
            for retry_index in range(self.startup_max_attempts)
        )

    @cached_property
    def webhook_allowed_hosts(self) -> tuple[str, ...]:
        return tuple(
            host for host in self.webhook_allowed_hosts_csv.split(",") if host
        )
