from __future__ import annotations

import logging
import os
import time
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import InterfaceError, OperationalError

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

logger = logging.getLogger("alembic.env")
target_metadata = None

MIGRATION_LOCK_KEY = 42069
CONNECTION_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2
CONNECTION_TIMEOUT_SECONDS = 3


def _database_url() -> str:
    url = os.getenv("DATABASE_URL") or config.get_main_option("sqlalchemy.url")
    if not url:
        raise RuntimeError("DATABASE_URL must be set before running migrations")
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg://", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    if url.startswith("postgresql+asyncpg://"):
        return url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
    return url


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        transactional_ddl=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def _run_locked_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_server_default=True,
        compare_type=True,
        transactional_ddl=True,
    )

    with context.begin_transaction():
        connection.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": MIGRATION_LOCK_KEY},
        )
        context.run_migrations()


def _migrate_with_retry(connectable: Engine) -> None:
    total_attempts = CONNECTION_RETRIES + 1
    for attempt in range(1, total_attempts + 1):
        try:
            with connectable.connect() as connection:
                _run_locked_migrations(connection)
            return
        except (OperationalError, InterfaceError, OSError) as exc:
            if attempt == total_attempts:
                raise RuntimeError(
                    f"database unavailable after {total_attempts} migration attempts"
                ) from exc

            delay = INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "Migration database connection attempt %d/%d failed; retrying in %ds",
                attempt,
                total_attempts,
                delay,
            )
            time.sleep(delay)


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = _database_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        connect_args={"connect_timeout": CONNECTION_TIMEOUT_SECONDS},
    )

    try:
        _migrate_with_retry(connectable)
    finally:
        connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
