"""Secure project provisioning and API-key rotation command-line utility."""

from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import os
import secrets
import sys
from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy import insert, select, text, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app_api.auth import MAX_API_KEY_BYTES, hash_api_key
from app_api.config import ConfigurationError, Settings
from app_api.database import create_engine, create_session_factory, ping_database
from app_api.db_schema import projects
from common_utils.retry import retry_async

MIN_API_KEY_BYTES = 32


class ProjectNotFoundError(LookupError):
    pass


class DuplicateProjectNameError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ProjectCredentials:
    project_id: UUID
    project_name: str
    api_key: str


@dataclass(frozen=True, slots=True)
class EnsuredProject:
    project_id: UUID
    project_name: str
    created: bool
    key_updated: bool


def generate_api_key() -> str:
    return f"dg_live_{secrets.token_urlsafe(32)}"


def validate_api_key(raw_api_key: str) -> str:
    encoded_length = len(raw_api_key.encode("utf-8"))
    if encoded_length < MIN_API_KEY_BYTES or encoded_length > MAX_API_KEY_BYTES:
        raise ValueError(
            f"API key must contain between {MIN_API_KEY_BYTES} and {MAX_API_KEY_BYTES} UTF-8 bytes"
        )
    return raw_api_key


def _validated_project_name(name: str) -> str:
    normalized = name.strip()
    if not normalized or len(normalized) > 100:
        raise ValueError("project name must contain 1 to 100 characters")
    return normalized


async def provision_project(
    session: AsyncSession,
    *,
    name: str,
    raw_api_key: str | None = None,
) -> ProjectCredentials:
    project_name = _validated_project_name(name)
    api_key = validate_api_key(raw_api_key or generate_api_key())
    project_id = uuid4()
    async with session.begin():
        await session.execute(
            insert(projects).values(
                id=project_id,
                name=project_name,
                api_key_hash=hash_api_key(api_key),
            )
        )
    return ProjectCredentials(
        project_id=project_id,
        project_name=project_name,
        api_key=api_key,
    )


async def rotate_project_key(
    session: AsyncSession,
    *,
    project_id: UUID,
    raw_api_key: str | None = None,
) -> ProjectCredentials:
    api_key = validate_api_key(raw_api_key or generate_api_key())
    async with session.begin():
        result = await session.execute(
            update(projects)
            .where(projects.c.id == project_id)
            .values(api_key_hash=hash_api_key(api_key))
            .returning(projects.c.name)
        )
        project_name = result.scalar_one_or_none()
    if project_name is None:
        raise ProjectNotFoundError("project does not exist")
    return ProjectCredentials(
        project_id=project_id,
        project_name=project_name,
        api_key=api_key,
    )


async def ensure_project(
    session: AsyncSession,
    *,
    name: str,
    raw_api_key: str,
) -> EnsuredProject:
    """Idempotently create or synchronize a named bootstrap project.

    A transaction-scoped PostgreSQL advisory lock serializes all cooperating
    bootstrap containers for the same fixed project name. The database stores
    only the SHA-256 digest; this function never returns the plaintext key.
    """

    project_name = _validated_project_name(name)
    api_key_hash = hash_api_key(validate_api_key(raw_api_key))
    async with session.begin():
        await session.execute(
            text(
                "SELECT pg_advisory_xact_lock("
                "hashtextextended(:bootstrap_project_name, 0))"
            ).bindparams(bootstrap_project_name=project_name)
        )
        result = await session.execute(
            select(projects.c.id, projects.c.api_key_hash)
            .where(projects.c.name == project_name)
            .order_by(projects.c.id)
            .with_for_update()
        )
        matches = list(result.mappings())
        if len(matches) > 1:
            raise DuplicateProjectNameError(
                "multiple projects already use the bootstrap project name"
            )

        if not matches:
            project_id = uuid4()
            await session.execute(
                insert(projects).values(
                    id=project_id,
                    name=project_name,
                    api_key_hash=api_key_hash,
                )
            )
            return EnsuredProject(
                project_id=project_id,
                project_name=project_name,
                created=True,
                key_updated=True,
            )

        existing = matches[0]
        project_id = existing["id"]
        key_updated = existing["api_key_hash"] != api_key_hash
        if key_updated:
            await session.execute(
                update(projects)
                .where(projects.c.id == project_id)
                .values(api_key_hash=api_key_hash)
            )
        return EnsuredProject(
            project_id=project_id,
            project_name=project_name,
            created=False,
            key_updated=key_updated,
        )


def credentials_json(credentials: ProjectCredentials) -> str:
    """Serialize credentials with exactly one occurrence of the plaintext key."""

    return json.dumps(
        {
            "project_id": str(credentials.project_id),
            "project_name": credentials.project_name,
            "api_key": credentials.api_key,
            "notice": "Store this API key now; DriftGuard persists only its SHA-256 digest.",
        },
        separators=(",", ":"),
    )


def ensured_project_json(project: EnsuredProject) -> str:
    return json.dumps(
        {
            "project_id": str(project.project_id),
            "project_name": project.project_name,
            "created": project.created,
            "key_updated": project.key_updated,
        },
        separators=(",", ":"),
    )


def _read_supplied_key(args: argparse.Namespace) -> str | None:
    if getattr(args, "api_key_env", None):
        value = os.getenv(args.api_key_env)
        if value is None:
            raise ValueError(f"environment variable {args.api_key_env!r} is not set")
        return value
    if getattr(args, "api_key_stdin", False):
        if sys.stdin.isatty():
            return getpass.getpass("API key: ")
        return sys.stdin.readline().rstrip("\r\n")
    return None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app_api.project_keys",
        description="Provision, ensure, or rotate DriftGuard project API keys.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    provision = subparsers.add_parser("provision", help="create a project")
    provision.add_argument("--name", required=True)
    provision_key = provision.add_mutually_exclusive_group()
    provision_key.add_argument("--api-key-env", metavar="VARIABLE")
    provision_key.add_argument("--api-key-stdin", action="store_true")

    rotate = subparsers.add_parser("rotate", help="replace a project's API key")
    rotate.add_argument("--project-id", required=True, type=UUID)
    rotate_key = rotate.add_mutually_exclusive_group()
    rotate_key.add_argument("--api-key-env", metavar="VARIABLE")
    rotate_key.add_argument("--api-key-stdin", action="store_true")

    ensure = subparsers.add_parser(
        "ensure",
        help="idempotently create or synchronize a fixed named project",
    )
    ensure.add_argument("--name", required=True)
    ensure.add_argument(
        "--api-key-env",
        default="DRIFTGUARD_BOOTSTRAP_PROJECT_KEY",
        metavar="VARIABLE",
    )
    return parser


async def _run_command(args: argparse.Namespace) -> ProjectCredentials | EnsuredProject:
    settings = Settings()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    try:
        await retry_async(
            lambda: ping_database(engine),
            operation_name="PostgreSQL",
            backoff_seconds=settings.startup_backoff_schedule,
        )
        supplied_key = _read_supplied_key(args)
        async with session_factory() as session:
            if args.command == "provision":
                return await provision_project(
                    session,
                    name=args.name,
                    raw_api_key=supplied_key,
                )
            if args.command == "ensure":
                if supplied_key is None:
                    raise ValueError("bootstrap project API key is required")
                return await ensure_project(
                    session,
                    name=args.name,
                    raw_api_key=supplied_key,
                )
            return await rotate_project_key(
                session,
                project_id=args.project_id,
                raw_api_key=supplied_key,
            )
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        credentials = asyncio.run(_run_command(args))
    except ProjectNotFoundError:
        print("project not found", file=sys.stderr)
        return 2
    except DuplicateProjectNameError:
        print("bootstrap project name is not unique", file=sys.stderr)
        return 6
    except IntegrityError:
        print("API key already belongs to another project", file=sys.stderr)
        return 3
    except (ConfigurationError, ValueError):
        print("invalid project-key configuration", file=sys.stderr)
        return 4
    except SQLAlchemyError:
        print("project-key database operation failed", file=sys.stderr)
        return 5

    if isinstance(credentials, EnsuredProject):
        print(ensured_project_json(credentials))
    else:
        print(credentials_json(credentials))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
