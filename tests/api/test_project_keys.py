from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest

from app_api.project_keys import (
    DuplicateProjectNameError,
    credentials_json,
    ensure_project,
    ensured_project_json,
    generate_api_key,
    provision_project,
    rotate_project_key,
    validate_api_key,
)

from .fakes import FakeResult, FakeSession


@pytest.mark.asyncio
async def test_provision_persists_only_sha256_digest() -> None:
    raw_key = "dg_live_" + "k" * 40
    session = FakeSession()

    credentials = await provision_project(
        session,
        name="Production",
        raw_api_key=raw_key,
    )

    params = session.statements[0].compile().params
    assert params["api_key_hash"] == hashlib.sha256(raw_key.encode()).hexdigest()
    assert raw_key not in params.values()
    assert credentials.api_key == raw_key
    assert credentials.project_name == "Production"
    assert session.events[-1] == "transaction.commit"


@pytest.mark.asyncio
async def test_rotation_replaces_hash_and_returns_plaintext_once() -> None:
    project_id = uuid4()
    raw_key = "dg_live_" + "r" * 40
    session = FakeSession(results=[FakeResult(scalar="Production")])

    credentials = await rotate_project_key(
        session,
        project_id=project_id,
        raw_api_key=raw_key,
    )

    params = session.statements[0].compile().params
    assert hashlib.sha256(raw_key.encode()).hexdigest() in params.values()
    assert raw_key not in params.values()
    rendered = credentials_json(credentials)
    assert rendered.count(raw_key) == 1
    assert "SHA-256" in rendered


def test_generated_keys_are_prefixed_and_not_reused() -> None:
    first = generate_api_key()
    second = generate_api_key()
    assert first.startswith("dg_live_")
    assert len(first.encode()) >= 32
    assert first != second


@pytest.mark.asyncio
async def test_ensure_serializes_bootstrap_and_creates_without_returning_plaintext() -> None:
    raw_key = "dg_live_" + "e" * 40
    session = FakeSession(results=[FakeResult(), FakeResult(rows=[]), FakeResult()])

    result = await ensure_project(session, name="Production", raw_api_key=raw_key)

    assert result.created is True
    assert result.key_updated is True
    assert not hasattr(result, "api_key")
    assert raw_key not in ensured_project_json(result)
    assert session.events[0] == "transaction.begin"
    assert session.events[-1] == "transaction.commit"
    advisory_sql = str(session.statements[0])
    assert "pg_advisory_xact_lock" in advisory_sql
    assert "hashtextextended" in advisory_sql
    insert_params = session.statements[2].compile().params
    assert insert_params["api_key_hash"] == hashlib.sha256(raw_key.encode()).hexdigest()
    assert raw_key not in insert_params.values()


@pytest.mark.asyncio
async def test_ensure_is_noop_when_named_project_already_has_requested_hash() -> None:
    project_id = uuid4()
    raw_key = "dg_live_" + "n" * 40
    digest = hashlib.sha256(raw_key.encode()).hexdigest()
    session = FakeSession(
        results=[
            FakeResult(),
            FakeResult(rows=[{"id": project_id, "api_key_hash": digest}]),
        ]
    )

    result = await ensure_project(session, name="Production", raw_api_key=raw_key)

    assert result.project_id == project_id
    assert result.created is False
    assert result.key_updated is False
    assert len(session.statements) == 2


@pytest.mark.asyncio
async def test_ensure_rotates_changed_hash_under_same_name_lock() -> None:
    project_id = uuid4()
    raw_key = "dg_live_" + "z" * 40
    session = FakeSession(
        results=[
            FakeResult(),
            FakeResult(rows=[{"id": project_id, "api_key_hash": "0" * 64}]),
            FakeResult(),
        ]
    )

    result = await ensure_project(session, name="Production", raw_api_key=raw_key)

    assert result.project_id == project_id
    assert result.created is False
    assert result.key_updated is True
    update_params = session.statements[2].compile().params
    assert hashlib.sha256(raw_key.encode()).hexdigest() in update_params.values()
    assert raw_key not in update_params.values()


@pytest.mark.asyncio
async def test_ensure_rejects_ambiguous_duplicate_project_names() -> None:
    raw_key = "dg_live_" + "d" * 40
    session = FakeSession(
        results=[
            FakeResult(),
            FakeResult(
                rows=[
                    {"id": uuid4(), "api_key_hash": "1" * 64},
                    {"id": uuid4(), "api_key_hash": "2" * 64},
                ]
            ),
        ]
    )

    with pytest.raises(DuplicateProjectNameError):
        await ensure_project(session, name="Production", raw_api_key=raw_key)

    assert session.events[-1] == "transaction.rollback"


@pytest.mark.parametrize("key", ["short", "x" * 513])
def test_supplied_key_length_is_validated(key: str) -> None:
    with pytest.raises(ValueError):
        validate_api_key(key)
