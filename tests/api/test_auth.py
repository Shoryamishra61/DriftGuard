from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app_api.auth import api_key_hash_from_header, hash_api_key, resolve_project

from .fakes import FakeResult, FakeSession


@pytest.mark.asyncio
async def test_api_key_is_sha256_hashed_before_tenant_lookup() -> None:
    raw_key = "dg_live_" + "a" * 40
    project_id = uuid4()
    session = FakeSession(results=[FakeResult(scalar=project_id)])

    digest = api_key_hash_from_header(raw_key)
    project = await resolve_project(api_key_hash=digest, session=session)

    assert project.id == project_id
    params = session.statements[0].compile().params
    assert params["api_key_hash_1"] == hashlib.sha256(raw_key.encode()).hexdigest()
    assert raw_key not in params.values()
    assert hash_api_key(raw_key) == params["api_key_hash_1"]


@pytest.mark.asyncio
async def test_invalid_key_has_same_public_response_as_missing_key() -> None:
    invalid_session = FakeSession(results=[FakeResult(scalar=None)])
    with pytest.raises(HTTPException) as invalid:
        await resolve_project(
            api_key_hash=hash_api_key("x" * 32),
            session=invalid_session,
        )
    with pytest.raises(HTTPException) as missing:
        api_key_hash_from_header(None)

    assert invalid.value.status_code == missing.value.status_code == 401
    assert invalid.value.detail == missing.value.detail == "invalid API key"
