import json
from uuid import uuid4

import pytest

from app_worker.config import DEFAULT_EMBEDDING_REVISION
from app_worker.domain import BaselineManifest
from app_worker.seed_baselines import (
    BaselineSeeder,
    BaselineValidationError,
    _parser,
    parse_baseline_line,
    validate_baseline_set,
)


class ProjectRepository:
    def __init__(self, exists=True, active_set=None):
        self.exists = exists
        self.active_set = active_set
        self.lookups = []
        self.activations = []

    async def project_exists(self, project_id):
        self.lookups.append(project_id)
        return self.exists

    async def activate_baseline_set(self, project_id, baseline_set):
        self.activations.append((project_id, baseline_set))
        self.active_set = baseline_set

    async def active_baseline_set(self, _project_id):
        return self.active_set


class BatchEmbedder:
    def __init__(self):
        self.batches = []

    async def embed_batch(self, texts):
        self.batches.append(list(texts))
        return [[float(index)] * 384 for index, _text in enumerate(texts, start=1)]


class BaselineStore:
    def __init__(self):
        self.batches = []
        self.manifest = None
        self.delete_calls = []

    async def upsert_baselines(self, baselines):
        self.batches.append(list(baselines))

    async def get_baseline_manifest(self, **_scope):
        return self.manifest

    async def delete_baseline_scope(self, **scope):
        self.delete_calls.append(scope)

    async def upsert_baseline_manifest(self, **manifest):
        self.manifest = BaselineManifest(
            manifest_hash=manifest["manifest_hash"],
            point_count=manifest["point_count"],
        )


class BaselineCache:
    def __init__(self):
        self.writes = []
        self.values = {}
        self.set_calls = []

    async def set(self, key, value, **kwargs):
        self.set_calls.append((key, value, kwargs))
        if kwargs.get("nx") and key in self.values:
            return None
        self.values[key] = value
        if ":baseline:" in key and "seed-lock" not in key:
            self.writes.append({key: value})
        return True

    async def get(self, key):
        return self.values.get(key)

    async def eval(self, _script, _count, key, owner):
        if self.values.get(key) == owner:
            self.values.pop(key, None)
            return 1
        return 0


def make_seeder(
    *,
    project_id=None,
    exists=True,
    activate=True,
    batch_size=2,
    active_set=None,
):
    project_id = project_id or uuid4()
    repository = ProjectRepository(exists, active_set)
    embedder = BatchEmbedder()
    store = BaselineStore()
    cache = BaselineCache()
    seeder = BaselineSeeder(
        project_id=project_id,
        baseline_set="stable-v1",
        batch_size=batch_size,
        embedder=embedder,
        vector_store=store,
        valkey=cache,
        repository=repository,
        embedding_model_revision=DEFAULT_EMBEDDING_REVISION,
        activate=activate,
    )
    return seeder, repository, embedder, store, cache


@pytest.mark.asyncio
async def test_seed_batches_upserts_caches_then_activates() -> None:
    project_id = uuid4()
    seeder, repository, embedder, store, cache = make_seeder(project_id=project_id)

    count = await seeder.seed(
        [
            json.dumps({"text": "golden one"}),
            json.dumps({"text": "golden two"}),
            json.dumps({"text": "golden three"}),
        ]
    )

    assert count == 3
    assert [len(batch) for batch in embedder.batches] == [2, 1]
    assert [len(batch) for batch in store.batches] == [2, 1]
    assert len(cache.writes) == 3
    assert repository.activations == [(project_id, "stable-v1")]
    assert len(store.delete_calls) == 1
    assert store.manifest is not None
    assert store.manifest.point_count == 3
    for batch in store.batches:
        for baseline in batch:
            assert baseline.project_id == project_id
            assert baseline.baseline_set == "stable-v1"
            assert baseline.embedding_model_revision == DEFAULT_EMBEDDING_REVISION
    for write in cache.writes:
        for key, value in write.items():
            assert key.startswith(f"driftguard:baseline:{project_id}:")
            payload = json.loads(value)
            assert payload["embedding_model_revision"] == DEFAULT_EMBEDDING_REVISION
            assert len(payload["vector"]) == 384
    vector_set_calls = [call for call in cache.set_calls if ":baseline:" in call[0]]
    assert all(call[2] == {"ex": 86400} for call in vector_set_calls)


@pytest.mark.asyncio
async def test_missing_project_performs_no_embedding_or_external_writes() -> None:
    seeder, repository, embedder, store, cache = make_seeder(exists=False)

    with pytest.raises(BaselineValidationError, match="project does not exist"):
        await seeder.seed([json.dumps({"text": "must not be embedded"})])

    assert len(repository.lookups) == 1
    assert repository.activations == []
    assert embedder.batches == []
    assert store.batches == []
    assert cache.writes == []


@pytest.mark.asyncio
async def test_no_activate_keeps_seeded_set_inactive() -> None:
    seeder, repository, _embedder, store, cache = make_seeder(activate=False)

    assert await seeder.seed([json.dumps({"text": "candidate"})]) == 1

    assert len(store.batches) == 1
    assert len(cache.writes) == 1
    assert repository.activations == []


def test_baseline_ids_are_deterministic_and_revision_scoped() -> None:
    project_id = uuid4()
    options = {
        "line_number": 1,
        "project_id": project_id,
        "baseline_set": "stable-v1",
        "embedding_model_revision": DEFAULT_EMBEDDING_REVISION,
    }

    first = parse_baseline_line('{"text":" café "}', **options)
    second = parse_baseline_line('{"text":"café"}', **options)
    changed_revision = parse_baseline_line(
        '{"text":"café"}',
        **{**options, "embedding_model_revision": "different-revision"},
    )

    assert first == second
    assert changed_revision[0] != first[0]


@pytest.mark.asyncio
async def test_exact_manifest_rerun_is_safe_but_changed_or_fewer_set_is_rejected() -> None:
    seeder, repository, _embedder, store, _cache = make_seeder()
    original = [json.dumps({"text": "one"}), json.dumps({"text": "two"})]

    assert await seeder.seed(original) == 2
    first_manifest = store.manifest
    embedded_batch_count = len(_embedder.batches)
    assert await seeder.seed(original) == 2
    assert store.manifest == first_manifest
    assert len(_embedder.batches) == embedded_batch_count
    writes_before_rejection = len(store.batches)

    with pytest.raises(BaselineValidationError, match="different content"):
        await seeder.seed([json.dumps({"text": "one"})])
    with pytest.raises(BaselineValidationError, match="different content"):
        await seeder.seed([json.dumps({"text": "one"}), json.dumps({"text": "changed"})])

    assert len(store.batches) == writes_before_rejection
    assert repository.active_set == "stable-v1"


@pytest.mark.asyncio
async def test_active_legacy_set_without_manifest_cannot_be_mutated() -> None:
    seeder, repository, embedder, store, cache = make_seeder(active_set="stable-v1")

    with pytest.raises(BaselineValidationError, match="legacy"):
        await seeder.seed([json.dumps({"text": "replacement"})])

    assert embedder.batches == []
    assert store.batches == []
    assert store.delete_calls == []
    assert cache.writes == []
    assert repository.activations == []


@pytest.mark.asyncio
async def test_exact_rerun_embeds_only_strict_cache_misses() -> None:
    seeder, _repository, embedder, _store, cache = make_seeder()
    records = [json.dumps({"text": "one"}), json.dumps({"text": "two"})]
    await seeder.seed(records)
    vector_keys = [key for key in cache.values if ":baseline:" in key]
    corrupted = json.loads(cache.values[vector_keys[0]])
    corrupted["project_id"] = str(uuid4())
    cache.values[vector_keys[0]] = json.dumps(corrupted)
    embedder.batches.clear()

    await seeder.seed(records)

    assert len(embedder.batches) == 1
    assert len(embedder.batches[0]) == 1


@pytest.mark.parametrize("value", ["", "has space", "slash/value", "x" * 101])
def test_baseline_set_validation_rejects_unsafe_names(value) -> None:
    with pytest.raises(BaselineValidationError):
        validate_baseline_set(value)


def test_cli_requires_exactly_one_project_selector() -> None:
    parser = _parser()

    by_id = parser.parse_args(
        ["--project-id", str(uuid4()), "--baseline-set", "competition-v1"]
    )
    by_name = parser.parse_args(
        ["--project-name", "Zerops Dashboard", "--baseline-set", "competition-v1"]
    )

    assert by_id.project_name is None
    assert by_name.project_id is None
    with pytest.raises(SystemExit):
        parser.parse_args(["--baseline-set", "competition-v1"])
