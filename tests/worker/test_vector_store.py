import hashlib
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app_worker.config import DEFAULT_EMBEDDING_REVISION
from app_worker.domain import BaselineSeed
from app_worker.vector_store import QdrantVectorStore
from app_worker.worker import DriftWorker


class RecordingQdrantClient:
    def __init__(self, point_id, score: float) -> None:
        self.point_id = point_id
        self.score = score
        self.query = None
        self.upserts = []
        self.deletes = []
        self.retrieved = []

    async def query_points(self, **kwargs):
        self.query = kwargs
        return SimpleNamespace(points=[SimpleNamespace(id=self.point_id, score=self.score)])

    async def upsert(self, **kwargs):
        self.upserts.append(kwargs)

    async def retrieve(self, **kwargs):
        self.retrieved.append(kwargs)
        return []

    async def delete(self, **kwargs):
        self.deletes.append(kwargs)


class ExistingCollectionClient:
    def __init__(self) -> None:
        self.indexes_created = set()
        self.index_calls = []

    async def get_collections(self):
        return SimpleNamespace(collections=[])

    async def collection_exists(self, collection_name):
        assert collection_name == "drift_baselines"
        return True

    async def get_collection(self, collection_name):
        assert collection_name == "drift_baselines"
        vectors = SimpleNamespace(size=384, distance=SimpleNamespace(value="Cosine"))
        payload_schema = {
            field_name: SimpleNamespace(data_type=SimpleNamespace(value="keyword"))
            for field_name in self.indexes_created
        }
        return SimpleNamespace(
            config=SimpleNamespace(params=SimpleNamespace(vectors=vectors)),
            payload_schema=payload_schema,
        )

    async def create_payload_index(self, **kwargs):
        self.index_calls.append(kwargs)
        self.indexes_created.add(kwargs["field_name"])


class CreateRaceClient(ExistingCollectionClient):
    def __init__(self, *, peer_created: bool) -> None:
        super().__init__()
        self.peer_created = peer_created
        self.exists_calls = 0

    async def collection_exists(self, collection_name):
        assert collection_name == "drift_baselines"
        self.exists_calls += 1
        return self.peer_created and self.exists_calls > 1

    async def create_collection(self, **_kwargs):
        raise RuntimeError("already exists")


@pytest.mark.asyncio
async def test_nearest_baseline_always_filters_global_collection_by_project() -> None:
    project_id = uuid4()
    baseline_id = uuid4()
    client = RecordingQdrantClient(baseline_id, 0.73)
    store = QdrantVectorStore(client, collection="drift_baselines", dimension=384)

    match = await store.nearest_baseline(
        [0.0] * 384,
        project_id,
        "stable-v1",
        DEFAULT_EMBEDDING_REVISION,
    )

    assert match is not None
    assert match.id == baseline_id
    assert match.similarity == pytest.approx(0.73)
    assert client.query["collection_name"] == "drift_baselines"
    assert client.query["limit"] == 1
    tenant_filter = client.query["query_filter"]
    assert len(tenant_filter.must) == 4
    assert tenant_filter.must[0].key == "project_id"
    assert tenant_filter.must[0].match.value == str(project_id)
    assert tenant_filter.must[1].key == "point_type"
    assert tenant_filter.must[1].match.value == "baseline"
    assert tenant_filter.must[2].key == "baseline_set"
    assert tenant_filter.must[2].match.value == "stable-v1"
    assert tenant_filter.must[3].key == "embedding_model_revision"
    assert tenant_filter.must[3].match.value == DEFAULT_EMBEDDING_REVISION


@pytest.mark.asyncio
async def test_existing_collection_gets_verified_project_keyword_index() -> None:
    client = ExistingCollectionClient()
    store = QdrantVectorStore(client, collection="drift_baselines", dimension=384)

    await store.initialize()

    assert client.index_calls == [
        {
            "collection_name": "drift_baselines",
            "field_name": "project_id",
            "field_schema": "keyword",
            "wait": True,
        },
        {
            "collection_name": "drift_baselines",
            "field_name": "point_type",
            "field_schema": "keyword",
            "wait": True,
        },
        {
            "collection_name": "drift_baselines",
            "field_name": "baseline_set",
            "field_schema": "keyword",
            "wait": True,
        },
        {
            "collection_name": "drift_baselines",
            "field_name": "embedding_model_revision",
            "field_schema": "keyword",
            "wait": True,
        },
    ]


@pytest.mark.asyncio
async def test_collection_creation_race_rechecks_peer_result() -> None:
    client = CreateRaceClient(peer_created=True)
    store = QdrantVectorStore(client, collection="drift_baselines", dimension=384)

    await store.initialize()

    assert client.exists_calls == 2
    assert {call["field_name"] for call in client.index_calls} == {
        "project_id",
        "point_type",
        "baseline_set",
        "embedding_model_revision",
    }


@pytest.mark.asyncio
async def test_collection_creation_failure_raises_when_collection_stays_absent() -> None:
    client = CreateRaceClient(peer_created=False)
    store = QdrantVectorStore(client, collection="drift_baselines", dimension=384)

    with pytest.raises(RuntimeError, match="already exists"):
        await store.initialize()


@pytest.mark.asyncio
async def test_evaluation_upsert_is_idempotently_keyed_by_run_uuid() -> None:
    project_id = uuid4()
    run_id = uuid4()
    baseline_id = uuid4()
    client = RecordingQdrantClient(baseline_id, 0.7)
    store = QdrantVectorStore(client, collection="drift_baselines", dimension=384)

    await store.upsert_evaluation(
        [0.25] * 384,
        run_id=run_id,
        project_id=project_id,
        drift_distance=0.3,
        matched_baseline_id=baseline_id,
        baseline_set="stable-v1",
        embedding_model_revision=DEFAULT_EMBEDDING_REVISION,
    )

    assert len(client.upserts) == 1
    upsert = client.upserts[0]
    assert upsert["collection_name"] == "drift_baselines"
    assert upsert["wait"] is True
    assert len(upsert["points"]) == 1
    point = upsert["points"][0]
    assert point.id == str(run_id)
    assert point.payload == {
        "project_id": str(project_id),
        "point_type": "evaluation",
        "run_id": str(run_id),
        "drift_distance": 0.3,
        "matched_baseline_id": str(baseline_id),
        "baseline_set": "stable-v1",
        "embedding_model_revision": DEFAULT_EMBEDDING_REVISION,
    }


@pytest.mark.parametrize(
    ("similarity", "expected_distance"),
    [(1.0, 0.0), (0.82, 0.18), (0.0, 1.0), (-1.0, 2.0)],
)
def test_cosine_distance_is_one_minus_qdrant_score(
    similarity: float,
    expected_distance: float,
) -> None:
    assert DriftWorker.cosine_distance(similarity) == pytest.approx(expected_distance)


@pytest.mark.asyncio
async def test_vector_query_rejects_wrong_embedding_dimension() -> None:
    store = QdrantVectorStore(RecordingQdrantClient(uuid4(), 1.0))

    with pytest.raises(ValueError, match="383 dimensions"):
        await store.nearest_baseline(
            [0.0] * 383,
            uuid4(),
            "stable-v1",
            DEFAULT_EMBEDDING_REVISION,
        )


@pytest.mark.asyncio
async def test_baseline_upsert_persists_tenant_set_and_model_revision() -> None:
    project_id = uuid4()
    point_id = uuid4()
    client = RecordingQdrantClient(point_id, 1.0)
    store = QdrantVectorStore(client, collection="drift_baselines", dimension=384)

    await store.upsert_baselines(
        [
            BaselineSeed(
                id=point_id,
                project_id=project_id,
                baseline_set="stable-v1",
                embedding_model_revision=DEFAULT_EMBEDDING_REVISION,
                text="golden output",
                vector=[0.1] * 384,
            )
        ]
    )

    point = client.upserts[0]["points"][0]
    assert point.payload == {
        "project_id": str(project_id),
        "point_type": "baseline",
        "baseline_set": "stable-v1",
        "embedding_model_revision": DEFAULT_EMBEDDING_REVISION,
        "text_sha256": hashlib.sha256(b"golden output").hexdigest(),
        "text": "golden output",
    }


@pytest.mark.asyncio
async def test_manifest_marker_is_deterministic_and_excluded_from_baseline_type() -> None:
    project_id = uuid4()
    client = RecordingQdrantClient(uuid4(), 1.0)
    store = QdrantVectorStore(client, collection="drift_baselines", dimension=384)
    manifest_hash = "a" * 64

    await store.upsert_baseline_manifest(
        project_id=project_id,
        baseline_set="stable-v2",
        embedding_model_revision=DEFAULT_EMBEDDING_REVISION,
        manifest_hash=manifest_hash,
        point_count=3,
    )

    point = client.upserts[0]["points"][0]
    assert point.id == str(
        store.baseline_manifest_id(
            project_id,
            "stable-v2",
            DEFAULT_EMBEDDING_REVISION,
        )
    )
    assert point.payload == {
        "project_id": str(project_id),
        "point_type": "baseline_manifest",
        "baseline_set": "stable-v2",
        "embedding_model_revision": DEFAULT_EMBEDDING_REVISION,
        "manifest_hash": manifest_hash,
        "point_count": 3,
    }
    assert len(point.vector) == 384


@pytest.mark.asyncio
async def test_partial_inactive_scope_can_be_pruned_before_retry() -> None:
    project_id = uuid4()
    client = RecordingQdrantClient(uuid4(), 1.0)
    store = QdrantVectorStore(client, collection="drift_baselines", dimension=384)

    await store.delete_baseline_scope(
        project_id=project_id,
        baseline_set="candidate-v2",
        embedding_model_revision=DEFAULT_EMBEDDING_REVISION,
    )

    assert len(client.deletes) == 2
    scope = client.deletes[0]["points_selector"].filter
    assert [(condition.key, condition.match.value) for condition in scope.must] == [
        ("project_id", str(project_id)),
        ("point_type", "baseline"),
        ("baseline_set", "candidate-v2"),
        ("embedding_model_revision", DEFAULT_EMBEDDING_REVISION),
    ]
