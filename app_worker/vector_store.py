"""Qdrant adapter with mandatory logical tenant isolation."""

from __future__ import annotations

import hashlib
import math
import re
from uuid import UUID, uuid5

from qdrant_client import AsyncQdrantClient, models

from .circuit_breaker import AsyncCircuitBreaker
from .domain import BaselineManifest, BaselineMatch, BaselineSeed

BASELINE_MANIFEST_NAMESPACE = UUID("728e5d03-329e-5592-a07f-7e681d12d3a4")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class VectorStoreConfigurationError(RuntimeError):
    """Raised when the baseline collection violates the vector contract."""


class QdrantVectorStore:
    def __init__(
        self,
        client: AsyncQdrantClient,
        *,
        collection: str = "drift_baselines",
        dimension: int = 384,
        circuit_breaker: AsyncCircuitBreaker | None = None,
    ) -> None:
        self._client = client
        self._collection = collection
        self._dimension = dimension
        self._circuit_breaker = circuit_breaker or AsyncCircuitBreaker(
            failure_threshold=3,
            reset_seconds=30.0,
        )

    @classmethod
    async def connect(
        cls,
        *,
        url: str,
        api_key: str,
        collection: str,
        dimension: int,
        circuit_failure_threshold: int = 3,
        circuit_reset_seconds: float = 30.0,
    ) -> QdrantVectorStore:
        client = AsyncQdrantClient(url=url, api_key=api_key, timeout=5.0)
        store = cls(
            client,
            collection=collection,
            dimension=dimension,
            circuit_breaker=AsyncCircuitBreaker(
                failure_threshold=circuit_failure_threshold,
                reset_seconds=circuit_reset_seconds,
            ),
        )
        try:
            await store.initialize()
        except Exception:
            await client.close()
            raise
        return store

    async def initialize(self) -> None:
        """Ping Qdrant, create the collection if absent, then validate it."""

        await self._client.get_collections()
        exists = await self._client.collection_exists(self._collection)
        if not exists:
            try:
                await self._client.create_collection(
                    collection_name=self._collection,
                    vectors_config=models.VectorParams(
                        size=self._dimension,
                        distance=models.Distance.COSINE,
                    ),
                )
            except Exception as creation_error:
                # A peer may win the absent -> create race during an HA rollout.
                if not await self._client.collection_exists(self._collection):
                    raise creation_error

        info = await self._client.get_collection(self._collection)
        vectors = info.config.params.vectors
        if isinstance(vectors, dict):
            raise VectorStoreConfigurationError(
                "drift_baselines must use one unnamed 384-dimensional vector"
            )
        size = getattr(vectors, "size", None)
        distance = getattr(vectors, "distance", None)
        distance_value = getattr(distance, "value", str(distance)).lower()
        if size != self._dimension or distance_value != "cosine":
            raise VectorStoreConfigurationError(
                "drift_baselines must be configured as 384-dimensional cosine vectors"
            )

        for field_name in (
            "project_id",
            "point_type",
            "baseline_set",
            "embedding_model_revision",
        ):
            await self._ensure_keyword_index(field_name)

    async def _ensure_keyword_index(self, field_name: str) -> None:
        info = await self._client.get_collection(self._collection)
        payload_index = (info.payload_schema or {}).get(field_name)
        if payload_index is None:
            try:
                await self._client.create_payload_index(
                    collection_name=self._collection,
                    field_name=field_name,
                    field_schema=models.PayloadSchemaType.KEYWORD,
                    wait=True,
                )
            except Exception as creation_error:
                # Another horizontally scaled worker can create the same index first.
                refreshed = await self._client.get_collection(self._collection)
                payload_index = (refreshed.payload_schema or {}).get(field_name)
                if payload_index is None:
                    raise creation_error
            else:
                refreshed = await self._client.get_collection(self._collection)
                payload_index = (refreshed.payload_schema or {}).get(field_name)

        data_type = getattr(payload_index, "data_type", None)
        data_type_value = getattr(data_type, "value", str(data_type)).lower()
        if data_type_value != "keyword":
            raise VectorStoreConfigurationError(
                f"drift_baselines.{field_name} requires a Qdrant keyword payload index"
            )

    async def nearest_baseline(
        self,
        embedding: list[float],
        project_id: UUID,
        baseline_set: str,
        embedding_model_revision: str,
    ) -> BaselineMatch | None:
        if len(embedding) != self._dimension:
            raise ValueError(
                f"query vector has {len(embedding)} dimensions, expected {self._dimension}"
            )

        project_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="project_id",
                    match=models.MatchValue(value=str(project_id)),
                ),
                models.FieldCondition(
                    key="point_type",
                    match=models.MatchValue(value="baseline"),
                ),
                models.FieldCondition(
                    key="baseline_set",
                    match=models.MatchValue(value=baseline_set),
                ),
                models.FieldCondition(
                    key="embedding_model_revision",
                    match=models.MatchValue(value=embedding_model_revision),
                ),
            ]
        )
        response = await self._circuit_breaker.call(
            lambda: self._client.query_points(
                collection_name=self._collection,
                query=embedding,
                query_filter=project_filter,
                limit=1,
                with_payload=False,
                with_vectors=False,
            )
        )
        points = getattr(response, "points", response)
        if not points:
            return None

        point = points[0]
        similarity = float(point.score)
        if not math.isfinite(similarity) or not -1.000001 <= similarity <= 1.000001:
            raise ValueError("Qdrant returned an invalid cosine similarity")
        try:
            point_id = UUID(str(point.id))
        except (TypeError, ValueError) as exc:
            raise ValueError("baseline point IDs must be UUIDs") from exc
        return BaselineMatch(id=point_id, similarity=max(-1.0, min(1.0, similarity)))

    async def upsert_evaluation(
        self,
        embedding: list[float],
        *,
        run_id: UUID,
        project_id: UUID,
        drift_distance: float | None,
        matched_baseline_id: UUID | None,
        baseline_set: str | None,
        embedding_model_revision: str,
    ) -> None:
        """Persist one evaluated vector idempotently without polluting baseline search."""

        if len(embedding) != self._dimension:
            raise ValueError(
                f"evaluation vector has {len(embedding)} dimensions, expected {self._dimension}"
            )
        payload = {
            "project_id": str(project_id),
            "point_type": "evaluation",
            "run_id": str(run_id),
            "drift_distance": drift_distance,
            "matched_baseline_id": (
                str(matched_baseline_id) if matched_baseline_id is not None else None
            ),
            "baseline_set": baseline_set,
            "embedding_model_revision": embedding_model_revision,
        }
        await self._circuit_breaker.call(
            lambda: self._client.upsert(
                collection_name=self._collection,
                points=[
                    models.PointStruct(
                        id=str(run_id),
                        vector=embedding,
                        payload=payload,
                    )
                ],
                wait=True,
            )
        )

    async def upsert_baselines(self, baselines: list[BaselineSeed]) -> None:
        if not baselines:
            return
        points = []
        for baseline in baselines:
            if len(baseline.vector) != self._dimension:
                raise ValueError(
                    f"baseline vector has {len(baseline.vector)} dimensions, "
                    f"expected {self._dimension}"
                )
            points.append(
                models.PointStruct(
                    id=str(baseline.id),
                    vector=baseline.vector,
                    payload={
                        "project_id": str(baseline.project_id),
                        "point_type": "baseline",
                        "baseline_set": baseline.baseline_set,
                        "embedding_model_revision": baseline.embedding_model_revision,
                        "text_sha256": hashlib.sha256(baseline.text.encode("utf-8")).hexdigest(),
                        "text": baseline.text,
                    },
                )
            )
        await self._circuit_breaker.call(
            lambda: self._client.upsert(
                collection_name=self._collection,
                points=points,
                wait=True,
            )
        )

    async def get_baseline_manifest(
        self,
        *,
        project_id: UUID,
        baseline_set: str,
        embedding_model_revision: str,
    ) -> BaselineManifest | None:
        marker_id = self.baseline_manifest_id(
            project_id,
            baseline_set,
            embedding_model_revision,
        )
        points = await self._circuit_breaker.call(
            lambda: self._client.retrieve(
                collection_name=self._collection,
                ids=[str(marker_id)],
                with_payload=True,
                with_vectors=False,
            )
        )
        if not points:
            return None
        payload = points[0].payload or {}
        manifest_hash = payload.get("manifest_hash")
        point_count = payload.get("point_count")
        expected = {
            "project_id": str(project_id),
            "point_type": "baseline_manifest",
            "baseline_set": baseline_set,
            "embedding_model_revision": embedding_model_revision,
        }
        if (
            any(payload.get(key) != value for key, value in expected.items())
            or not isinstance(manifest_hash, str)
            or SHA256_PATTERN.fullmatch(manifest_hash) is None
            or isinstance(point_count, bool)
            or not isinstance(point_count, int)
            or point_count < 1
        ):
            raise VectorStoreConfigurationError("baseline manifest payload is invalid")
        return BaselineManifest(manifest_hash=manifest_hash, point_count=point_count)

    async def delete_baseline_scope(
        self,
        *,
        project_id: UUID,
        baseline_set: str,
        embedding_model_revision: str,
    ) -> None:
        scope_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="project_id",
                    match=models.MatchValue(value=str(project_id)),
                ),
                models.FieldCondition(
                    key="point_type",
                    match=models.MatchValue(value="baseline"),
                ),
                models.FieldCondition(
                    key="baseline_set",
                    match=models.MatchValue(value=baseline_set),
                ),
                models.FieldCondition(
                    key="embedding_model_revision",
                    match=models.MatchValue(value=embedding_model_revision),
                ),
            ]
        )
        await self._circuit_breaker.call(
            lambda: self._client.delete(
                collection_name=self._collection,
                points_selector=models.FilterSelector(filter=scope_filter),
                wait=True,
            )
        )
        marker_id = self.baseline_manifest_id(
            project_id,
            baseline_set,
            embedding_model_revision,
        )
        await self._circuit_breaker.call(
            lambda: self._client.delete(
                collection_name=self._collection,
                points_selector=models.PointIdsList(points=[str(marker_id)]),
                wait=True,
            )
        )

    async def upsert_baseline_manifest(
        self,
        *,
        project_id: UUID,
        baseline_set: str,
        embedding_model_revision: str,
        manifest_hash: str,
        point_count: int,
    ) -> None:
        if SHA256_PATTERN.fullmatch(manifest_hash) is None or point_count < 1:
            raise ValueError("baseline manifest hash or point count is invalid")
        marker_id = self.baseline_manifest_id(
            project_id,
            baseline_set,
            embedding_model_revision,
        )
        marker_vector = [1.0, *([0.0] * (self._dimension - 1))]
        await self._circuit_breaker.call(
            lambda: self._client.upsert(
                collection_name=self._collection,
                points=[
                    models.PointStruct(
                        id=str(marker_id),
                        vector=marker_vector,
                        payload={
                            "project_id": str(project_id),
                            "point_type": "baseline_manifest",
                            "baseline_set": baseline_set,
                            "embedding_model_revision": embedding_model_revision,
                            "manifest_hash": manifest_hash,
                            "point_count": point_count,
                        },
                    )
                ],
                wait=True,
            )
        )

    @staticmethod
    def baseline_manifest_id(
        project_id: UUID,
        baseline_set: str,
        embedding_model_revision: str,
    ) -> UUID:
        return uuid5(
            BASELINE_MANIFEST_NAMESPACE,
            f"{project_id}:{baseline_set}:{embedding_model_revision}",
        )

    async def close(self) -> None:
        await self._client.close()
