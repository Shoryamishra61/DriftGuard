"""Project-scoped, idempotent JSONL baseline seeding CLI."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import math
import sys
import unicodedata
from collections.abc import Iterable
from pathlib import Path
from typing import TextIO
from uuid import UUID, uuid4, uuid5

from redis.asyncio import Redis

from .config import WorkerConfig
from .domain import BaselineSeed
from .embedding import SentenceTransformerEmbedder
from .repository import PostgresRepository
from .retry import retry_startup
from .vector_store import QdrantVectorStore

LOGGER = logging.getLogger("driftguard.worker.baselines")
BASELINE_ID_NAMESPACE = UUID("5d463a31-9a25-57e6-907c-bdc81a9bdb49")
MAX_BASELINE_CHARACTERS = 2048
MAX_JSONL_BYTES = 64 * 1024
MAX_BASELINES_PER_SET = 10_000
SEED_LOCK_TTL_SECONDS = 3600


class BaselineValidationError(ValueError):
    """Raised without echoing sensitive baseline content."""


def validate_baseline_set(value: str) -> str:
    normalized = value.strip()
    if not normalized or len(normalized) > 100:
        raise BaselineValidationError("baseline_set must contain 1 to 100 characters")
    if not all(character.isalnum() or character in "._-" for character in normalized):
        raise BaselineValidationError(
            "baseline_set may contain only letters, numbers, dot, underscore, and hyphen"
        )
    return normalized


def parse_baseline_line(
    line: str,
    *,
    line_number: int,
    project_id: UUID,
    baseline_set: str,
    embedding_model_revision: str,
) -> tuple[UUID, str]:
    if len(line.encode("utf-8")) > MAX_JSONL_BYTES:
        raise BaselineValidationError(f"line {line_number} exceeds 64 KiB")
    try:
        payload = json.loads(line)
    except json.JSONDecodeError as exc:
        raise BaselineValidationError(f"line {line_number} is not valid JSON") from exc
    if not isinstance(payload, dict) or set(payload) != {"text"}:
        raise BaselineValidationError(f"line {line_number} must be an object containing only text")
    text = payload["text"]
    if not isinstance(text, str):
        raise BaselineValidationError(f"line {line_number} text must be a string")
    normalized = unicodedata.normalize("NFC", text).strip()
    if not normalized or len(normalized) > MAX_BASELINE_CHARACTERS:
        raise BaselineValidationError(f"line {line_number} text must contain 1 to 2048 characters")
    text_digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    point_id = uuid5(
        BASELINE_ID_NAMESPACE,
        f"{project_id}:{baseline_set}:{embedding_model_revision}:{text_digest}",
    )
    return point_id, normalized


class BaselineSeeder:
    def __init__(
        self,
        *,
        project_id: UUID,
        baseline_set: str,
        batch_size: int,
        embedder,
        vector_store,
        valkey,
        repository,
        embedding_model_revision: str,
        activate: bool = True,
        cache_ttl_seconds: int = 86400,
        embedding_dimension: int = 384,
    ) -> None:
        if not 1 <= batch_size <= 128:
            raise BaselineValidationError("batch_size must be between 1 and 128")
        self.project_id = project_id
        self.baseline_set = validate_baseline_set(baseline_set)
        self.batch_size = batch_size
        self.embedder = embedder
        self.vector_store = vector_store
        self.valkey = valkey
        self.repository = repository
        self.embedding_model_revision = embedding_model_revision
        self.activate = activate
        self.cache_ttl_seconds = cache_ttl_seconds
        self.embedding_dimension = embedding_dimension
        if not 300 <= cache_ttl_seconds <= 604800:
            raise BaselineValidationError(
                "baseline cache TTL must be between 300 and 604800 seconds"
            )

    async def seed(self, lines: Iterable[str]) -> int:
        if not await self.repository.project_exists(self.project_id):
            raise BaselineValidationError("project does not exist")

        lock_key = self.seed_lock_key(
            self.project_id,
            self.baseline_set,
            self.embedding_model_revision,
        )
        lock_owner = str(uuid4())
        acquired = await self.valkey.set(
            lock_key,
            lock_owner,
            ex=SEED_LOCK_TTL_SECONDS,
            nx=True,
        )
        if not acquired:
            raise BaselineValidationError("baseline set seeding is already in progress")
        try:
            return await self._seed_locked(lines)
        finally:
            script = (
                "if redis.call('get', KEYS[1]) == ARGV[1] then "
                "return redis.call('del', KEYS[1]) else return 0 end"
            )
            try:
                await self.valkey.eval(script, 1, lock_key, lock_owner)
            except Exception as exc:
                LOGGER.warning("baseline seed lock release failed: %s", type(exc).__name__)

    async def _seed_locked(self, lines: Iterable[str]) -> int:
        pending: list[tuple[UUID, str]] = []
        seen_point_ids: set[UUID] = set()
        for line_number, line in enumerate(lines, start=1):
            if not line.strip():
                continue
            record = parse_baseline_line(
                line,
                line_number=line_number,
                project_id=self.project_id,
                baseline_set=self.baseline_set,
                embedding_model_revision=self.embedding_model_revision,
            )
            if record[0] in seen_point_ids:
                raise BaselineValidationError(
                    f"line {line_number} duplicates another baseline record"
                )
            seen_point_ids.add(record[0])
            pending.append(record)
            if len(pending) > MAX_BASELINES_PER_SET:
                raise BaselineValidationError(
                    f"baseline sets may contain at most {MAX_BASELINES_PER_SET} records"
                )
        if not pending:
            raise BaselineValidationError("baseline input contains no records")

        manifest_hash = hashlib.sha256(
            "\n".join(sorted(str(point_id) for point_id in seen_point_ids)).encode("ascii")
        ).hexdigest()
        active_set = await self.repository.active_baseline_set(self.project_id)
        existing_manifest = await self.vector_store.get_baseline_manifest(
            project_id=self.project_id,
            baseline_set=self.baseline_set,
            embedding_model_revision=self.embedding_model_revision,
        )
        if existing_manifest is not None and (
            existing_manifest.manifest_hash != manifest_hash
            or existing_manifest.point_count != len(pending)
        ):
            raise BaselineValidationError(
                "baseline_set already exists with different content; use a new versioned name"
            )
        if existing_manifest is None:
            if active_set == self.baseline_set:
                raise BaselineValidationError(
                    "active legacy baseline_set has no manifest and cannot be replaced"
                )
            await self.vector_store.delete_baseline_scope(
                project_id=self.project_id,
                baseline_set=self.baseline_set,
                embedding_model_revision=self.embedding_model_revision,
            )

        seeded = 0
        for offset in range(0, len(pending), self.batch_size):
            seeded += await self._seed_batch(
                pending[offset : offset + self.batch_size],
                reuse_cache=existing_manifest is not None,
            )
        await self.vector_store.upsert_baseline_manifest(
            project_id=self.project_id,
            baseline_set=self.baseline_set,
            embedding_model_revision=self.embedding_model_revision,
            manifest_hash=manifest_hash,
            point_count=seeded,
        )
        if self.activate:
            await self.repository.activate_baseline_set(
                self.project_id,
                self.baseline_set,
            )
        return seeded

    async def _seed_batch(
        self,
        records: list[tuple[UUID, str]],
        *,
        reuse_cache: bool,
    ) -> int:
        vectors: list[list[float] | None] = [None] * len(records)
        if reuse_cache:
            cache_keys = [
                self.cache_key(self.project_id, point_id, self.embedding_model_revision)
                for point_id, _text in records
            ]
            cached_values = await asyncio.gather(*(self.valkey.get(key) for key in cache_keys))
            for index, ((point_id, _text), raw) in enumerate(
                zip(records, cached_values, strict=True)
            ):
                vectors[index] = self._validated_cached_vector(raw, point_id)

        missing_indexes = [index for index, vector in enumerate(vectors) if vector is None]
        if missing_indexes:
            embedded = await self.embedder.embed_batch(
                [records[index][1] for index in missing_indexes]
            )
            if len(embedded) != len(missing_indexes):
                raise RuntimeError("embedding batch size mismatch")
            for index, vector in zip(missing_indexes, embedded, strict=True):
                vectors[index] = vector
        resolved_vectors = [vector for vector in vectors if vector is not None]
        if len(resolved_vectors) != len(records):
            raise RuntimeError("baseline vector resolution is incomplete")
        baselines = [
            BaselineSeed(
                id=point_id,
                project_id=self.project_id,
                baseline_set=self.baseline_set,
                embedding_model_revision=self.embedding_model_revision,
                text=text,
                vector=vector,
            )
            for (point_id, text), vector in zip(
                records,
                resolved_vectors,
                strict=True,
            )
        ]
        await self.vector_store.upsert_baselines(baselines)
        await asyncio.gather(
            *(
                self.valkey.set(
                    self.cache_key(
                        self.project_id,
                        baseline.id,
                        self.embedding_model_revision,
                    ),
                    json.dumps(
                        {
                            "point_id": str(baseline.id),
                            "project_id": str(self.project_id),
                            "baseline_set": self.baseline_set,
                            "embedding_model_revision": self.embedding_model_revision,
                            "vector": baseline.vector,
                        },
                        separators=(",", ":"),
                    ),
                    ex=self.cache_ttl_seconds,
                )
                for baseline in baselines
            )
        )
        return len(baselines)

    def _validated_cached_vector(
        self,
        raw: str | bytes | None,
        point_id: UUID,
    ) -> list[float] | None:
        if raw is None:
            return None
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                return None
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
        expected = {
            "point_id": str(point_id),
            "project_id": str(self.project_id),
            "baseline_set": self.baseline_set,
            "embedding_model_revision": self.embedding_model_revision,
        }
        vector = payload.get("vector") if isinstance(payload, dict) else None
        if (
            not isinstance(payload, dict)
            or any(payload.get(key) != value for key, value in expected.items())
            or not isinstance(vector, list)
            or len(vector) != self.embedding_dimension
            or any(
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                for value in vector
            )
        ):
            return None
        return [float(value) for value in vector]

    @staticmethod
    def cache_key(project_id: UUID, point_id: UUID, model_revision: str) -> str:
        revision_digest = hashlib.sha256(model_revision.encode("utf-8")).hexdigest()[:16]
        return f"driftguard:baseline:{project_id}:{revision_digest}:{point_id}"

    @staticmethod
    def seed_lock_key(
        project_id: UUID,
        baseline_set: str,
        model_revision: str,
    ) -> str:
        revision_digest = hashlib.sha256(model_revision.encode("utf-8")).hexdigest()[:16]
        return f"driftguard:baseline-seed-lock:{project_id}:{baseline_set}:{revision_digest}"


async def _connect_valkey(config: WorkerConfig) -> Redis:
    client = Redis(
        host=config.valkey_host,
        port=config.valkey_port,
        password=config.valkey_password,
        decode_responses=True,
        socket_connect_timeout=5.0,
        socket_timeout=10.0,
    )
    try:
        await client.ping()
    except Exception:
        await client.aclose()
        raise
    return client


def _input_stream(path: str) -> tuple[TextIO, bool]:
    if path == "-":
        return sys.stdin, False
    return Path(path).open("r", encoding="utf-8"), True


async def _run(args: argparse.Namespace) -> int:
    config = WorkerConfig.from_env()
    project_id = UUID(args.project_id)
    baseline_set = validate_baseline_set(args.baseline_set)
    valkey = None
    vector_store = None
    repository = None
    stream = None
    close_stream = False
    try:
        dependency_results = await asyncio.gather(
            retry_startup(
                "PostgreSQL",
                lambda: PostgresRepository.connect(
                    config.database_url,
                    max_size=config.db_pool_max_size,
                ),
            ),
            retry_startup("Valkey", lambda: _connect_valkey(config)),
            retry_startup(
                "Qdrant",
                lambda: QdrantVectorStore.connect(
                    url=config.qdrant_url,
                    api_key=config.qdrant_api_key,
                    collection=config.qdrant_collection,
                    dimension=config.embedding_dimension,
                    circuit_failure_threshold=(config.qdrant_circuit_failure_threshold),
                    circuit_reset_seconds=config.qdrant_circuit_reset_seconds,
                ),
            ),
            return_exceptions=True,
        )
        if not isinstance(dependency_results[0], BaseException):
            repository = dependency_results[0]
        if not isinstance(dependency_results[1], BaseException):
            valkey = dependency_results[1]
        if not isinstance(dependency_results[2], BaseException):
            vector_store = dependency_results[2]
        failure = next(
            (result for result in dependency_results if isinstance(result, BaseException)),
            None,
        )
        if failure is not None:
            raise failure

        embedder = await SentenceTransformerEmbedder.load(
            config.embedding_model,
            dimension=config.embedding_dimension,
            local_files_only=True,
        )
        stream, close_stream = _input_stream(args.input)
        seeder = BaselineSeeder(
            project_id=project_id,
            baseline_set=baseline_set,
            batch_size=args.batch_size,
            embedder=embedder,
            vector_store=vector_store,
            valkey=valkey,
            repository=repository,
            embedding_model_revision=config.embedding_model_revision,
            activate=not args.no_activate,
            cache_ttl_seconds=config.baseline_cache_ttl_seconds,
            embedding_dimension=config.embedding_dimension,
        )
        count = await seeder.seed(stream)
        print(f"Seeded {count} baseline vectors for project {project_id} in set {baseline_set}.")
        return 0
    finally:
        if close_stream and stream is not None:
            stream.close()
        closers = []
        if vector_store is not None:
            closers.append(vector_store.close())
        if valkey is not None:
            closers.append(valkey.aclose())
        if repository is not None:
            closers.append(repository.close())
        if closers:
            await asyncio.gather(*closers, return_exceptions=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Seed project-scoped DriftGuard baselines")
    parser.add_argument("--project-id", required=True, help="Project UUID")
    parser.add_argument("--baseline-set", required=True, help="Logical baseline set name")
    parser.add_argument(
        "--input",
        default="-",
        help="UTF-8 JSONL file containing one {'text': ...} object per line, or - for stdin",
    )
    parser.add_argument("--batch-size", type=int, default=32, choices=range(1, 129))
    parser.add_argument(
        "--no-activate",
        action="store_true",
        help="seed and cache the set without switching project traffic to it",
    )
    return parser


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parser().parse_args()
    try:
        return asyncio.run(_run(args))
    except (BaselineValidationError, ValueError) as exc:
        LOGGER.error("baseline seeding rejected: %s", exc)
        return 2
    except Exception as exc:
        LOGGER.error("baseline seeding failed: %s", type(exc).__name__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
