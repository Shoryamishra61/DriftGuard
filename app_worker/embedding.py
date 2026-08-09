"""Bounded local sentence embedding adapter."""

from __future__ import annotations

import asyncio
import math
from pathlib import Path
from typing import Any


class EmbeddingError(RuntimeError):
    """Raised when the local model does not produce a safe 384-vector."""


class SentenceTransformerEmbedder:
    def __init__(self, model: Any, dimension: int) -> None:
        self._model = model
        self._dimension = dimension

    @classmethod
    async def load(
        cls,
        model_name: str,
        *,
        dimension: int = 384,
        revision: str | None = None,
        local_files_only: bool = False,
    ) -> SentenceTransformerEmbedder:
        from sentence_transformers import SentenceTransformer

        load_options = {"revision": revision} if revision is not None else {}
        load_options["local_files_only"] = local_files_only
        model = await asyncio.to_thread(
            SentenceTransformer,
            model_name,
            **load_options,
        )
        reported_dimension = model.get_sentence_embedding_dimension()
        if reported_dimension != dimension:
            raise EmbeddingError(
                f"{model_name} produces {reported_dimension} dimensions, expected {dimension}"
            )
        return cls(model, dimension)

    async def embed(self, text: str) -> list[float]:
        vectors = await self.embed_batch([text])
        return vectors[0]

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        encoded = await asyncio.to_thread(
            self._model.encode,
            texts,
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        raw_vectors = encoded.tolist() if hasattr(encoded, "tolist") else list(encoded)
        if len(raw_vectors) != len(texts):
            raise EmbeddingError("embedding batch size does not match input batch size")
        results: list[list[float]] = []
        for raw in raw_vectors:
            if len(raw) != self._dimension:
                raise EmbeddingError(
                    f"embedding has {len(raw)} dimensions, expected {self._dimension}"
                )
            result = [float(value) for value in raw]
            if not all(math.isfinite(value) for value in result):
                raise EmbeddingError("embedding contains a non-finite value")
            results.append(result)
        return results

    async def save(self, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._model.save, str(destination))
