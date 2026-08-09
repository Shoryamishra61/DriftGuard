"""Build-time MiniLM cache warmer used by the Zerops worker image."""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from .embedding import SentenceTransformerEmbedder

DEFAULT_MODEL_SOURCE = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_MODEL_REVISION = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"


async def _preload() -> None:
    source = os.environ.get("EMBEDDING_MODEL_SOURCE", DEFAULT_MODEL_SOURCE).strip()
    revision = os.environ.get(
        "EMBEDDING_MODEL_REVISION",
        DEFAULT_MODEL_REVISION,
    ).strip()
    destination_text = os.environ.get("EMBEDDING_MODEL_PATH", "models/all-MiniLM-L6-v2").strip()
    if not source or not revision or not destination_text:
        raise ValueError("embedding model source, revision, and destination may not be empty")
    embedder = await SentenceTransformerEmbedder.load(
        source,
        dimension=384,
        revision=revision,
    )
    await embedder.save(Path(destination_text))


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_preload())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
