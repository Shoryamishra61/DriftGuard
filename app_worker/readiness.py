"""Atomic worker readiness marker lifecycle for Zerops exec probes."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

LOGGER = logging.getLogger("driftguard.worker.readiness")
READINESS_MARKER = Path("/tmp/driftguard-worker-ready")  # noqa: S108
UTC = getattr(__import__("datetime"), "UTC", timezone.utc)  # noqa: UP017


def refresh_readiness_marker() -> None:
    temporary_marker = READINESS_MARKER.with_name(f".{READINESS_MARKER.name}.{os.getpid()}.tmp")
    try:
        temporary_marker.write_text(
            f"{datetime.now(UTC).isoformat()}\n",
            encoding="utf-8",
        )
        os.replace(temporary_marker, READINESS_MARKER)
    finally:
        temporary_marker.unlink(missing_ok=True)


def remove_readiness_marker() -> None:
    try:
        READINESS_MARKER.unlink(missing_ok=True)
    except OSError as exc:
        LOGGER.warning("could not remove worker readiness marker: %s", type(exc).__name__)
