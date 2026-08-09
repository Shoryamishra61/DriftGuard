"""Executable entry point for the DriftGuard background worker."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app_worker.config import WorkerConfig
from app_worker.readiness import refresh_readiness_marker, remove_readiness_marker
from app_worker.worker import DriftWorker

LOGGER = logging.getLogger("driftguard.worker.main")


def _install_signal_handlers(worker: DriftWorker) -> None:
    loop = asyncio.get_running_loop()

    def begin_shutdown() -> None:
        remove_readiness_marker()
        worker.request_shutdown()

    for signal_number in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signal_number, begin_shutdown)
        except (NotImplementedError, RuntimeError):
            signal.signal(
                signal_number,
                lambda _signum, _frame: loop.call_soon_threadsafe(begin_shutdown),
            )


async def _run() -> None:
    remove_readiness_marker()
    worker: DriftWorker | None = None
    try:
        config = WorkerConfig.from_env()
        worker = await DriftWorker.create(config)
        _install_signal_handlers(worker)
        refresh_readiness_marker()
        await worker.run()
    finally:
        remove_readiness_marker()
        if worker is not None:
            await worker.close()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        LOGGER.critical("worker terminated during startup: %s", type(exc).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
