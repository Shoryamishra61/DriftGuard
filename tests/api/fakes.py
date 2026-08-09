"""Small async fakes that preserve transaction and statement evidence."""

from __future__ import annotations

from collections import defaultdict
from typing import Any


class FakeResult:
    def __init__(self, rows: list[dict[str, Any]] | None = None, scalar: Any = None):
        self.rows = rows or []
        self.scalar = scalar

    def mappings(self) -> FakeResult:
        return self

    def first(self) -> dict[str, Any] | None:
        return self.rows[0] if self.rows else None

    def one_or_none(self) -> dict[str, Any] | None:
        if len(self.rows) > 1:
            raise AssertionError("expected at most one row")
        return self.first()

    def one(self) -> dict[str, Any]:
        if len(self.rows) != 1:
            raise AssertionError("expected exactly one row")
        return self.rows[0]

    def scalar_one_or_none(self) -> Any:
        return self.scalar

    def scalar_one(self) -> Any:
        if self.scalar is None:
            raise AssertionError("expected one scalar value")
        return self.scalar

    def __iter__(self):
        return iter(self.rows)


class FakeTransaction:
    def __init__(self, events: list[str]):
        self.events = events

    async def __aenter__(self) -> FakeTransaction:
        self.events.append("transaction.begin")
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> bool:
        self.events.append("transaction.rollback" if exc_type else "transaction.commit")
        return False


class FakeSession:
    def __init__(
        self,
        *,
        results: list[FakeResult] | None = None,
        failure: BaseException | None = None,
        fail_on_execute: int | None = None,
        events: list[str] | None = None,
    ):
        self.results = list(results or [])
        self.failure = failure
        self.fail_on_execute = fail_on_execute
        self.events = events if events is not None else []
        self.statements: list[Any] = []

    async def __aenter__(self) -> FakeSession:
        self.events.append("session.enter")
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> bool:
        self.events.append("session.exit")
        return False

    def begin(self) -> FakeTransaction:
        return FakeTransaction(self.events)

    async def execute(self, statement) -> FakeResult:
        self.statements.append(statement)
        execute_number = len(self.statements)
        self.events.append(f"execute.{execute_number}")
        if self.failure is not None and execute_number == self.fail_on_execute:
            raise self.failure
        return self.results.pop(0) if self.results else FakeResult()

    async def rollback(self) -> None:
        self.events.append("session.rollback")


class FakeSessionFactory:
    def __init__(self, session: FakeSession):
        self.session = session

    def __call__(self) -> FakeSession:
        return self.session


class FakeValkey:
    def __init__(self):
        self.markers: set[str] = set()
        self.queues: dict[str, list[str]] = defaultdict(list)
        self.eval_calls: list[tuple[Any, ...]] = []
        self.values: dict[str, str] = {}
        self.raise_on_eval: BaseException | None = None

    async def eval(self, script: str, numkeys: int, *args):
        self.eval_calls.append((script, numkeys, *args))
        if self.raise_on_eval is not None:
            raise self.raise_on_eval
        marker, queue, payload, _ttl = args
        if marker in self.markers:
            return 0
        self.markers.add(marker)
        self.queues[queue].insert(0, payload)
        return 1

    async def ping(self) -> bool:
        return True

    async def llen(self, queue: str) -> int:
        return len(self.queues[queue])

    async def get(self, key: str):
        return self.values.get(key)

    async def aclose(self) -> None:
        return None
