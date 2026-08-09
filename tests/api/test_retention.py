from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app_api.retention import LegalHoldCreate

UTC = getattr(__import__("datetime"), "UTC", timezone.utc)  # noqa: UP017


def test_legal_hold_requires_timezone_and_ordered_range() -> None:
    with pytest.raises(ValidationError, match="timezone"):
        LegalHoldCreate(
            starts_at=datetime(2026, 8, 1),
            reason="Court preservation order",
        )

    with pytest.raises(ValidationError, match="must not precede"):
        LegalHoldCreate(
            starts_at=datetime(2026, 8, 2, tzinfo=UTC),
            ends_at=datetime(2026, 8, 1, tzinfo=UTC),
            reason="Court preservation order",
        )


def test_open_ended_legal_hold_is_valid() -> None:
    hold = LegalHoldCreate(
        starts_at=datetime(2026, 8, 1, tzinfo=UTC),
        reason="Incident investigation",
    )

    assert hold.ends_at is None
    assert hold.reason == "Incident investigation"
