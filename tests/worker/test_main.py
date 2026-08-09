from pathlib import Path

from app_worker import readiness


def test_readiness_marker_is_created_and_removed_atomically(
    tmp_path: Path,
    monkeypatch,
) -> None:
    marker = tmp_path / "driftguard-worker-ready"
    monkeypatch.setattr(readiness, "READINESS_MARKER", marker)

    readiness.refresh_readiness_marker()

    assert marker.read_text(encoding="utf-8").strip()
    assert list(tmp_path.glob(".*.tmp")) == []

    readiness.remove_readiness_marker()
    assert not marker.exists()
