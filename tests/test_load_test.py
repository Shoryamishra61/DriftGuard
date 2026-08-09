from scripts.load_test import percentile, server_timing_ms


def test_percentile_uses_nearest_rank() -> None:
    values = [10.0, 30.0, 20.0, 40.0, 50.0]

    assert percentile(values, 0.50) == 30.0
    assert percentile(values, 0.95) == 50.0
    assert percentile([], 0.95) is None


def test_server_timing_parser_extracts_application_duration() -> None:
    assert server_timing_ms('db;dur=1.2, app;dur=4.875;desc="ASGI"') == 4.875
    assert server_timing_ms("app;dur=invalid") is None
    assert server_timing_ms(None) is None
