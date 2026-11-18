"""Guards the deterministic sampling and error capture of the Amazon medidor.

The medidor samples 1% of the product metadata by line-aligned byte windows and
then keeps every 0core interaction for those products. The window plan must be
deterministic and the line splitter must never emit a partial JSON line.
"""

from benchmark.instrument import measure_operation
from benchmark.sources import complete_lines, metadata_window_ranges


def test_metadata_windows_are_deterministic_and_bounded():
    total = 100_000_000
    first = metadata_window_ranges(total, 0.01, 200, 1_024)
    second = metadata_window_ranges(total, 0.01, 200, 1_024)

    assert first == second
    assert len(first) == 200
    for item in first:
        assert 0 <= item["start"] < item["end"] < total
        assert item["target"] > 0
    for previous, current in zip(first, first[1:]):
        assert previous["start"] < current["start"]
        assert previous["start"] + previous["target"] <= current["start"]
    assert sum(item["target"] for item in first) <= total * 0.01 + 200


def test_complete_lines_drops_partial_edges():
    chunk = b"partial\nfull-1\nfull-2\npartial-2"
    assert complete_lines(chunk, drop_first=True) == [b"full-1", b"full-2"]
    assert complete_lines(b"full-1\nfull-2\n", drop_first=False) == [
        b"full-1",
        b"full-2",
    ]
    assert complete_lines(b"partial", drop_first=False) == []
    assert complete_lines(b"full\n", drop_first=False) == [b"full"]


def test_measure_operation_records_exception_without_raising():
    def boom():
        raise RuntimeError("boom")

    step = measure_operation("boom", boom)

    assert step["error"] == "RuntimeError: boom"
    assert step["time_s"] >= 0
    assert step["peak_mb"] >= step["baseline_mb"] > 0
