"""A failed download keeps its partial file and resumes with HTTP Range.

The raw review JSONL is 31 GB. Without resume, an interruption near the end
costs the whole download again; that is why ``_download_once`` keeps the
``.part`` file and continues from its size.
"""

from __future__ import annotations

import pytest

from benchmark.sources import _download_once


class FakeResponse:
    """Minimal file-like HTTP response; items may be bytes or exceptions."""

    def __init__(self, items, status: int = 200) -> None:
        self._items = list(items)
        self.status = status

    def read(self, size: int = -1) -> bytes:
        if not self._items:
            return b""
        item = self._items.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


def test_failed_attempt_keeps_partial_and_next_attempt_resumes(
    monkeypatch, tmp_path
):
    destination = tmp_path / "big.jsonl"
    partial = tmp_path / "big.jsonl.part"
    seen_ranges: list = []

    def fake_urlopen(request, timeout=None):
        seen_ranges.append(request.get_header("Range"))
        if len(seen_ranges) == 1:
            return FakeResponse([b"a" * 400, IOError("connection reset")], 200)
        return FakeResponse([b"a" * 600, b""], 206)

    monkeypatch.setattr("benchmark.sources.urlopen", fake_urlopen)

    with pytest.raises(IOError):
        _download_once("https://example.test/big.jsonl", destination, 1000)

    assert partial.exists()
    assert partial.stat().st_size == 400

    result = _download_once("https://example.test/big.jsonl", destination, 1000)

    assert seen_ranges == [None, "bytes=400-"]
    assert result["resumed_from"] == 400
    assert destination.read_bytes() == b"a" * 1000
    assert not partial.exists()


def test_server_ignoring_the_range_restarts_cleanly(monkeypatch, tmp_path):
    destination = tmp_path / "big.jsonl"
    seen_ranges: list = []

    def fake_urlopen(request, timeout=None):
        seen_ranges.append(request.get_header("Range"))
        if len(seen_ranges) == 1:
            return FakeResponse([b"a" * 400, IOError("connection reset")], 200)
        return FakeResponse([b"a" * 1000, b""], 200)  # 200: range ignored

    monkeypatch.setattr("benchmark.sources.urlopen", fake_urlopen)

    with pytest.raises(IOError):
        _download_once("https://example.test/big.jsonl", destination, 1000)
    result = _download_once("https://example.test/big.jsonl", destination, 1000)

    assert seen_ranges == [None, "bytes=400-"]
    assert result["resumed_from"] == 0
    assert destination.read_bytes() == b"a" * 1000
    assert not (tmp_path / "big.jsonl.part").exists()


def test_incomplete_download_keeps_partial_for_the_next_attempt(
    monkeypatch, tmp_path
):
    destination = tmp_path / "big.jsonl"

    def fake_urlopen(request, timeout=None):
        return FakeResponse([b"a" * 300, b""], 200)

    monkeypatch.setattr("benchmark.sources.urlopen", fake_urlopen)

    with pytest.raises(IOError):
        _download_once("https://example.test/big.jsonl", destination, 1000)

    assert (tmp_path / "big.jsonl.part").stat().st_size == 300
