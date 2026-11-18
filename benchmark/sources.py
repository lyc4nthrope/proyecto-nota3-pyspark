"""HTTP download and deterministic sampling of the source tables.

Why this sample shape:

- The raw review JSONL is 31.4 GB (67.4M reviews with text); the metadata JSONL
  is 11.8 GB. Downloading both to keep 1% is wasteful for a probe.
- The published 0core rating table is 3.86 GB and contains every interaction
  (user_id, parent_asin, rating, timestamp), without text.
- The product sample is 1% of metadata bytes in deterministic, line-aligned
  windows. Every 0core interaction for those products is kept. That gives a
  coherent product-cluster sample and a real catalog join, and the actual review
  fraction is measured and reported.

This module talks to the network and to disk only: it needs neither Spark nor
Java, so it can run and be tested on any machine.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

from benchmark.config import (
    DOWNLOAD_PROGRESS_MB,
    META_LINE_MARGIN,
    META_SAMPLE,
    META_URL,
    RAW_DIR,
    REVIEW_RAW,
    REVIEW_URL,
)


def metadata_window_ranges(
    total_bytes: int,
    fraction: float,
    windows: int,
    margin: int,
) -> list[dict]:
    """Return deterministic line-aligned byte windows covering ``fraction``.

    Each window keeps ``target`` bytes of complete JSONL lines starting at
    ``start``. ``end`` adds ``margin`` so the caller can finish the last line;
    only ``target`` bytes are kept, so the selected byte fraction is the
    requested one (up to one line per window).
    """
    if total_bytes <= 0:
        raise ValueError("total_bytes must be positive")
    if not 0 < fraction <= 1:
        raise ValueError("fraction must be in (0, 1]")
    if windows <= 0:
        raise ValueError("windows must be positive")
    span = total_bytes / windows
    target = max(1, int(total_bytes * fraction / windows))
    ranges = []
    for index in range(windows):
        start = int(index * span)
        end = min(total_bytes - 1, start + target + margin - 1)
        ranges.append(
            {"index": index, "start": start, "end": end, "target": target}
        )
    return ranges


def complete_lines(chunk: bytes, drop_first: bool) -> list[bytes]:
    """Split a byte chunk into complete lines, dropping partial edges."""
    parts = chunk.split(b"\n")
    if drop_first and parts:
        parts = parts[1:]
    if chunk.endswith(b"\n"):
        if parts and parts[-1] == b"":
            parts = parts[:-1]
    elif parts:
        parts = parts[:-1]
    return [part for part in parts if part]


def _user_agent() -> dict[str, str]:
    return {"User-Agent": "proyecto-nota3-medidor/1.0"}


def head_file(url: str) -> tuple[int, str]:
    request = Request(url, method="HEAD", headers=_user_agent())
    with urlopen(request, timeout=120) as response:
        size = int(response.headers.get("Content-Length") or 0)
        etag = (response.headers.get("ETag") or "").strip('"')
    if size <= 0:
        raise IOError(f"no Content-Length for {url}")
    return size, etag


def fetch_range(url: str, start: int, end: int, attempts: int = 3) -> bytes:
    for attempt in range(1, attempts + 1):
        try:
            request = Request(
                url,
                headers={**_user_agent(), "Range": f"bytes={start}-{end}"},
            )
            with urlopen(request, timeout=180) as response:
                if response.status != 206:
                    raise IOError(
                        f"expected HTTP 206 for range {start}-{end}, "
                        f"got {response.status}"
                    )
                return response.read()
        except Exception:
            if attempt == attempts:
                raise
            print(
                f"[meta] retry range {start}-{end} "
                f"({attempt}/{attempts - 1})",
                flush=True,
            )
            time.sleep(2 * attempt)
    raise RuntimeError("unreachable")


def _download_once(
    url: str,
    destination: Path,
    expected_size: int,
    progress_mb: int = DOWNLOAD_PROGRESS_MB,
) -> dict:
    """Download ``url`` to ``destination``, resuming any leftover partial file.

    The ``.part`` file is kept when an attempt fails, so the next one
    continues from where the previous stopped using an HTTP ``Range``
    request. A server that ignores the range (HTTP 200 instead of 206) makes
    the attempt restart cleanly from zero. This matters for multi-GB files:
    without it, an interruption at 90 % costs the whole download again.
    """
    partial = destination.with_suffix(destination.suffix + ".part")
    offset = partial.stat().st_size if partial.exists() else 0
    headers = _user_agent()
    if offset:
        headers["Range"] = f"bytes={offset}-"
    request = Request(url, headers=headers)
    block = progress_mb * 1024**2
    with urlopen(request, timeout=300) as response:
        status = getattr(response, "status", 200)
        if offset and status != 206:
            offset = 0  # the server ignored the range: start over
        written = offset
        next_report = (offset // block + 1) * block
        with open(partial, "ab" if offset else "wb") as handle:
            while True:
                chunk = response.read(1 << 20)
                if not chunk:
                    break
                handle.write(chunk)
                written += len(chunk)
                if written >= next_report:
                    print(
                        f"[source] {destination.name}: "
                        f"{written / (1024**2):.0f} / "
                        f"{expected_size / (1024**2):.0f} MB",
                        flush=True,
                    )
                    next_report += block
    if written != expected_size:
        raise IOError(
            f"incomplete download: {written} of {expected_size} bytes"
        )
    os.replace(partial, destination)
    return {"url": url, "bytes": written, "resumed_from": offset}


def download_full(
    url: str,
    destination: Path,
    expected_size: int,
    force: bool = False,
    attempts: int = 3,
) -> dict:
    if (
        not force
        and destination.exists()
        and destination.stat().st_size == expected_size
    ):
        print(f"[source] reusing {destination.name}", flush=True)
        return {"url": url, "bytes": expected_size, "cached": True}
    for attempt in range(1, attempts + 1):
        try:
            info = _download_once(url, destination, expected_size)
            print(
                f"[source] downloaded {destination.name}: "
                f"{info['bytes'] / (1024**2):.1f} MB",
                flush=True,
            )
            return {**info, "cached": False}
        except Exception:
            if attempt == attempts:
                raise
            print(
                f"[source] retry {attempt}/{attempts - 1} for {destination.name}",
                flush=True,
            )
            time.sleep(5 * attempt)
    raise RuntimeError("unreachable")


def sample_metadata(
    url: str,
    total_bytes: int,
    etag: str,
    destination: Path,
    fraction: float,
    windows: int,
    margin: int,
    force: bool = False,
) -> dict:
    """Keep ~``fraction`` of metadata bytes using deterministic windows."""
    manifest_path = destination.with_suffix(".manifest.json")
    if not force and destination.exists() and manifest_path.exists():
        prior = json.loads(manifest_path.read_text(encoding="utf-8"))
        expected = {
            "url": url,
            "total_bytes": total_bytes,
            "etag": etag,
            "fraction": fraction,
            "windows": windows,
            # Cache key: the probe entry point plus the sampling algorithm
            # version. Keep it stable or every stored sample is rebuilt.
            "tool": "benchmark.measure_sample/meta-windows-v1",
        }
        if all(prior.get(key) == value for key, value in expected.items()):
            print(f"[meta] reusing {destination.name}", flush=True)
            return prior

    started = time.perf_counter()
    ranges = metadata_window_ranges(total_bytes, fraction, windows, margin)
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    seen: set[str] = set()
    products = 0
    lines = 0
    parse_errors = 0
    fetched_bytes = 0
    kept_bytes = 0
    with partial.open("w", encoding="utf-8", newline="\n") as output:
        for item in ranges:
            chunk = fetch_range(url, item["start"], item["end"])
            fetched_bytes += len(chunk)
            window_kept = 0
            for raw_line in complete_lines(
                chunk, drop_first=item["start"] > 0
            ):
                if window_kept >= item["target"]:
                    break
                try:
                    record = json.loads(raw_line.decode("utf-8"))
                except Exception:
                    parse_errors += 1
                    continue
                parent_asin = record.get("parent_asin")
                if not parent_asin:
                    continue
                window_kept += len(raw_line) + 1
                kept_bytes += len(raw_line) + 1
                if parent_asin in seen:
                    continue
                seen.add(parent_asin)
                output.write(raw_line.decode("utf-8") + "\n")
                products += 1
                lines += 1
            if (item["index"] + 1) % 25 == 0:
                print(
                    f"[meta] window {item['index'] + 1}/{windows}: "
                    f"{products:,} products",
                    flush=True,
                )
    elapsed = time.perf_counter() - started
    if products == 0:
        partial.unlink(missing_ok=True)
        raise IOError("metadata sampling returned zero products")
    os.replace(partial, destination)
    manifest = {
        "url": url,
        "total_bytes": total_bytes,
        "etag": etag,
        "fraction": fraction,
        "windows": windows,
        "line_margin": margin,
        "tool": "benchmark.measure_sample/meta-windows-v1",
        "products": products,
        "lines": lines,
        "parse_errors": parse_errors,
        "fetched_bytes": fetched_bytes,
        "kept_bytes": kept_bytes,
        "sample_bytes": destination.stat().st_size,
        "seconds": round(elapsed, 3),
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8"
    )
    print(
        f"[meta] sampled {products:,} products from "
        f"{fetched_bytes / (1024**2):.1f} MB in {elapsed:.1f}s",
        flush=True,
    )
    return manifest


def ensure_sources(
    fraction: float, windows: int, force: bool = False
) -> dict:
    """Download the 0core interactions and build the metadata product sample."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    review_size, review_etag = head_file(REVIEW_URL)
    meta_size, meta_etag = head_file(META_URL)
    review_info = download_full(
        REVIEW_URL, REVIEW_RAW, review_size, force=force
    )
    meta_manifest = sample_metadata(
        META_URL,
        meta_size,
        meta_etag,
        META_SAMPLE,
        fraction,
        windows,
        META_LINE_MARGIN,
        force=force,
    )
    return {
        "review_path": REVIEW_RAW,
        "review_url": REVIEW_URL,
        "review_bytes": review_size,
        "review_etag": review_etag,
        "review_cached": review_info.get("cached", False),
        "meta_path": META_SAMPLE,
        "meta_url": META_URL,
        "meta_bytes": meta_size,
        "meta_etag": meta_etag,
        "meta_manifest": meta_manifest,
    }
