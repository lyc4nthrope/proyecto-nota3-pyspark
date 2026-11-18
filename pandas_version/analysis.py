"""pandas transformations of the Home_and_Kitchen analysis.

Mirror of ``spark_version.analysis``: the same cleaning rules, the same derived
columns and the same aggregates, so the two engines can be compared and their
outputs checked for equivalence. This module is the deliverable, not the
measurement harness.

Equivalence notes (why some values are computed in a non-obvious way):

- Spark's ``dayofweek`` is 1=Sunday .. 7=Saturday; pandas' ``dt.dayofweek`` is
  0=Monday .. 6=Sunday. The mapping below reproduces Spark's numbering so the
  ``review_dow`` groups match one to one.
- Spark timestamps are interpreted in the session time zone, which the project
  pins to UTC. The derived columns therefore use UTC and are then made naive,
  matching Spark's naive-UTC timestamp values.
- ``stddev`` in Spark and ``std`` in pandas both use ddof=1. Averages and
  standard deviations are rounded to 4 decimals in both engines; the two
  libraries use different rounding modes (half-up vs half-even) so the last
  decimal may differ on exact ties. Equivalence checks compare with tolerance.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

CATALOG_COLUMNS = [
    "parent_asin",
    "main_category",
    "store",
    "price",
    "average_rating",
    "rating_number",
]


def read_parquet(path: Path) -> pd.DataFrame:
    """Read a Parquet file or a Spark-written part directory.

    pyarrow's dataset discovery only picks up ``*.parquet`` parts and ignores
    Spark's ``_SUCCESS`` and ``.crc`` sidecars, so a Spark output directory
    reads as one frame without any manual concatenation (which would double
    the peak memory at exactly the sizes this benchmark exists to measure).
    """
    return pd.read_parquet(Path(path))


def load_catalog(catalog_path: Path) -> pd.DataFrame:
    """Read the projected product catalog and mark every row as matched."""
    catalog = read_parquet(catalog_path)[CATALOG_COLUMNS].copy()
    catalog["_meta_matched"] = True
    return catalog


def clean_reviews(reviews: pd.DataFrame) -> pd.DataFrame:
    """Drop incomplete rows and out-of-range values."""
    cleaned = reviews.dropna(
        subset=["user_id", "parent_asin", "rating", "timestamp"]
    )
    return cleaned[
        (cleaned["rating"] >= 1)
        & (cleaned["rating"] <= 5)
        & (cleaned["timestamp"] > 0)
    ]


def add_derived_columns(reviews: pd.DataFrame) -> pd.DataFrame:
    """Add the date parts the temporal questions need, plus text_length.

    ``text_length`` is the number of characters of ``text`` (0 when the text
    is null or the source has no text column at all), matching Spark's
    ``coalesce(length(text), 0)``.
    """
    derived = reviews.copy()
    # UTC because the Spark session is pinned to UTC; made naive to match it.
    review_ts = pd.to_datetime(
        derived["timestamp"], unit="ms", utc=True
    ).dt.tz_localize(None)
    derived["review_ts"] = review_ts
    derived["review_date"] = review_ts.dt.date
    derived["review_year"] = review_ts.dt.year
    derived["review_month"] = review_ts.dt.month
    derived["review_dow"] = ((review_ts.dt.day_of_week + 1) % 7) + 1
    derived["review_hour"] = review_ts.dt.hour
    if "text" in derived.columns:
        derived["text_length"] = derived["text"].fillna("").str.len().astype("int64")
    else:
        derived["text_length"] = 0
    return derived


def join_with_catalog(
    reviews: pd.DataFrame, catalog: pd.DataFrame
) -> pd.DataFrame:
    """Left join: every review keeps its row, matched products add catalog columns."""
    return reviews.merge(catalog, on="parent_asin", how="left", sort=False)


def aggregate_by_store(reviews: pd.DataFrame) -> pd.DataFrame:
    """Volume, rating quality and text size per store (the group-by question)."""
    aggregated = reviews.groupby("store", dropna=False, sort=False).agg(
        reviews=("parent_asin", "size"),
        products=("parent_asin", "nunique"),
        avg_rating=("rating", "mean"),
        stddev_rating=("rating", "std"),
        avg_text_length=("text_length", "mean"),
    ).reset_index()
    aggregated["avg_rating"] = aggregated["avg_rating"].round(4)
    aggregated["stddev_rating"] = aggregated["stddev_rating"].round(4)
    aggregated["avg_text_length"] = aggregated["avg_text_length"].round(4)
    return aggregated


def aggregate_by_time(reviews: pd.DataFrame) -> pd.DataFrame:
    """Volume, rating quality and text size per year/month/weekday."""
    aggregated = reviews.groupby(
        ["review_year", "review_month", "review_dow"],
        dropna=False,
        sort=False,
    ).agg(
        reviews=("parent_asin", "size"),
        avg_rating=("rating", "mean"),
        stddev_rating=("rating", "std"),
        avg_text_length=("text_length", "mean"),
    ).reset_index()
    aggregated["avg_rating"] = aggregated["avg_rating"].round(4)
    aggregated["stddev_rating"] = aggregated["stddev_rating"].round(4)
    aggregated["avg_text_length"] = aggregated["avg_text_length"].round(4)
    return aggregated


def write_outputs(
    store_aggregate: pd.DataFrame,
    time_aggregate: pd.DataFrame,
    tables_dir: Path,
) -> int:
    """Write both aggregate tables as Parquet and return the part-file count."""
    tables_dir = Path(tables_dir)
    tables_dir.mkdir(parents=True, exist_ok=True)
    store_aggregate.to_parquet(tables_dir / "agg_by_store.parquet", index=False)
    time_aggregate.to_parquet(tables_dir / "agg_by_time.parquet", index=False)
    files = [
        path for path in tables_dir.rglob("*.parquet") if path.is_file()
    ]
    return len(files)
