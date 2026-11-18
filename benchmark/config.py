"""Which data is measured, where it comes from and where results are written.

Everything that identifies the dataset lives in this single module: the remote
URLs, the local paths and the sampling knobs. Changing the category or the
source table is one edit here instead of a search across the benchmark.
"""

from __future__ import annotations

from pathlib import Path

DATASET_ID = "McAuley-Lab/Amazon-Reviews-2023"
HF_RESOLVE = (
    "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/"
)
REVIEW_URL = HF_RESOLVE + "benchmark/0core/rating_only/Home_and_Kitchen.csv"
REVIEW_JSONL_URL = HF_RESOLVE + "raw/review_categories/Home_and_Kitchen.jsonl"
META_URL = HF_RESOLVE + "raw/meta_categories/meta_Home_and_Kitchen.jsonl"

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw" / "amazon_hk"
SAMPLE_DIR = REPO_ROOT / "data" / "samples"
OUTPUT_DIR = REPO_ROOT / "output" / "measurements"
TABLES_DIR = OUTPUT_DIR / "tables"

REVIEW_RAW = RAW_DIR / "Home_and_Kitchen.0core.rating_only.csv"
REVIEW_JSONL_RAW = RAW_DIR / "reviews_Home_and_Kitchen_raw.jsonl"
META_SAMPLE = RAW_DIR / "meta_Home_and_Kitchen_1pct.jsonl"
SAMPLE_PATH = SAMPLE_DIR / "amazon_hk_reviews_1pct.parquet"

# Facts of the text route (T1): the raw reviews converted once to Parquet,
# plus the ladder samples derived from it.
RAW_TEXT_DIR = SAMPLE_DIR / "ladder_raw"
RAW_TEXT_FACTS = RAW_TEXT_DIR / "facts_with_text.parquet"


def tables_dir_for(engine: str) -> Path:
    """Per-engine output directory: Spark writes part-file directories and
    pandas writes single files, so both cannot share one table path."""
    return TABLES_DIR / engine

DEFAULT_FRACTION = 0.01
DEFAULT_META_WINDOWS = 200
META_LINE_MARGIN = 1024 * 1024
DOWNLOAD_PROGRESS_MB = 512
