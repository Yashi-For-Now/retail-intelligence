# Batch ingestion of raw CSV data
# Reads the online retail II dataset in chunks and validates basic structure

import pandas as pd
import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# config
load_dotenv()

RAW_DATA_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
CHUNK_SIZE = 50_000  # rows per batch

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

log = logging.getLogger(__name__)

# Expected_Columns
REQUIRED_COLUMNS = {
    "Invoice",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "Price",
    "Customer ID",
    "Country",
}


def find_csv(directory: Path) -> Path:
    """Find the first CSV file in the given directory"""
    csvs = list(directory.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No CSV file found in {directory}")
    log.info(f"Found dataset: {csvs[0].name}")
    return csvs[0]


def validate_columns(df: pd.DataFrame, filepath: Path) -> None:
    """Check all required columns are present"""
    actual = set(df.columns)
    missing = REQUIRED_COLUMNS - actual
    if missing:
        raise ValueError(f"Missing columns in {filepath.name}: {missing}")
    log.info("Column validation passed")


def ingest_data(csv_path: Path) -> pd.DataFrame:
    """
    Read CSV in chunks of 50k rows
    Logs progress per batch and returns the full dataframe
    """

    chunks = []
    total_rows = 0
    batch_num = 0

    log.info(f"Starting ingestion: {csv_path.name}")
    log.info(f"Chunk size: {CHUNK_SIZE:,} rows")

    for chunk in pd.read_csv(
        csv_path,
        chunksize=CHUNK_SIZE,
        encoding="utf-8",
        on_bad_lines="skip",
        low_memory=False,
    ):
        batch_num += 1
        total_rows += len(chunk)
        chunks.append(chunk)
        log.info(
            f"Batch {batch_num}: {len(chunk):,} rows loaded. Total so far: {total_rows}"
        )

    df = pd.concat(chunks, ignore_index=True)
    log.info(
        f"Ingestion complete - {total_rows,} total rows, {len(df.columns)} columns"
    )
    return df


def save_processed(df: pd.DataFrame) -> Path:
    """Save the ingested dataframe as a parquet file for PySpark to read next"""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PROCESSED_DIR / "retail_raw.parquet"
    df.to_parquet(out_path, index=False)
    log.info(f"Saved to {out_path} ({out_path.stat().st_size/1_000_000:.1f} MB)")
    return out_path


def main():
    log.info("=" * 50)
    log.info("RETAIL INTELLIGENCE - INGEST STAGE")
    log.info("=" * 50)

    csv_path = find_csv(RAW_DATA_DIR)

    sample = pd.read_csv(csv_path, nrows=5, encoding="utf-8", low_memory=False)
    validate_columns(sample, csv_path)

    df = ingest_data(csv_path)

    log.info("─" * 50)
    log.info(f"Shape          : {df.shape[0]:,} rows × {df.shape[1]} columns")
    log.info(f"Null values    : {df.isnull().sum().sum():,} total")
    log.info(f"Duplicate rows : {df.duplicated().sum():,}")
    log.info(f"Date range     : {df['InvoiceDate'].min()} → {df['InvoiceDate'].max()}")
    log.info(f"Countries      : {df['Country'].nunique()}")
    log.info("─" * 50)

    save_processed(df)

    log.info("Ingest stage complete.")


if __name__ == "__main__":
    main()
