# Data profiling — runs BEFORE ingest/transform
# Understands the raw data's shape, quality issues and distribution
# Findings from this file directly informed the cleaning strategy in transform.py

import pandas as pd
import numpy as np
from pathlib import Path
import logging
import json
from datetime import datetime

#  Config
RAW_DATA_DIR = Path("data/raw")
REPORT_DIR = Path("data/processed")

#  Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


#  Load raw data
def load_raw(directory: Path) -> pd.DataFrame:
    csvs = list(directory.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No CSV found in {directory}")
    log.info(f"Loading raw file: {csvs[0].name}")
    df = pd.read_csv(csvs[0], encoding="utf-8", on_bad_lines="skip", low_memory=False)
    log.info(f"Loaded {len(df):,} rows × {len(df.columns)} columns")
    return df


#  Profile 1: Basic shape
def profile_shape(df: pd.DataFrame) -> dict:
    log.info("── Profile 1: Basic Shape ──")
    result = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "duplicate_rows": int(df.duplicated().sum()),
        "duplicate_pct": round(df.duplicated().sum() / len(df) * 100, 2),
        "columns": list(df.columns),
    }
    log.info(f"  Total rows       : {result['total_rows']:,}")
    log.info(
        f"  Duplicate rows   : {result['duplicate_rows']:,} ({result['duplicate_pct']}%)"
    )
    return result


#  Profile 2: Null analysis
def profile_nulls(df: pd.DataFrame) -> dict:
    log.info("── Profile 2: Null Analysis ──")
    null_counts = df.isnull().sum()
    null_pcts = (null_counts / len(df) * 100).round(2)

    result = {}
    for col in df.columns:
        result[col] = {
            "null_count": int(null_counts[col]),
            "null_pct": float(null_pcts[col]),
        }
        if null_counts[col] > 0:
            log.info(f"  {col:<20} → {null_counts[col]:>8,} nulls ({null_pcts[col]}%)")

    return result


#  Profile 3: Cancellation analysis
def profile_cancellations(df: pd.DataFrame) -> dict:
    log.info("── Profile 3: Cancellation Analysis ──")

    total = len(df)
    cancelled = df["Invoice"].astype(str).str.startswith("C").sum()
    cancellation_rate = round(cancelled / total * 100, 2)

    result = {
        "total_invoices": total,
        "cancelled_invoices": int(cancelled),
        "cancellation_rate": cancellation_rate,
    }

    log.info(f"  Total rows           : {total:,}")
    log.info(f"  Cancellation rows    : {cancelled:,}")
    log.info(f"  Cancellation rate    : {cancellation_rate}%")
    log.info(
        f"  → Informed decision  : filter invoices starting with 'C' in transform.py"
    )

    return result


#  Profile 4: Quantity distribution
def profile_quantity(df: pd.DataFrame) -> dict:
    log.info("── Profile 4: Quantity Distribution ──")

    qty = pd.to_numeric(df["Quantity"], errors="coerce")

    result = {
        "min": float(qty.min()),
        "max": float(qty.max()),
        "mean": round(float(qty.mean()), 2),
        "median": float(qty.median()),
        "negative": int((qty < 0).sum()),
        "zero": int((qty == 0).sum()),
        "p99": float(qty.quantile(0.99)),
        "p999": float(qty.quantile(0.999)),
    }

    log.info(f"  Min              : {result['min']:,}")
    log.info(f"  Max              : {result['max']:,}")
    log.info(f"  Mean             : {result['mean']:,}")
    log.info(f"  Negative values  : {result['negative']:,}")
    log.info(f"  Zero values      : {result['zero']:,}")
    log.info(f"  99th percentile  : {result['p99']:,}")
    log.info(f"  → Informed decision: filter Quantity <= 0 in transform.py")

    return result


#  Profile 5: Price distribution
def profile_price(df: pd.DataFrame) -> dict:
    log.info("── Profile 5: Price Distribution ──")

    price = pd.to_numeric(df["Price"], errors="coerce")

    result = {
        "min": float(price.min()),
        "max": float(price.max()),
        "mean": round(float(price.mean()), 2),
        "zero_or_neg": int((price <= 0).sum()),
        "below_001": int((price < 0.01).sum()),
        "p99": float(price.quantile(0.99)),
    }

    log.info(f"  Min              : {result['min']}")
    log.info(f"  Max              : {result['max']}")
    log.info(f"  Zero/Negative    : {result['zero_or_neg']:,}")
    log.info(f"  Below £0.01      : {result['below_001']:,}")
    log.info(f"  99th percentile  : {result['p99']}")
    log.info(f"  → Informed decision: filter Price < 0.01 in transform.py")

    return result


#  Profile 6: Dataset scope
def profile_scope(df: pd.DataFrame) -> dict:
    log.info("── Profile 6: Dataset Scope ──")

    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")

    result = {
        "date_min": str(df["InvoiceDate"].min()),
        "date_max": str(df["InvoiceDate"].max()),
        "unique_customers": int(df["Customer ID"].nunique()),
        "unique_products": int(df["StockCode"].nunique()),
        "unique_countries": int(df["Country"].nunique()),
        "top_countries": df["Country"].value_counts().head(5).to_dict(),
    }

    log.info(f"  Date range         : {result['date_min']} → {result['date_max']}")
    log.info(f"  Unique customers   : {result['unique_customers']:,}")
    log.info(f"  Unique products    : {result['unique_products']:,}")
    log.info(f"  Unique countries   : {result['unique_countries']}")
    log.info(f"  Top 5 countries    : {result['top_countries']}")

    return result


# Save report
def save_report(report: dict) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / "data_profile_report.json"
    with open(out, "w") as f:
        json.dump(report, f, indent=2)
    log.info(f"Profile report saved → {out}")


#  Main
def main():
    log.info("=" * 50)
    log.info("RETAIL INTELLIGENCE — DATA PROFILING STAGE")
    log.info("=" * 50)

    df = load_raw(RAW_DATA_DIR)

    report = {
        "profiled_at": datetime.now().isoformat(),
        "shape": profile_shape(df),
        "nulls": profile_nulls(df),
        "cancellations": profile_cancellations(df),
        "quantity": profile_quantity(df),
        "price": profile_price(df),
        "scope": profile_scope(df),
    }

    save_report(report)

    log.info("=" * 50)
    log.info("PROFILING COMPLETE — KEY FINDINGS:")
    log.info("=" * 50)
    log.info(f"  Duplicate rows      : {report['shape']['duplicate_pct']}%")
    log.info(f"  Cancellation rate   : {report['cancellations']['cancellation_rate']}%")
    log.info(f"  Null Customer IDs   : {report['nulls']['Customer ID']['null_pct']}%")
    log.info(f"  Negative quantities : {report['quantity']['negative']:,}")
    log.info(f"  Sub-penny prices    : {report['price']['below_001']:,}")
    log.info("=" * 50)
    log.info("These findings directly informed the cleaning strategy in transform.py")
    log.info("Profiling stage complete.")


if __name__ == "__main__":
    main()
