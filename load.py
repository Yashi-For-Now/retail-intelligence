# load.py
# Stage 4: Load cleaned data into PostgreSQL star schema
# Creates 4 dimension tables + 1 fact table and loads data

import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# ── Config ────────────────────────────────────────────────────────────────────
load_dotenv()

PROCESSED_DIR = Path("data/processed")
INPUT_FILE = PROCESSED_DIR / "retail_cleaned.parquet"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "retail_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "postgres")

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

#  Logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


#  DB Connection 
def get_engine():
    log.info(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}")
    engine = create_engine(DB_URL)
    # Test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    log.info("Database connection successful")
    return engine


#  Create Schema 
def create_schema(engine):
    """
    Creates the star schema DDL.
    1 fact table: fact_sales
    4 dimension tables: dim_customer, dim_product, dim_date, dim_region
    """
    ddl = """
    -- ── Dimension: Customer ──────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_id     INTEGER PRIMARY KEY,
        country         VARCHAR(100)
    );

    -- ── Dimension: Product ───────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS dim_product (
        stock_code      VARCHAR(50) PRIMARY KEY,
        description     TEXT
    );

    -- ── Dimension: Date ──────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id         SERIAL PRIMARY KEY,
        full_date       TIMESTAMP,
        year            INTEGER,
        month           INTEGER,
        day             INTEGER,
        hour            INTEGER,
        weekday         INTEGER,
        is_weekend      BOOLEAN
    );

    -- ── Dimension: Region ────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS dim_region (
        region_id       SERIAL PRIMARY KEY,
        country         VARCHAR(100) UNIQUE,
        region_group    VARCHAR(100)  -- e.g. Europe, Asia, Americas
    );

    -- ── Fact Table: Sales ─────────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS fact_sales (
        sale_id         SERIAL PRIMARY KEY,
        invoice         VARCHAR(20),
        customer_id     INTEGER REFERENCES dim_customer(customer_id),
        stock_code      VARCHAR(50) REFERENCES dim_product(stock_code),
        date_id         INTEGER REFERENCES dim_date(date_id),
        region_id       INTEGER REFERENCES dim_region(region_id),
        quantity        INTEGER,
        price           DOUBLE PRECISION,
        revenue         DOUBLE PRECISION
    );
    """

    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
    log.info("Star schema created successfully")


#  Load: dim_customer
def load_dim_customer(df: pd.DataFrame, engine) -> None:
    dim = (
        df[["Customer ID", "Country"]]
        .drop_duplicates(subset=["Customer ID"])
        .rename(columns={"Customer ID": "customer_id", "Country": "country"})
        .dropna(subset=["customer_id"])
    )
    dim["customer_id"] = dim["customer_id"].astype(int)

    dim.to_sql("dim_customer", engine, if_exists="append", index=False, method="multi")
    log.info(f"dim_customer loaded: {len(dim):,} records")


#  Load: dim_product
def load_dim_product(df: pd.DataFrame, engine) -> None:
    dim = (
        df[["StockCode", "Description"]]
        .drop_duplicates(subset=["StockCode"])
        .rename(columns={"StockCode": "stock_code", "Description": "description"})
    )

    dim.to_sql("dim_product", engine, if_exists="append", index=False, method="multi")
    log.info(f"dim_product loaded: {len(dim):,} records")


#  Load: dim_date
def load_dim_date(df: pd.DataFrame, engine) -> pd.DataFrame:
    dim = (
        df[["InvoiceDate", "Year", "Month", "Day", "Hour", "Weekday"]]
        .drop_duplicates(subset=["InvoiceDate"])
        .rename(
            columns={
                "InvoiceDate": "full_date",
                "Year": "year",
                "Month": "month",
                "Day": "day",
                "Hour": "hour",
                "Weekday": "weekday",
            }
        )
    )

    # Weekday: 1=Sun, 7=Sat in PySpark → is_weekend if 1 or 7
    dim["is_weekend"] = dim["weekday"].isin([1, 7])

    dim.to_sql("dim_date", engine, if_exists="append", index=False, method="multi")

    # Fetch back with generated date_id for fact table join
    date_map = pd.read_sql("SELECT date_id, full_date FROM dim_date", engine)
    log.info(f"dim_date loaded: {len(dim):,} records")
    return date_map


#  Load: dim_region
def load_dim_region(df: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Assigns a region_group to each country.
    Simple rule-based mapping — extendable later.
    """
    europe = [
        "United Kingdom",
        "Germany",
        "France",
        "Spain",
        "Netherlands",
        "Belgium",
        "Switzerland",
        "Portugal",
        "Italy",
        "Norway",
        "Denmark",
        "Finland",
        "Sweden",
        "Austria",
        "Greece",
        "Cyprus",
        "Malta",
        "Poland",
        "Czech Republic",
        "Lithuania",
        "Iceland",
    ]
    americas = ["USA", "Canada", "Brazil"]
    asia = ["Japan", "China", "Singapore", "Hong Kong", "India", "Bahrain"]

    def assign_region(country):
        if country in europe:
            return "Europe"
        elif country in americas:
            return "Americas"
        elif country in asia:
            return "Asia"
        else:
            return "Other"

    dim = df[["Country"]].drop_duplicates().rename(columns={"Country": "country"})
    dim["region_group"] = dim["country"].apply(assign_region)

    dim.to_sql("dim_region", engine, if_exists="append", index=False, method="multi")

    # Fetch back with generated region_id for fact table join
    region_map = pd.read_sql("SELECT region_id, country FROM dim_region", engine)
    log.info(f"dim_region loaded: {len(dim):,} records")
    return region_map


#  Load: fact_sales
def load_fact_sales(
    df: pd.DataFrame, engine, date_map: pd.DataFrame, region_map: pd.DataFrame
) -> None:
    """
    Builds the fact table by joining dim keys back onto the cleaned dataframe.
    """
    fact = df.copy()

    # Ensure InvoiceDate is datetime for merge
    fact["InvoiceDate"] = pd.to_datetime(fact["InvoiceDate"])
    date_map["full_date"] = pd.to_datetime(date_map["full_date"])

    # Join date_id
    fact = fact.merge(date_map, left_on="InvoiceDate", right_on="full_date", how="left")

    # Join region_id
    fact = fact.merge(region_map, left_on="Country", right_on="country", how="left")

    # Select only fact table columns
    fact = fact[
        [
            "Invoice",
            "Customer ID",
            "StockCode",
            "date_id",
            "region_id",
            "Quantity",
            "Price",
            "Revenue",
        ]
    ].rename(
        columns={
            "Invoice": "invoice",
            "Customer ID": "customer_id",
            "StockCode": "stock_code",
            "Quantity": "quantity",
            "Price": "price",
            "Revenue": "revenue",
        }
    )

    fact["customer_id"] = fact["customer_id"].astype("Int64")

    # Load in batches of 10K rows to avoid memory spikes
    batch_size = 10_000
    total_rows = len(fact)
    batches = (total_rows // batch_size) + 1

    log.info(f"Loading fact_sales: {total_rows:,} rows in {batches} batches")

    for i in range(batches):
        start = i * batch_size
        end = min(start + batch_size, total_rows)
        batch = fact.iloc[start:end]

        if len(batch) == 0:
            break

        batch.to_sql(
            "fact_sales", engine, if_exists="append", index=False, method="multi"
        )
        log.info(f"  Batch {i+1}/{batches}: rows {start:,}–{end:,} loaded")

    log.info(f"fact_sales loaded: {total_rows:,} total records")


#  Verify load
def verify_load(engine) -> None:
    """
    Quick row count check on all tables after load.
    """
    tables = ["dim_customer", "dim_product", "dim_date", "dim_region", "fact_sales"]

    log.info("─" * 50)
    log.info("LOAD VERIFICATION")
    log.info("─" * 50)

    with engine.connect() as conn:
        for table in tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            log.info(f"  {table:<20} → {count:>10,} rows")

    log.info("─" * 50)


#  Main
def main():
    log.info("=" * 50)
    log.info("RETAIL INTELLIGENCE — LOAD STAGE")
    log.info("=" * 50)

    # 1. Load cleaned parquet
    log.info(f"Reading {INPUT_FILE}")
    df = pd.read_parquet(INPUT_FILE)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    log.info(f"Loaded {len(df):,} rows")

    # 2. Connect to DB
    engine = get_engine()

    # 3. Create star schema
    create_schema(engine)

    # 4. Load dimensions first (fact table references them)
    load_dim_customer(df, engine)
    load_dim_product(df, engine)
    date_map = load_dim_date(df, engine)
    region_map = load_dim_region(df, engine)

    # 5. Load fact table
    load_fact_sales(df, engine, date_map, region_map)

    # 6. Verify
    verify_load(engine)

    log.info("Load stage complete. Ready for dashboard.")


if __name__ == "__main__":
    main()
