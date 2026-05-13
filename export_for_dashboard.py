# Exports aggregated data from PostgreSQL to CSVs for dashboard consumption

import logging
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv
import os

#  Config
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "retail_db")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")

if not DB_USER or not DB_PASS:
    raise ValueError("DB_USER and DB_PASSWORD must be set in .env file")

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
OUTPUT_DIR = Path("data/processed")

#  Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# Export: Monthly Revenue Trend
def export_monthly(engine) -> None:
    log.info("Exporting monthly revenue trend...")
    query = """
        SELECT
            dd.year,
            dd.month,
            ROUND(SUM(fs.revenue)::numeric, 2)      AS monthly_revenue,
            COUNT(DISTINCT fs.invoice)               AS total_orders,
            COUNT(DISTINCT fs.customer_id)           AS unique_customers
        FROM fact_sales fs
        JOIN dim_date dd ON fs.date_id = dd.date_id
        GROUP BY dd.year, dd.month
        ORDER BY dd.year, dd.month
    """
    df = pd.read_sql(query, engine)
    # Create a proper date column for Tableau
    df["month_year"] = pd.to_datetime(
        df["year"].astype(str) + "-" + df["month"].astype(str) + "-01"
    )
    out = OUTPUT_DIR / "dash_monthly.csv"
    df.to_csv(out, index=False)
    log.info(f"Monthly trend exported: {len(df)} rows → {out}")


#  Export: Top 20 Products
def export_top_products(engine) -> None:
    log.info("Exporting top 20 products by revenue...")
    query = """
        SELECT
            dp.stock_code,
            dp.description,
            ROUND(SUM(fs.revenue)::numeric, 2)  AS total_revenue,
            SUM(fs.quantity)                     AS total_quantity,
            COUNT(DISTINCT fs.invoice)           AS total_orders
        FROM fact_sales fs
        JOIN dim_product dp ON fs.stock_code = dp.stock_code
        GROUP BY dp.stock_code, dp.description
        ORDER BY total_revenue DESC
        LIMIT 20
    """
    df = pd.read_sql(query, engine)
    out = OUTPUT_DIR / "dash_top_products.csv"
    df.to_csv(out, index=False)
    log.info(f"Top products exported: {len(df)} rows → {out}")


#  Export: Revenue by Country
def export_by_country(engine) -> None:
    log.info("Exporting revenue by country...")
    query = """
        SELECT
            dr.country,
            dr.region_group,
            ROUND(SUM(fs.revenue)::numeric, 2)      AS total_revenue,
            COUNT(DISTINCT fs.customer_id)           AS unique_customers,
            COUNT(DISTINCT fs.invoice)               AS total_orders
        FROM fact_sales fs
        JOIN dim_region dr ON fs.region_id = dr.region_id
        GROUP BY dr.country, dr.region_group
        ORDER BY total_revenue DESC
    """
    df = pd.read_sql(query, engine)
    out = OUTPUT_DIR / "dash_by_country.csv"
    df.to_csv(out, index=False)
    log.info(f"By country exported: {len(df)} rows → {out}")


#  Main
def main():
    log.info("=" * 50)
    log.info("RETAIL INTELLIGENCE — EXPORT DASHBOARD DATA")
    log.info("=" * 50)

    engine = create_engine(DB_URL)
    log.info("Database connected")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    export_monthly(engine)
    export_top_products(engine)
    export_by_country(engine)

    log.info("─" * 50)
    log.info("All CSVs exported. Ready for Tableau.")
    log.info(f"Location: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
