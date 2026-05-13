# Cleans, enriches and aggregates the retail dataset

import os
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from dotenv import load_dotenv

# Config
load_dotenv()

PROCESSED_DIR = Path("data/processed")
INPUT_FILE = PROCESSED_DIR / "retail_raw.parquet"
OUTPUT_FILE = PROCESSED_DIR / "retail_cleaned.parquet"

# Logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# Spark Session
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("RetailIntelligence-Transform")
        .master("local[*]")
        .config("sparks.driver.memory", "2g")
        .config("sparks.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# 1. Load
def load_data(spark: SparkSession):
    log.info(f"Loading: {INPUT_FILE}")
    df = spark.read.parquet(str(INPUT_FILE))
    log.info(f"Loaded {df.count():,} rows, {len(df.columns)} columns")
    return df


# 2. Drop duplicates
def remove_duplicates(df):
    before = df.count()
    df = df.dropDuplicates()
    after = df.count()
    log.info(f"Duplicates removed: {before-after:,} rows dropped")
    return df


# 3. Handle nulls
def handle_nulls(df):
    df = df.dropna(subset=["Invoice", "StockCode", "InvoiceDate", "Price", "Quantity"])
    df = df.fillna({"Description": "Unknown", "Country": "Unknown"})
    df = df.fillna({"Customer ID": 0})

    log.info("Null handling complete")
    return df


# 4. Fix data types
def fix_types(df):
    df = (
        df.withColumn("Quantity", F.col("Quantity").cast("integer"))
        .withColumn("Price", F.col("Price").cast("double"))
        .withColumn("Customer ID", F.col("Customer ID").cast("integer"))
        .withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "yyyy-MM-dd H:mm:ss"))
    )
    log.info("Data types fixed")
    return df


# 5. Filter bad data
def filter_bad_data(df):
    before = df.count()

    df = (
        df.filter(~F.col("Invoice").startswith("C"))  # cancellations
        .filter(F.col("Quantity") > 0)  # negative or zero quantities
        .filter(F.col("Price") >= 0.01)  # negative or zero prices
    )

    after = df.count()
    log.info(f"Bad data filtered: {before-after:,} rows removed.")
    return df


# 6. Add derived columns
def add_derived_cols(df):
    df = (
        df.withColumn("Revenue", F.round(F.col("Quantity") * F.col("Price"), 2))
        .withColumn("Year", F.year("InvoiceDate"))
        .withColumn("Month", F.month("InvoiceDate"))
        .withColumn("Day", F.day("InvoiceDate"))
        .withColumn("Hour", F.hour("InvoiceDate"))
        .withColumn("Weekday", F.dayofweek("InvoiceDate"))
        .withColumn("Description", F.trim(F.upper(F.col("Description"))))
    )
    log.info("Derived columns added: Revenue, Year, Month, Hour, Weekday")
    return df


# 7. Rank products by revenue
def rank_prods(df):
    """
    For each country, we rank the products by total revenue
    Advanced Pyspark transforms
    """
    prod_revenue = df.groupBy("StockCode", "Description", "Country").agg(
        F.round(F.sum("Revenue"), 2).alias("TotalRevenue")
    )

    window = Window.partitionBy("Country").orderBy(F.desc("TotalRevenue"))

    prod_ranked = prod_revenue.withColumn("RevenueRank", F.rank().over(window))

    log.info("Product ranked by revenue")
    return prod_ranked


# 8. Aggregations
def compute_aggregations(df):
    """
    Three aggregations saved separately- used by dashboard later
    Three are:  1. Monthly revenue trend
                2. Top products by revenue
                3. Revenue by country
    """

    # Monthly revenue trend
    monthly = (
        df.groupBy("Year", "Month")
        .agg(
            F.round(F.sum("Revenue"), 2).alias("MonthlyRevenue"),
            F.count_distinct("Invoice").alias("TotalOrders"),
            F.count_distinct("Customer ID").alias("UniqueCustomers"),
        )
        .orderBy("Year", "Month")
    )

    # Top products by revenue
    top_prods = (
        df.groupBy("StockCode", "Description")
        .agg(
            F.round(F.sum("Revenue"), 2).alias("TotalRevenue"),
            F.sum("Quantity").alias("TotalQuantity"),
        )
        .orderBy(F.desc("TotalRevenue"))
        .limit(20)
    )

    # Revenue by country
    by_country = (
        df.groupBy("Country")
        .agg(
            F.round(F.sum("Revenue"), 2).alias("TotalRevenue"),
            F.countDistinct("Customer ID").alias("UniqueCustomers"),
            F.countDistinct("Invoice").alias("TotalOrders"),
        )
        .orderBy(F.desc("TotalRevenue"))
    )

    return monthly, top_prods, by_country


# 9. Save outputs
def save_outputs(df_clean, monthly, top_prods, by_country):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    df_clean.write.mode("overwrite").parquet(str(OUTPUT_FILE))
    log.info(f"Cleaned data saved → {OUTPUT_FILE}")

    monthly.write.mode("overwrite").parquet(str(PROCESSED_DIR / "agg_monthly.parquet"))
    log.info("Monthly aggregation saved → agg_monthly.parquet")

    top_prods.write.mode("overwrite").parquet(
        str(PROCESSED_DIR / "agg_top_products.parquet")
    )
    log.info("Top products saved → agg_top_products.parquet")

    by_country.write.mode("overwrite").parquet(
        str(PROCESSED_DIR / "agg_by_country.parquet")
    )
    log.info("Country aggregation saved → agg_by_country.parquet")


#  Main
def main():
    log.info("=" * 50)
    log.info("RETAIL INTELLIGENCE — TRANSFORM STAGE")
    log.info("=" * 50)

    spark = create_spark_session()
    log.info(f"Spark version: {spark.version}")

    # Pipeline
    df = load_data(spark)
    df = remove_duplicates(df)
    df = handle_nulls(df)
    df = fix_types(df)
    df = filter_bad_data(df)
    df = add_derived_cols(df)

    # Window function
    prod_ranked = rank_prods(df)

    # Aggregations
    monthly, top_prods, by_country = compute_aggregations(df)

    # Final row count
    log.info(f"Final clean dataset: {df.count():,} rows")

    # Save everything
    save_outputs(df, monthly, top_prods, by_country)

    spark.stop()
    log.info("Transform stage complete.")


if __name__ == "__main__":
    main()
