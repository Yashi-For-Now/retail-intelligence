# Data quality validation using Great Expectations

import logging
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import great_expectations as gx

#  Config
load_dotenv()

PROCESSED_DIR = Path("data/processed")
INPUT_FILE = PROCESSED_DIR / "retail_cleaned.parquet"

#  Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


#  Load cleaned data
def load_cleaned_data() -> pd.DataFrame:
    log.info(f"Loading cleaned data from {INPUT_FILE}")
    df = pd.read_parquet(INPUT_FILE)
    log.info(f"Loaded {len(df):,} rows for validation")
    return df


#  Run validations
def run_validations(df: pd.DataFrame) -> dict:
    log.info("Initialising Great Expectations context")

    context = gx.get_context()

    # Add pandas datasource
    datasource = context.data_sources.add_pandas(name="retail_datasource")
    asset = datasource.add_dataframe_asset(name="retail_cleaned")
    batch_def = asset.add_batch_definition_whole_dataframe("retail_batch")
    # batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    #  Define expectations
    suite = context.suites.add(gx.ExpectationSuite(name="retail_quality_suite"))

    # 1. Required columns exist
    for col in [
        "Invoice",
        "StockCode",
        "Description",
        "Quantity",
        "Price",
        "Customer ID",
        "Country",
        "Revenue",
        "InvoiceDate",
    ]:
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=col))

    # 2. No nulls in critical columns
    for col in ["Invoice", "StockCode", "Quantity", "Price", "Revenue"]:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=col))

    # 3. Quantity must be positive
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="Quantity", min_value=1, max_value=100_000
        )
    )

    # 4. Price must be positive
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="Price", min_value=0.01, max_value=50_000
        )
    )

    # 5. Revenue must be positive
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="Revenue", min_value=0.0)
    )

    # 6. Country unique count reasonable
    suite.add_expectation(
        gx.expectations.ExpectColumnUniqueValueCountToBeBetween(
            column="Country", min_value=1, max_value=200
        )
    )

    #  Run validation
    validation_def = context.validation_definitions.add(
        gx.ValidationDefinition(name="retail_validation", data=batch_def, suite=suite)
    )

    results = validation_def.run(batch_parameters={"dataframe": df})
    return results


#  Revenue integrity check
def check_revenue_integrity(df: pd.DataFrame) -> bool:
    df["_check"] = (df["Quantity"] * df["Price"]).round(2)
    mismatch = (abs(df["Revenue"] - df["_check"]) > 0.01).sum()
    df.drop(columns=["_check"], inplace=True)

    if mismatch > 0:
        log.warning(f"Revenue mismatch in {mismatch:,} rows")
        return False
    else:
        log.info("Revenue integrity check passed ✅")
        return True


#  Print summary
def print_summary(results) -> bool:
    log.info("=" * 50)
    log.info("VALIDATION RESULTS")
    log.info("=" * 50)

    all_passed = True

    for result in results.results:
        expectation = result.expectation_config.type
        column = (
            result.expectation_config.column
            if hasattr(result.expectation_config, "column")
            else "—"
        )
        success = result.success
        status = "✅ PASS" if success else "❌ FAIL"
        log.info(f"{status}  |  {expectation}  |  column: {column}")

        if not success:
            all_passed = False

    log.info("─" * 50)
    if all_passed:
        log.info("ALL EXPECTATIONS PASSED ✅")
    else:
        log.warning("SOME EXPECTATIONS FAILED")

    return all_passed


#  Main
def main():
    log.info("=" * 50)
    log.info("RETAIL INTELLIGENCE — VALIDATE STAGE")
    log.info("=" * 50)

    df = load_cleaned_data()

    # Pre-check summary
    log.info("─" * 50)
    log.info(f"Rows          : {len(df):,}")
    log.info(f"Null count    : {df.isnull().sum().sum():,}")
    log.info(f"Negative qty  : {(df['Quantity'] < 0).sum():,}")
    log.info(f"Negative price: {(df['Price'] < 0).sum():,}")
    log.info(f"Zero revenue  : {(df['Revenue'] <= 0).sum():,}")
    log.info("─" * 50)

    # Revenue integrity
    check_revenue_integrity(df)

    # GE validations
    results = run_validations(df)
    all_good = print_summary(results)

    if not all_good:
        log.error("Validation failed — fix issues before loading to DB")
        raise SystemExit(1)

    log.info("Validate stage complete.")


if __name__ == "__main__":
    main()
