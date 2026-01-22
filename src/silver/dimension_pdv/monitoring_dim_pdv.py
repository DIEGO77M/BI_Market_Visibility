"""
SilverFactSellInTransformer

Production-grade transformation layer for Sell-In fact table.
- Serverless compatible
- Unity Catalog compatible
- Idempotent (MERGE INTO)
- Transformation only (no validation, no monitoring)

Author: Senior Data Engineer (Portfolio)
"""

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
import logging

# ---------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverFactSellInTransformer")


class SilverFactSellInTransformer:
    def __init__(
        self,
        spark: SparkSession,
        source_table: str,
        target_table: str
    ):
        self.spark = spark
        self.source_table = source_table
        self.target_table = target_table

    # -----------------------------------------------------------------
    # Read Bronze
    # -----------------------------------------------------------------
    def _read_bronze(self) -> DataFrame:
        logger.info(f"Reading bronze source table: {self.source_table}")
        return self.spark.table(self.source_table)

    # -----------------------------------------------------------------
    # Transform
    # -----------------------------------------------------------------
    def _transform(self, df: DataFrame) -> DataFrame:
        logger.info("Applying Silver fact transformations")

        df = (
            df.select(
                F.col("PDV_Code").alias("pdv_code"),
                F.col("Product_Code").alias("product_code"),

                F.col("Year").cast(T.IntegerType()).alias("year"),
                F.col("Month").cast(T.IntegerType()).alias("month"),

                F.col("Opening_Stock_Units").cast(T.IntegerType()).alias("opening_stock_units"),
                F.col("Sell_In_Units").cast(T.IntegerType()).alias("sell_in_units"),
                F.col("Returns_Units").cast(T.IntegerType()).alias("returns_units"),
                F.col("Closing_Stock_Units").cast(T.IntegerType()).alias("closing_stock_units"),

                F.col("Days_of_Inventory").cast(T.DecimalType(10, 2)).alias("days_of_inventory"),
                F.col("Inventory_Turnover").cast(T.DecimalType(10, 2)).alias("inventory_turnover"),

                F.col("Replenishment_Flag").alias("replenishment_flag"),
                F.col("Stock_Risk_Level").alias("stock_risk_level"),

                F.col("_ingestion_timestamp").alias("ingestion_timestamp"),
                F.col("_load_date").alias("load_date"),
                F.col("_source_file").alias("source_file"),
                F.col("_batch_id").alias("bronze_batch_id")
            )
            .withColumn(
                "year_month",
                F.concat_ws("-", F.col("year"), F.lpad(F.col("month"), 2, "0"))
            )
            .withColumn(
                "silver_processed_at",
                F.current_timestamp()
            )
        )

        return df

    # -----------------------------------------------------------------
    # Upsert (Idempotent)
    # -----------------------------------------------------------------
    def _merge(self, df: DataFrame):
        logger.info(f"Upserting into Silver fact table: {self.target_table}")

        df.createOrReplaceTempView("source_fact_sell_in")

        merge_sql = f"""
        MERGE INTO {self.target_table} AS target
        USING source_fact_sell_in AS source
        ON
            target.pdv_code = source.pdv_code
            AND target.product_code = source.product_code
            AND target.year_month = source.year_month
        WHEN MATCHED THEN UPDATE SET
            opening_stock_units = source.opening_stock_units,
            sell_in_units = source.sell_in_units,
            returns_units = source.returns_units,
            closing_stock_units = source.closing_stock_units,
            days_of_inventory = source.days_of_inventory,
            inventory_turnover = source.inventory_turnover,
            replenishment_flag = source.replenishment_flag,
            stock_risk_level = source.stock_risk_level,
            ingestion_timestamp = source.ingestion_timestamp,
            load_date = source.load_date,
            source_file = source.source_file,
            bronze_batch_id = source.bronze_batch_id,
            silver_processed_at = source.silver_processed_at
        WHEN NOT MATCHED THEN INSERT *
        """

        self.spark.sql(merge_sql)

    # -----------------------------------------------------------------
    # Run
    # -----------------------------------------------------------------
    def run(self) -> None:
        logger.info("Starting SilverFactSellIn transformation")

        try:
            df_bronze = self._read_bronze()
            df_silver = self._transform(df_bronze)
            self._merge(df_silver)

            logger.info("SilverFactSellIn transformation completed successfully")

        except Exception as e:
            logger.error(f"Critical error in SilverFactSellInTransformer: {e}", exc_info=True)
            raise


# ---------------------------------------------------------------------
# Orchestration entrypoint (same pattern as MonitoringDimPDV)
# ---------------------------------------------------------------------
def run_silver_fact_sell_in(
    spark: SparkSession = None,
    source_table: str = "workspace.bronze.sell_in",
    target_table: str = "workspace.silver.fact_sell_in"
):
    """
    Orchestration-friendly entrypoint.
    """

    local_spark = False
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
        local_spark = True

    transformer = SilverFactSellInTransformer(
        spark=spark,
        source_table=source_table,
        target_table=target_table
    )

    transformer.run()

    if local_spark:
        logger.info("SilverFactSellIn finished in local mode.")


if __name__ == "__main__":
    logger.info("Running SilverFactSellIn in standalone mode")
    run_silver_fact_sell_in()
