from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
import logging
import os

# ================= CONFIG =================
GOLD_HOTSPOT_PATH = "/workspace/data_lake/gold/crash_hotspots/"
GOLD_RISK_PATH = "/workspace/data_lake/gold/road_risk_index/"

DW_BASE = "/workspace/data_warehouse/"

FACT_PATH = DW_BASE + "fact_accidents/"
DIM_LOCATION_PATH = DW_BASE + "dim_location/"
DIM_RISK_PATH = DW_BASE + "dim_risk/"

LOG_PATH = "/workspace/logs/dw.log"
os.makedirs("/workspace/logs", exist_ok=True)

# ================= LOGGING =================
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ================= SPARK =================
spark = SparkSession.builder \
    .appName("DW - Accident Warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    logging.info("DW Job Started")

    # ================= READ GOLD =================
    hotspot_df = spark.read.parquet(GOLD_HOTSPOT_PATH)
    risk_df = spark.read.parquet(GOLD_RISK_PATH)

    print("Hotspot rows:", hotspot_df.count())
    print("Risk rows:", risk_df.count())

    # ================= DIM LOCATION =================
    dim_location = hotspot_df.select(
        col("latitude"),
        col("longitude")
    ).dropDuplicates()

    dim_location = dim_location.withColumn(
        "location_id",
        monotonically_increasing_id()
    )

    # ================= DIM RISK =================
    dim_risk = risk_df.select(
        col("latitude"),
        col("longitude"),
        col("risk_score")
    )

    dim_risk = dim_risk.withColumn(
        "risk_id",
        monotonically_increasing_id()
    )

    # ================= FACT TABLE =================
    # ONLY REQUIRED COLUMNS (avoid ambiguity)
    risk_small = risk_df.select(
        "latitude",
        "longitude",
        "risk_score"
    )

    fact_df = hotspot_df.join(
        risk_small,
        ["latitude", "longitude"],
        "inner"
    ).select(
        col("latitude"),
        col("longitude"),
        col("year"),
        col("month"),
        col("accident_count"),
        col("risk_score")
    )

    fact_df = fact_df.withColumn(
        "fact_id",
        monotonically_increasing_id()
    )

    print("Fact rows:", fact_df.count())

    # ================= WRITE =================
    dim_location.write.mode("overwrite").parquet(DIM_LOCATION_PATH)
    dim_risk.write.mode("overwrite").parquet(DIM_RISK_PATH)
    fact_df.write.mode("overwrite").parquet(FACT_PATH)

    print(" Data Warehouse Created Successfully")
    logging.info("DW Completed Successfully")

except Exception as e:
    print(" Error:", e)
    logging.error(f"DW Failed: {e}")

finally:
    spark.stop()