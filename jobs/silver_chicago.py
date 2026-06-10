from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month,
    current_timestamp, lit
)
import logging

# ================= LOGGING =================
logging.basicConfig(
    filename="silver_chicago.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

spark = SparkSession.builder.appName("Silver - Chicago").getOrCreate()

try:
    # ================= READ BRONZE =================
    df = spark.read.option("multiLine", "true").json("/workspace/data_lake/bronze/crashes_partitioned/*/*.json")

    print("Raw Count:", df.count())

    # ================= SELECT REQUIRED COLUMNS =================
    df_clean = df.select(
    col("crash_record_id"),
    col("crash_date"),
    col("weather_condition"),
    col("lighting_condition"),
    col("first_crash_type"),
    col("roadway_surface_cond"),
    col("latitude"),    
    col("longitude"),
    col("injuries_total"),
    col("injuries_fatal")
)
    

    # ================= DATA CLEANING =================
    df_clean = df_clean.dropna(subset=["crash_record_id", "crash_date"])

    # Remove duplicates
    df_clean = df_clean.dropDuplicates(["crash_record_id"])

    # Fix timestamp
    df_clean = df_clean.withColumn(
        "crash_date",
        to_timestamp(col("crash_date"))
    )

    # ================= FILTER VALID COMMUNITY =================
    df_clean = df_clean.filter(col("latitude").isNotNull() & col("longitude").isNotNull())
    # ================= PARTITION COLUMNS =================
    df_clean = df_clean.withColumn("year", year(col("crash_date"))) \
                       .withColumn("month", month(col("crash_date")))

    # ================= METADATA =================
    df_clean = df_clean.withColumn("ingestion_time", current_timestamp()) \
                       .withColumn("source", lit("chicago_api_silver")) \
                       .withColumn("batch_id", lit("batch_001"))

    print("Clean Count:", df_clean.count())

    # ================= WRITE =================
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet("/workspace/data_lake/silver/traffic_crashes/")
    # Creates: silver/traffic_crashes/year=2021/month=1/part-00000.parquet

    print("Chicago Silver Done ✅")

except Exception as e:
    print("Error:", e)
    logging.error(f"Error: {e}")

finally:
    spark.stop()