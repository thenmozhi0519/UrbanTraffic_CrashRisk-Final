from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, count, current_timestamp,
    lit, coalesce, round
)
import logging
import os
import time

# ================= CONFIG =================
GOLD_PATH = "/workspace/data_lake/gold/youtube_chicago_monthly/"
CHECKPOINT_PATH = "/workspace/checkpoints/gold/"
LOG_FILE = "/workspace/logs/gold_layer.log"

os.makedirs("/workspace/logs", exist_ok=True)

# ================= LOGGING =================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ================= SPARK =================
spark = SparkSession.builder \
    .appName("Gold Layer - Monthly Insights") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir(CHECKPOINT_PATH)

try:
    logging.info("Gold Monthly Pipeline Started")

    # ================= READ SILVER =================
    youtube_df = spark.read.parquet("/workspace/data_lake/silver/youtube/")
    chicago_df = spark.read.parquet("/workspace/data_lake/silver/traffic_crashes/")

    # ================= DATA QUALITY =================
    youtube_df = youtube_df.filter(col("video_id").isNotNull())
    chicago_df = chicago_df.filter(col("crash_record_id").isNotNull())

    # ================= YOUTUBE MONTHLY =================
    youtube_monthly = youtube_df.groupBy(
        year("published_at").alias("year"),
        month("published_at").alias("month")
    ).agg(count("*").alias("youtube_video_count"))

    youtube_monthly = youtube_monthly.checkpoint()

    # ================= CHICAGO MONTHLY =================
    chicago_monthly = chicago_df.groupBy(
        year("crash_date").alias("year"),
        month("crash_date").alias("month")
    ).agg(count("*").alias("accident_count"))

    chicago_monthly = chicago_monthly.checkpoint()

    # ================= JOIN (LEFT JOIN for more data) =================
    gold_df = chicago_monthly.join(
        youtube_monthly,
        ["year", "month"],
        "left"
    )

    # ================= HANDLE NULL =================
    gold_df = gold_df.withColumn(
        "youtube_video_count",
        coalesce(col("youtube_video_count"), lit(0))
    )

    # ================= INSIGHTS COLUMN =================
    gold_df = gold_df.withColumn(
        "videos_per_100_accidents",
        round((col("youtube_video_count") / col("accident_count")) * 100, 2)
    )

    # ================= AUDIT =================
    gold_df = gold_df.withColumn("ingestion_time", current_timestamp()) \
                     .withColumn("source", lit("gold_monthly_pipeline")) \
                     .withColumn("batch_id", lit(f"batch_{int(time.time())}"))

    # ================= FINAL CHECK =================
    count_final = gold_df.count()
    print("Gold Monthly Count:", count_final)
    logging.info(f"Gold Monthly Count: {count_final}")

    gold_df.orderBy(col("year"), col("month")).show(20, False)

    # ================= WRITE =================
    gold_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(GOLD_PATH)

    print(" Gold Monthly Layer Created Successfully")
    logging.info("Gold Monthly Completed")

except Exception as e:
    print(" Error:", e)
    logging.error(f"Gold Monthly Failed: {e}")

finally:
    spark.stop()