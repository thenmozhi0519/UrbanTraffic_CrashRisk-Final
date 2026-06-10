from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month,
    current_timestamp, lit
)

spark = SparkSession.builder \
    .appName("YouTube Silver") \
    .getOrCreate()

try:
    # ================= READ BRONZE =================
    df = spark.read \
        .option("multiline", "true") \
        .json("/workspace/data_lake/bronze/youtube_sentiment/")

    print("Raw Count:", df.count())

    # ================= SELECT COLUMNS =================
    df_clean = df.select(
        col("id.videoId").alias("video_id"),
        col("snippet.title").alias("title"),
        col("snippet.channelTitle").alias("channel"),
        col("snippet.publishedAt").alias("published_at")
    )

    # ================= CLEANING =================
    df_clean = df_clean.dropDuplicates(["video_id"])
    df_clean = df_clean.filter(col("video_id").isNotNull())

    # ================= FIX TIMESTAMP =================
    df_clean = df_clean.withColumn(
        "published_at",
        to_timestamp(col("published_at"))
    )

    # ================= ADD PARTITIONS =================
    df_clean = df_clean.withColumn("year", year(col("published_at"))) \
                       .withColumn("month", month(col("published_at")))

    # ================= METADATA =================
    df_clean = df_clean.withColumn("ingestion_time", current_timestamp()) \
                       .withColumn("source", lit("youtube_api_silver")) \
                       .withColumn("batch_id", lit("batch_001"))

    print("Clean Count:", df_clean.count())

    # ================= WRITE =================
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet("/workspace/data_lake/silver/youtube/")

    print("YouTube Silver Done ✅")

except Exception as e:
    print("Error:", e)

finally:
    spark.stop()