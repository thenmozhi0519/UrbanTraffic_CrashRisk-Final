from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("Crash Hotspots").getOrCreate()

# Read Silver
df = spark.read.parquet("/workspace/data_lake/silver/traffic_crashes/")

# Filter valid coords
df = df.filter(
    col("latitude").isNotNull() &
    col("longitude").isNotNull()
)

# Grouping → hotspot logic
hotspots = df.groupBy(
    "latitude",
    "longitude",
    "year",
    "month"
).agg(
    count("*").alias("accident_count")
)

print("Hotspot Count:", hotspots.count())

# Save
hotspots.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("/workspace/data_lake/gold/crash_hotspots/")

print("Crash Hotspots Created ✅")

spark.stop()