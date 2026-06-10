from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round

spark = SparkSession.builder.appName("Road Risk Index").getOrCreate()

# ================= READ SILVER =================
df = spark.read.parquet("/workspace/data_lake/silver/traffic_crashes/")

# ================= BASE FILTER =================
df = df.filter(
    col("latitude").isNotNull() &
    col("longitude").isNotNull()
)

# ================= AGGREGATION =================
risk_df = df.groupBy(
    "latitude",
    "longitude"
).agg(
    count("*").alias("accident_count"),

    # Bad weather count
    count(
        when(
            col("weather_condition").isin(
                "RAIN", "SNOW", "FOG", "SLEET"
            ), True
        )
    ).alias("bad_weather_count"),

    # Night time count
    count(
        when(
            col("lighting_condition").isin(
                "DARKNESS", "DUSK", "DAWN"
            ), True
        )
    ).alias("night_count")
)

# ================= RISK SCORE =================
risk_df = risk_df.withColumn(
    "risk_score",
    round(
        (col("accident_count") * 0.5) +
        (col("bad_weather_count") * 0.3) +
        (col("night_count") * 0.2),
        2
    )
)

print("Risk Rows:", risk_df.count())
risk_df.show(20, False)

# ================= SAVE =================
risk_df.write \
    .mode("overwrite") \
    .parquet("/workspace/data_lake/gold/road_risk_index/")

print("Road Risk Index Created ✅")

spark.stop()