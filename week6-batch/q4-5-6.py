from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, unix_timestamp

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("zoomcamp-week6") \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# --------------------------------
# Q3 — Trips on Nov 15
# --------------------------------

trips_nov15 = df.filter(
    to_date(col("tpep_pickup_datetime")) == "2025-11-15"
).count()

print("Trips on Nov 15:", trips_nov15)


# --------------------------------
# Q4 — Longest trip
# --------------------------------

df_trip = df.withColumn(
    "trip_hours",
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600
)

longest_trip = df_trip.selectExpr("max(trip_hours)").collect()[0][0]

print("Longest trip hours:", longest_trip)


result.show()

spark.stop()