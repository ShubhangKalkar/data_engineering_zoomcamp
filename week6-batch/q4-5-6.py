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


result.show()

spark.stop()