from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("zoomcamp-q3") \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

count = df.filter(
    to_date(col("tpep_pickup_datetime")) == "2025-11-15"
).count()

print("Trips on Nov 15:", count)

spark.stop()