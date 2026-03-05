from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("zoomcamp-q2") \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# repartition
df_repart = df.repartition(4)

# write parquet
df_repart.write.mode("overwrite").parquet("yellow_output")

spark.stop()