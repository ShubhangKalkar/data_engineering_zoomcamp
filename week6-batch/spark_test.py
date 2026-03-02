from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("zoomcamp-week6") \
    .getOrCreate()

print("Spark Version:", spark.version)

spark.stop()