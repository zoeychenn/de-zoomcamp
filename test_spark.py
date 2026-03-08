import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

print(f"Spark version: {spark.version}")

df = spark.range(10)
spark.version
df.show()

# spark.stop()