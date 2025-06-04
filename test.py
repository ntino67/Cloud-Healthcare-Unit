from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test PySpark") \
    .master("spark://spark_container:7077") \
    .config("spark.eventLog.enabled", "true") \
    .getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Cathy", 28)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

df_filtered = df.filter(df["Age"] > 26)

df_filtered.show()

spark.stop()