from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InspectUNSW").master("local[2]").getOrCreate()
df = spark.read.parquet("hdfs://namenode:9000/bda/clean/unsw/")
print("UNSW Columns Count:", len(df.columns))
print("UNSW Columns:", df.columns)
df.select("dur", "proto", "service", "state", "spkts", "dpkts", "sbytes", "dbytes", "attack_cat", "label").show(5)
spark.stop()
