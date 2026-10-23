import time
import math
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, ArrayType, LongType

spark = (
    SparkSession.builder
    .appName("TestPayloadBench")
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "1024m")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/bda/clean/packets/")
print("Total rows:", df.count())

# Test with 64 byte columns first
byte_cols_64 = [f"payload_byte_{i}" for i in range(1, 65)]
sample_df = df.select("flow_id", "payload_length", *byte_cols_64).limit(500)

print("Testing array creation on 64 columns...")
t0 = time.time()
arr_df = sample_df.withColumn("bytes_arr", F.array(*byte_cols_64))
row = arr_df.first()
print(f"Array created in {time.time() - t0:.2f}s, sample length: {len(row['bytes_arr'])}")

spark.stop()
