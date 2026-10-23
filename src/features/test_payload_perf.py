import time
import math
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, ArrayType

spark = (
    SparkSession.builder
    .appName("TestPayloadFull")
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "1024m")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/bda/clean/packets/")
byte_cols = [f"payload_byte_{i}" for i in range(1, 65)]

print("Reading packets and projecting columns...")
# Select flow_id and 64 byte columns
t0 = time.time()
proj_df = df.select("flow_id", *byte_cols)

# Define UDF / RDD processing
def calc_entropy_hist(row):
    flow_id = row[0]
    bytes_list = [b for b in row[1:] if b is not None and 0 <= b <= 255]
    return (flow_id, bytes_list)

def merge_bytes(a, b):
    # a and b are lists of byte counts (length 256)
    return [x + y for x, y in zip(a, b)]

def row_to_counts(row):
    flow_id = row[0]
    counts = [0] * 256
    for b in row[1:]:
        if b is not None and 0 <= b <= 255:
            counts[b] += 1
    return (flow_id, counts)

print("Running reduceByKey on byte counts...")
rdd_counts = proj_df.rdd.map(row_to_counts).reduceByKey(merge_bytes)

def counts_to_stats(item):
    flow_id, counts = item
    total = sum(counts)
    if total == 0:
        entropy = 0.0
        bins = [0.0] * 16
    else:
        entropy = 0.0
        for c in counts:
            if c > 0:
                p = c / total
                entropy -= p * math.log2(p)
        bins = [0.0] * 16
        for b in range(16):
            bins[b] = sum(counts[b * 16 : (b + 1) * 16]) / total
    return (flow_id, float(entropy), *[float(x) for x in bins])

stats_rdd = rdd_counts.map(counts_to_stats)

schema = StructType(
    [StructField("flow_id", LongType(), False), StructField("payload_entropy", DoubleType(), False)]
    + [StructField(f"payload_hist_bin_{i}", DoubleType(), False) for i in range(16)]
)

result_df = spark.createDataFrame(stats_rdd, schema)
print("Result count:", result_df.count())
result_df.show(5)
print(f"Total time: {time.time() - t0:.2f}s")
spark.stop()
