from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("TestPayload").getOrCreate()
df = spark.read.parquet("hdfs://namenode:9000/bda/clean/packets/")
print("df schema payload_length dtype:", dict(df.dtypes).get("payload_length"))
df.select("flow_id", "packet_id", "payload_length", "payload_byte_1", "payload_byte_2", "payload_byte_3", "payload_byte_4", "payload_byte_5").show(10)
df.select(
    F.min("payload_length"),
    F.max("payload_length"),
    F.avg("payload_length"),
    F.count(F.when(F.col("payload_length") > 0, 1))
).show()
spark.stop()
