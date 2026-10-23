from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.appName("InspectFlowCols").getOrCreate()
df = spark.read.parquet("hdfs://namenode:9000/bda/clean/flows/")
print("Flow Columns and Data Types:")
dtypes = df.dtypes
for c, t in dtypes:
    print(f"'{c}': '{t}',")

with open("/tmp/flow_dtypes.json", "w") as f:
    json.dump(dtypes, f, indent=2)

spark.stop()
