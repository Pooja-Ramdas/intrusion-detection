from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('sample-ts').getOrCreate()
df = spark.read.parquet('hdfs://namenode:9000/bda/raw/cicids2017_flow/').select('Timestamp', 'attack_label')
df.show(15, truncate=False)
spark.stop()
