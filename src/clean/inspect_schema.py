from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('inspect-raw').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df_flow = spark.read.parquet('hdfs://namenode:9000/bda/raw/cicids2017_flow/')
print('=== FLOW COLUMNS ===')
print(df_flow.columns)
print('TIMESTAMP/DAY COLS IN FLOW:', [c for c in df_flow.columns if any(k in c.lower() for k in ['time', 'date', 'day', 'timestamp'])])
print('ID/IP/PORT COLS IN FLOW:', [c for c in df_flow.columns if any(k in c.lower() for k in ['id', 'ip', 'port', 'proto'])])

df_packet = spark.read.parquet('hdfs://namenode:9000/bda/raw/cicids2017_packet/')
print('=== PACKET COLUMNS (first 25) ===')
print(df_packet.columns[:25])
print('PACKET METADATA COLS:', [c for c in df_packet.columns if not c.startswith('payload_byte_')])

df_unsw = spark.read.parquet('hdfs://namenode:9000/bda/raw/unswnb15/')
print('=== UNSW COLUMNS ===')
print(df_unsw.columns)

spark.stop()
