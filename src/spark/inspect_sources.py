from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("BDA-Phase2-SourceSchemaProbe").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
paths = {
    "cic_flow": "hdfs://namenode:9000/bda/raw/cicids2017_flow/CICIDS_Flow.parquet",
    "cic_packet": "hdfs://namenode:9000/bda/raw/cicids2017_packet/Packet_Bytes_File_13.parquet",
    "unsw_train": "hdfs://namenode:9000/bda/raw/unswnb15/train.parquet",
}
for name, path in paths.items():
    df = spark.read.parquet(path)
    print(f"SOURCE={name} cols={len(df.columns)}")
    print("IDENTIFIERS=" + repr([(f.name, f.dataType.simpleString()) for f in df.schema.fields
                                  if any(k in f.name.lower() for k in
                                         ("label", "attack", "flow_id", "packet_id", "timestamp", "time",
                                          "source", "destination", "port", "protocol", "bytes/s", "packets/s"))]))
    if name == "cic_packet":
        print("PAYLOAD_SAMPLE=" + repr([(f.name, f.dataType.simpleString()) for f in df.schema.fields[:14]]))
spark.stop()
