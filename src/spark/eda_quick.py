"""Deadline-bounded EDA: deterministic 10k-row prefixes, projected columns only."""
import json
import time
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, FloatType, NumericType
from common import HDFS, RAW, day_column, find_column, label_and_day, normalize_columns, spark_session, write_hdfs_json, write_small_table

LIMIT = 10_000


def sample(path, dataset, columns):
    df = normalize_columns(spark.read.parquet(path))
    kept = [c for c in columns if c in df.columns]
    df = df.select(*kept).limit(LIMIT)
    return label_and_day(df, dataset)


def main():
    global spark
    start = time.time()
    spark = spark_session("BDA-Phase2-EDA-Quick")
    spark.sparkContext.setLogLevel("WARN")
    flow_path = f"{RAW}/cicids2017_flow/CICIDS_Flow.parquet"
    packet_path = f"{RAW}/cicids2017_packet/cicids2017_packet_slice.parquet"
    unsw_path = f"{RAW}/unswnb15"
    flow = sample(flow_path, "cicids2017_flow", ["flow_id", "timestamp", "protocol", "flow_duration", "flow_bytes_s", "flow_packets_s", "attack_label"])
    packet = sample(packet_path, "cicids2017_packet", ["flow_id", "packet_id", "protocol", "payload_length", "attack_label"])
    unsw = sample(unsw_path, "unswnb15", ["attack_cat", "proto", "label"])
    packet_key = find_column(packet, ["flow_id"])
    flow_key = find_column(flow, ["flow_id"])
    if packet_key and flow_key:
        days = flow.select(F.col(flow_key).cast("string").alias("_key"), F.col("day").alias("_day")).dropDuplicates(["_key"])
        packet = packet.drop("day").join(F.broadcast(days), F.col(packet_key).cast("string") == F.col("_key"), "left")
        packet = packet.withColumn("day", F.coalesce(F.col("_day"), F.lit("UNKNOWN"))).drop("_key", "_day")
    frames = [flow, packet, unsw]
    counts = None
    map_rows = []
    quality_rows = []
    row_counts = {}
    summaries = []
    for dataset, df in zip(("cicids2017_flow", "cicids2017_packet", "unswnb15"), frames):
        start_one = time.time()
        fields = [f for f in df.schema.fields if f.name not in ("label_multi", "label_binary", "label_raw", "day")]
        exprs = [F.count(F.lit(1)).alias("_rows")]
        aliases = {}
        for i, field in enumerate(fields):
            col = F.col(f"`{field.name}`")
            aliases[field.name] = (f"n{i}", f"a{i}", f"b{i}", f"c{i}")
            exprs.extend([F.sum(F.when(col.isNull(), 1).otherwise(0)).alias(f"n{i}"), F.min(col).alias(f"a{i}"), F.max(col).alias(f"b{i}")])
            if isinstance(field.dataType, (FloatType, DoubleType)):
                x=col.cast("double")
                exprs.extend([F.sum(F.when(F.isnan(x),1).otherwise(0)).alias(f"c{i}_nan"),F.sum(F.when(F.abs(x)==float("inf"),1).otherwise(0)).alias(f"c{i}_inf")])
        aggregate=df.agg(*exprs).first().asDict()
        n=int(aggregate["_rows"] or 0)
        row_counts[dataset]=n
        counts_one = df.groupBy("day", "label_multi", "label_binary").count().withColumnRenamed("count", "rows").withColumn("dataset", F.lit(dataset))
        counts = counts_one if counts is None else counts.unionByName(counts_one)
        for row in df.select("label_raw", "label_multi", "label_binary").distinct().collect():
            map_rows.append({"dataset": dataset, "raw_label": row[0], "canonical_class": row[1], "binary_class": row[2]})
        for field in fields:
            c=field.name
            na,mi,ma,_=aliases[c]
            nulls=int(aggregate[na] or 0)
            minimum,maximum=aggregate[mi],aggregate[ma]
            nans=int(aggregate.get(f"{aliases[c][0]}_nan",0) or 0)
            infs=int(aggregate.get(f"{aliases[c][0]}_inf",0) or 0)
            quality_rows.append((dataset,c,field.dataType.simpleString(),n,nulls,nulls/n if n else 0.0,nans,nans/n if n else 0.0,infs,infs/n if n else 0.0,None if minimum is None else str(minimum),None if maximum is None else str(maximum),bool(minimum==maximum)))
        summaries.append((dataset, n, len(df.columns), time.time()-start_one))
        print(f"EDA_SAMPLE dataset={dataset} rows={n} columns={len(df.columns)}", flush=True)
    imbalances = (counts.groupBy("dataset", "label_multi", "label_binary").agg(F.sum("rows").alias("rows"))
                 .withColumn("max_to_class_ratio", F.max("rows").over(Window.partitionBy("dataset"))))
    imbalances = imbalances.withColumn("max_to_class_ratio", F.col("max_to_class_ratio") / F.col("rows"))
    protocol_col = find_column(packet, ["protocol", "proto"])
    if protocol_col:
        protocols = packet.groupBy(F.lower(F.col(protocol_col).cast("string")).alias("protocol")).count().withColumnRenamed("count", "rows").withColumn("dataset", F.lit("cicids2017_packet"))
        write_small_table(spark, protocols, "protocol_distribution")
    size_col = find_column(packet, ["payload_length", "packet_size", "length"])
    if size_col:
        x=F.col(size_col).cast("double")
        b=F.when(x.isNull(),"missing").when(x<=0,"0").when(x<=64,"1-64").when(x<=128,"65-128").when(x<=256,"129-256").when(x<=512,"257-512").when(x<=1024,"513-1024").otherwise("1025+")
        sizes=packet.select(b.alias("packet_size_bin")).groupBy("packet_size_bin").count().withColumnRenamed("count","rows")
        write_small_table(spark, sizes, "packet_size_distribution")
    dedup = packet.dropDuplicates(packet.columns)
    valid_condition = F.col(size_col).cast("double").between(1,65535) if size_col else F.lit(True)
    clean_n_expr = F.sum(F.when(valid_condition,1).otherwise(0)).alias("valid_rows")
    after_stats=dedup.agg(F.count(F.lit(1)).alias("dedup_rows"),clean_n_expr).first()
    dedup_n=int(after_stats["dedup_rows"] or 0)
    clean_n=int(after_stats["valid_rows"] or 0)
    packet_n=row_counts["cicids2017_packet"]
    before_after = spark.createDataFrame([
        ("cicids2017_packet", "sample_before", packet_n),
        ("cicids2017_packet", "after_exact_dedupe", dedup_n),
        ("cicids2017_packet", "after_valid_size_rule", clean_n),
    ], "dataset string, stage string, rows long")
    write_small_table(spark, counts, "rows_by_dataset_day_label")
    write_small_table(spark, imbalances, "class_imbalance_by_class")
    quality=spark.createDataFrame(quality_rows,"dataset string,column_name string,data_type string,row_count long,null_count long,null_rate double,nan_count long,nan_rate double,infinity_count long,infinity_rate double,min_value string,max_value string,is_constant boolean")
    write_small_table(spark, quality, "column_quality")
    write_small_table(spark, before_after, "eda_before_after")
    summary=spark.createDataFrame(summaries,"dataset string,rows_before long,columns long,profile_seconds double")
    write_small_table(spark, summary, "profile_summary")
    label_map={"mapping_rule_version":"canonical_labels_v1","sample_row_limit":LIMIT,"mappings":map_rows}
    write_hdfs_json(spark,f"{HDFS}/bda/_meta/label_map.json",label_map)
    open("/logs/profile/phase2_v1/label_map.json","w",encoding="utf-8").write(json.dumps(label_map,ensure_ascii=False,indent=2))
    print(f"EDA_COMPLETE packet_before={packet_n} exact_deduped={dedup_n} valid_size={clean_n} seconds={time.time()-start:.2f}",flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
