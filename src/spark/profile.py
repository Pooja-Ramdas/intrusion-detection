import json
import math
import time

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, FloatType, NumericType

from common import (
    HDFS, LOG_ROOT, META_HDFS, RAW, canonical_label, day_column,
    find_column, label_and_day, normalize_columns, path_exists, safe_identifier,
    spark_session, write_hdfs_json, write_small_table,
)

SAMPLE_LIMIT = 50_000


def table_for_labels(df, dataset):
    return (
        df.groupBy("dataset", "day", "label_multi", "label_binary")
        .count().withColumnRenamed("count", "rows")
    )


def column_quality(df, dataset, total_rows):
    output = []
    fields = list(df.schema.fields)
    # Wide packet tables have 1.6k columns; 256-column passes bound query width
    # without rescanning once per 64-column slice.
    for offset in range(0, len(fields), 256):
        batch = fields[offset:offset + 256]
        exprs = []
        names = []
        for field in batch:
            col = F.col(f"`{field.name.replace('`', '``')}`")
            safe = safe_identifier(field.name)
            null_alias = f"{safe}__null"
            exprs.append(F.sum(F.when(col.isNull(), 1).otherwise(0)).alias(null_alias))
            names.append((field.name, field.dataType, null_alias, None, None, None, None))
            if isinstance(field.dataType, NumericType):
                min_alias, max_alias = f"{safe}__min", f"{safe}__max"
                exprs.extend([F.min(col).alias(min_alias), F.max(col).alias(max_alias)])
                nan_alias = inf_alias = None
                if isinstance(field.dataType, (FloatType, DoubleType)):
                    nan_alias, inf_alias = f"{safe}__nan", f"{safe}__inf"
                    numeric = col.cast("double")
                    exprs.extend([
                        F.sum(F.when(F.isnan(numeric), 1).otherwise(0)).alias(nan_alias),
                        F.sum(F.when((F.abs(numeric) == float("inf")), 1).otherwise(0)).alias(inf_alias),
                    ])
                names[-1] = (field.name, field.dataType, null_alias, min_alias, max_alias, nan_alias, inf_alias)
            else:
                min_alias, max_alias = f"{safe}__min", f"{safe}__max"
                exprs.extend([F.min(col).alias(min_alias), F.max(col).alias(max_alias)])
                names[-1] = (field.name, field.dataType, null_alias, min_alias, max_alias, None, None)
        row = df.agg(*exprs).first()
        values = row.asDict(recursive=True)
        for entry in names:
            name, dtype, null_alias, min_alias, max_alias, *extra = entry
            nan_alias, inf_alias = extra if extra else (None, None)
            nulls = int(values.get(null_alias) or 0)
            nan = int(values.get(nan_alias) or 0) if nan_alias else 0
            inf = int(values.get(inf_alias) or 0) if inf_alias else 0
            minimum = values.get(min_alias) if min_alias else None
            maximum = values.get(max_alias) if max_alias else None
            output.append((dataset, name, dtype.simpleString(), total_rows, nulls,
                           nulls / total_rows if total_rows else 0.0,
                           nan, nan / total_rows if total_rows else 0.0,
                           inf, inf / total_rows if total_rows else 0.0,
                           None if minimum is None else str(minimum),
                           None if maximum is None else str(maximum),
                           bool(total_rows == 0 or (nulls == total_rows) or
                                                  (minimum is not None and maximum is not None and minimum == maximum))))
        print(f"COLUMN_QUALITY dataset={dataset} columns={offset + len(batch)}/{len(fields)}", flush=True)
    return output


def profile_one(spark, dataset, path):
    started = time.time()
    # Deadline-scoped, deterministic bounded scan. Counts below describe this prefix sample only.
    raw_df = normalize_columns(spark.read.parquet(path).limit(SAMPLE_LIMIT))
    df = label_and_day(raw_df, dataset)
    total = df.count()
    counts = table_for_labels(df.withColumn("dataset", F.lit(dataset)), dataset)
    imbalance = (counts.groupBy("dataset", "label_multi", "label_binary")
                 .agg(F.sum("rows").alias("rows"))
                 .withColumn("max_to_class_ratio", F.max("rows").over(Window.partitionBy("dataset")) / F.col("rows")))
    counts = counts.withColumn("percent", F.col("rows") * F.lit(100.0 / total if total else 0.0))
    quality = column_quality(df, dataset, total)
    duplicate_rows = total - df.dropDuplicates(raw_df.columns).count()
    print(f"PROFILE dataset={dataset} sample_rows={total} sample_limit={SAMPLE_LIMIT} columns={len(raw_df.columns)} exact_duplicate_rows={duplicate_rows} seconds={time.time()-started:.2f}", flush=True)
    return df, counts, imbalance, quality, (dataset, total, len(raw_df.columns), duplicate_rows, time.time() - started)


def main():
    started = time.time()
    spark = spark_session("BDA-Phase2-Profile")
    spark.sparkContext.setLogLevel("WARN")
    input_paths = {
        "cicids2017_flow": f"{RAW}/cicids2017_flow/CICIDS_Flow.parquet",
        "cicids2017_packet": f"{RAW}/cicids2017_packet/cicids2017_packet_slice.parquet",
        "unswnb15": f"{RAW}/unswnb15",
    }
    all_counts, all_imbalance, all_quality, all_summary, map_rows = [], [], [], [], []
    flow = None
    for dataset, path in input_paths.items():
        df, counts, imbalance, quality, summary = profile_one(spark, dataset, path)
        if dataset == "cicids2017_flow":
            flow = df
        if dataset == "cicids2017_packet" and flow is not None:
            packet_key = find_column(df, ["flow_id"])
            flow_key = find_column(flow, ["flow_id"])
            packet_ts = find_column(df, ["timestamp", "packet_timestamp", "time"])
            if packet_key and flow_key and not packet_ts:
                flow_day = flow.select(F.col(flow_key).cast("string").alias("_flow_id"), F.col("day").alias("_flow_day")).dropDuplicates(["_flow_id"])
                df = df.drop("day").join(flow_day, F.col(packet_key).cast("string") == F.col("_flow_id"), "left")
                df = df.withColumn("day", F.coalesce(F.col("_flow_day"), F.lit("UNKNOWN"))).drop("_flow_id", "_flow_day")
                counts = table_for_labels(df.withColumn("dataset", F.lit(dataset)), dataset)
                total = summary[1]
                imbalance = (counts.groupBy("dataset", "label_multi", "label_binary")
                             .agg(F.sum("rows").alias("rows"))
                             .withColumn("max_to_class_ratio", F.max("rows").over(Window.partitionBy("dataset")) / F.col("rows")))
                counts = counts.withColumn("percent", F.col("rows") * F.lit(100.0 / total if total else 0.0))
        all_counts.append(counts)
        all_imbalance.append(imbalance)
        all_quality.extend(quality)
        all_summary.append(summary)
        raw_col = find_column(df, ["label_raw"])
        if raw_col:
            observed = (df.select("label_raw", "label_multi", "label_binary").distinct().collect())
            for row in observed:
                map_rows.append({"dataset": dataset, "raw_label": row["label_raw"],
                                 "canonical_class": row["label_multi"], "binary_class": row["label_binary"]})
        if dataset == "cicids2017_packet":
            protocol = find_column(df, ["protocol", "proto"])
            if protocol:
                protocol_table = df.groupBy(F.lower(F.col(protocol).cast("string")).alias("protocol")).count().withColumnRenamed("count", "rows")
                protocol_table = protocol_table.withColumn("dataset", F.lit(dataset))
                write_small_table(spark, protocol_table, "protocol_distribution")
            payload_size = find_column(df, ["payload_length", "packet_size", "length"])
            if payload_size:
                size = F.col(payload_size).cast("double")
                size_table = df.select(
                    F.when(size.isNull() | F.isnan(size), "missing").when(size <= 0, "0").when(size <= 64, "1-64").when(size <= 128, "65-128")
                    .when(size <= 256, "129-256").when(size <= 512, "257-512")
                    .when(size <= 1024, "513-1024").otherwise("1025+").alias("packet_size_bin")
                ).groupBy("packet_size_bin").count().withColumnRenamed("count", "rows")
                write_small_table(spark, size_table, "packet_size_distribution")
        if dataset == "cicids2017_flow":
            duration = find_column(df, ["flow_duration", "duration"])
            negative = df.filter(F.col(duration).cast("double") < 0).count() if duration else 0
            all_summary[-1] = summary + (negative,)
    labels = all_counts[0]
    for extra in all_counts[1:]:
        labels = labels.unionByName(extra, allowMissingColumns=True)
    imbalance_df = all_imbalance[0]
    for extra in all_imbalance[1:]:
        imbalance_df = imbalance_df.unionByName(extra, allowMissingColumns=True)
    quality_df = spark.createDataFrame(
        all_quality,
        "dataset string, column_name string, data_type string, row_count long, null_count long, null_rate double, nan_count long, nan_rate double, infinity_count long, infinity_rate double, min_value string, max_value string, is_constant boolean",
    ).withColumn("min_value", F.col("min_value").cast("string")).withColumn("max_value", F.col("max_value").cast("string"))
    summary_df = spark.createDataFrame(
        [(r[0], int(r[1]), int(r[2]), int(r[3]), float(r[4]), int(r[5]) if len(r) > 5 else 0)
         for r in all_summary],
        "dataset string, rows_before long, columns long, exact_duplicate_rows long, profile_seconds double, negative_durations long",
    )
    write_small_table(spark, labels, "rows_by_dataset_day_label")
    write_small_table(spark, imbalance_df, "class_imbalance")
    write_small_table(spark, quality_df, "column_quality")
    write_small_table(spark, summary_df, "profile_summary")
    label_map = {"generated_utc_epoch": time.time(), "mapping_rule_version": "canonical_labels_v1", "mappings": map_rows}
    write_hdfs_json(spark, f"{HDFS}/bda/_meta/label_map.json", label_map)
    (LOG_ROOT / "label_map.json").write_text(json.dumps(label_map, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"PROFILE_COMPLETE seconds={time.time()-started:.2f} rows={[(r[0], r[1]) for r in all_summary]}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
