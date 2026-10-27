from pyspark.sql import functions as F
from common import HDFS, path_exists, spark_session, write_parquet_once, write_small_table


def main():
    spark = spark_session("BDA-Phase2-MakeSplits")
    spark.sparkContext.setLogLevel("WARN")
    root = f"{HDFS}/bda/clean/phase2_v1"
    out = f"{root}/split_assignments"
    if not path_exists(spark, root):
        raise RuntimeError(f"Cleaned phase2 output not found: {root}")
    if path_exists(spark, out):
        raise RuntimeError(f"Refusing to overwrite split assignments: {out}")
    frames = []
    for dataset in ("cicids2017_flow", "cicids2017_packet", "unswnb15"):
        p = f"{root}/{dataset}"
        if not path_exists(spark, p):
            raise RuntimeError(f"Missing cleaned dataset: {p}")
        df = spark.read.parquet(p)
        if "split_key" not in df.columns:
            raise RuntimeError(f"Clean dataset has no deterministic split_key: {p}")
        core = df.select(F.lit(dataset).alias("dataset"), "split_key", "label_multi", "label_binary", "day")
        if dataset == "unswnb15":
            frames.append(core.withColumn("split_scheme", F.lit("external"))
                         .withColumn("split", F.lit("external_validation")))
        else:
            bucket = F.pmod(F.xxhash64("label_multi", "split_key", F.lit(20261027)), F.lit(100))
            stratified = (core.withColumn("split_scheme", F.lit("stratified_70_15_15_seed_20261027"))
                          .withColumn("split", F.when(bucket < 70, "train").when(bucket < 85, "validation").otherwise("test")))
            per_day = (core.withColumn("split_scheme", F.lit("day_holdout_friday"))
                       .withColumn("split", F.when(F.col("day") == "Friday", "test")
                                   .when(F.col("day").isin("Monday", "Tuesday", "Wednesday", "Thursday"), "train")
                                   .otherwise("unassigned_day")))
            frames.extend([stratified, per_day])
    assignments = frames[0]
    for frame in frames[1:]:
        assignments = assignments.unionByName(frame)
    counts = assignments.groupBy("dataset", "split_scheme", "split", "day", "label_multi").count().withColumnRenamed("count", "rows")
    write_small_table(spark, counts, "split_counts")
    write_parquet_once(spark, assignments.repartition(32, "dataset", "split_scheme", "split"), out,
                       ["dataset", "split_scheme", "split"])
    print(f"SPLITS_COMPLETE output={out} seed=20261027", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
