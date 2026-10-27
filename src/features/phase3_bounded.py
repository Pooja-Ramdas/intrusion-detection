import time
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, VectorAssembler, StandardScaler
from feature_safety import safe

SEED = 20261027
BASE = "hdfs://namenode:9000/bda"
OUT = BASE + "/features/phase3_v1"
MODEL = BASE + "/models/preprocess/phase3_v1"
def main():
    t0 = time.time()
    s = SparkSession.builder.appName("BDA-Phase3-BoundedFeatures").getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    flows0 = s.read.parquet(BASE + "/clean/flows")
    flows0 = flows0.sample(False, 0.1, SEED).limit(2000)
    cols = {c.lower().strip(): c for c in flows0.columns}
    def col(*opts):
        for o in opts:
            if o.lower() in cols: return F.col("`"+cols[o.lower()].replace("`","``")+"`")
        return None
    fid = col("flow_id")
    if fid is None: raise RuntimeError("flow_id missing from CIC clean flows")
    label = col("label_multi", "attack_label", "label_raw")
    split = col("split_stratified")
    if split is None: split = F.when(F.pmod(F.xxhash64(fid),100)<70,"train").when(F.pmod(F.xxhash64(fid),100)<85,"validation").otherwise("test")
    # Safe shared flow features; all absent columns are explicitly omitted.
    mapping = [
      ("duration_seconds",("flow_duration","dur"),1e-6),
      ("forward_packet_count",("total_fwd_packets","spkts"),1),
      ("backward_packet_count",("total_backward_packets","dpkts"),1),
      ("forward_bytes",("total_length_of_fwd_packets","sbytes"),1),
      ("backward_bytes",("total_length_of_bwd_packets","dbytes"),1),
      ("flow_bytes_per_second",("flow_bytes_s","bytes_per_second"),1),
      ("flow_packets_per_second",("flow_packets_s","packets_per_second"),1),
      ("flow_iat_mean",("flow_iat_mean",),1), ("flow_iat_std",("flow_iat_std",),1),
      ("syn_flag_count",("syn_flag_count",),1), ("ack_flag_count",("ack_flag_count",),1),
    ]
    selected=[]
    for out, srcs, scale in mapping:
        c=col(*srcs)
        if c is not None: selected.append((out,c.cast("double")*F.lit(scale)))
    if not selected: raise RuntimeError("no usable numeric flow columns")
    flow=flows0.select(fid.cast("string").alias("flow_id"), *( [label.cast("string").alias("label")] if label is not None else []), F.lit("cicids2017_flow").alias("dataset"), split.cast("string").alias("split"), *[v.alias(k) for k,v in selected])
    # Packet-level sample from the existing 169 MB slice only.
    packets=s.read.parquet(BASE+"/raw/cicids2017_packet/cicids2017_packet_slice.parquet").limit(2000)
    pc={c.lower():c for c in packets.columns}
    pflow=pc.get("flow_id")
    bytecols=sorted([c for c in packets.columns if c.lower().startswith(("packet_byte_","payload_byte_"))], key=lambda x:int(x.rsplit("_",1)[-1]))[:64]
    if pflow and bytecols:
        p=packets.select(F.col(pflow).cast("string").alias("flow_id"),*[F.col(c).cast("int").alias("b%d"%i) for i,c in enumerate(bytecols)])
        arr=F.array(*[F.col("b%d"%i) for i in range(len(bytecols))])
        exploded=p.select("flow_id",F.explode(arr).alias("byte")).where(F.col("byte").isNotNull())
        counts=exploded.withColumn("bin",F.floor(F.col("byte")/16).cast("int")).groupBy("flow_id","byte","bin").count()
        totals=counts.groupBy("flow_id").agg(F.sum("count").alias("n"),F.sum(F.when(F.col("byte").between(32,126),F.col("count")).otherwise(0)).alias("printable"),F.sum(F.when(F.col("byte")==0,F.col("count")).otherwise(0)).alias("nulls"),F.sum(-F.col("count")*F.log(F.col("count"))).alias("hraw"))
        h=counts.groupBy("flow_id","bin").agg(F.sum("count").alias("n_bin")).groupBy("flow_id").pivot("bin",list(range(16))).sum("n_bin")
        for i in range(16): h=h.withColumn("payload_hist_%02d"%i,F.coalesce(F.col(str(i)),F.lit(0))/F.lit(float(len(bytecols))))
        stats=totals.join(h,"flow_id","left").select("flow_id",(F.col("hraw")/F.col("n")/F.lit(-0.6931471805599453)).alias("payload_entropy_nats"),(F.col("printable")/F.col("n")).alias("printable_byte_ratio"),(F.col("nulls")/F.col("n")).alias("null_byte_ratio"),*[F.col("payload_hist_%02d"%i) for i in range(16)])
        flow=flow.join(stats,"flow_id","left")
    feature_cols=[c for c in flow.columns if c not in ("flow_id","label","dataset","split")]
    safe(feature_cols)
    print("FEATURE_VECTOR_LEAKAGE_TEST_PASS", len(feature_cols), flush=True)
    # Feature and metadata outputs are separate. Identifiers/labels excluded from vector.
    metadata=flow.select("flow_id","dataset","split",*[c for c in ["label"] if c in flow.columns])
    features=flow.select("dataset","split",*[c for c in ["label"] if c in flow.columns],*feature_cols)
    # Fit only the training split. Missing packet-derived values are median-imputed.
    train=features.where((F.col("dataset")=="cicids2017_flow")&(F.col("split")=="train"))
    imputed=[c+"_imp" for c in feature_cols]
    imp=Imputer(inputCols=feature_cols,outputCols=imputed,strategy="median")
    vec=VectorAssembler(inputCols=imputed,outputCol="features_unscaled",handleInvalid="keep")
    scale=StandardScaler(inputCol="features_unscaled",outputCol="features_scaled",withMean=False,withStd=True)
    pipe=Pipeline(stages=[imp,vec,scale]); model=pipe.fit(train)
    model.write().save(MODEL)
    model.transform(features).select("dataset","split",*[c for c in ["label"] if c in flow.columns],"features_scaled").write.mode("errorifexists").option("compression","snappy").parquet(OUT+"/vectors")
    features.write.mode("errorifexists").option("compression","snappy").parquet(OUT+"/readable")
    metadata.write.mode("errorifexists").option("compression","snappy").parquet(OUT+"/metadata")
    count=features.count(); train_count=train.count()
    print("PHASE3_RESULT rows=%d train=%d dimensions=%d seconds=%.2f paths=%s,%s"%(count,train_count,len(feature_cols),time.time()-t0,OUT,MODEL),flush=True)
    print("PHASE3_SCHEMA",features.schema.simpleString(),flush=True)
    s.stop()

if __name__=="__main__": main()
