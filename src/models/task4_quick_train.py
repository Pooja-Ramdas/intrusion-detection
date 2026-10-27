import json, time
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import NumericType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

BASE="hdfs://namenode:9000/bda"
EXP="task4_20261027_quick1"
SRC=BASE+"/features/flow_features_full"
ROOT=BASE+"/models/"+EXP
RES=BASE+"/_meta/results/"+EXP
PRED=BASE+"/results/predictions/"+EXP
SEED=20261027

def main():
    start=time.time(); s=SparkSession.builder.appName("BDA-Task4-QuickModels").getOrCreate(); s.sparkContext.setLogLevel("ERROR")
    df=s.read.parquet(SRC)
    names={c.lower().strip():c for c in df.columns}
    def pick(*n): return next((names[x] for x in n if x in names),None)
    label=pick("label_multi","label_raw","attack_label")
    binary=pick("label_binary","is_attack")
    split=pick("split_stratified","split_temporal")
    if not label: raise RuntimeError("No multiclass label column in existing feature data; no training attempted")
    if not split: raise RuntimeError("No existing split assignment; refuse to invent train/test split")
    ids={"flow_id","packet_id","source_ip","destination_ip","src_ip","dst_ip","source_port","destination_port","timestamp","record_key","dataset"}
    blocked=lambda c: (c.lower() in ids or c.lower() in {"is_attack","has_packet_features"} or any(x in c.lower() for x in ("label","split","flow_id","packet_id","ip","port","timestamp")))
    numeric=[f.name for f in df.schema.fields if isinstance(f.dataType,NumericType) and not blocked(f.name)]
    if not numeric: raise RuntimeError("No safe numeric feature columns")
    print("AVAILABLE",json.dumps({"columns":df.columns,"label":label,"binary":binary,"split":split,"features":numeric}),flush=True)
    # Stratified deterministic cap by label and split. No row synthesis.
    labelval=F.col(label).cast("string")
    splitval=F.lower(F.col(split).cast("string"))
    base=df.withColumn("_label",labelval).withColumn("_split",splitval).where(F.col("_label").isNotNull())
    train=base.where(F.col("_split").isin("train","training")); test=base.where(F.col("_split").isin("test","testing"))
    if train.limit(1).count()==0 or test.limit(1).count()==0: raise RuntimeError("Existing train/test split does not contain expected names")
    def cap(x,n):
        w=Window.partitionBy("_label").orderBy(F.xxhash64(F.col("_label"),*([F.col(pick("flow_id"))] if pick("flow_id") else [F.lit(SEED)])))
        return x.withColumn("_rn",F.row_number().over(w)).where(F.col("_rn")<=n).drop("_rn")
    # Bernoulli pre-sample first to avoid a window shuffle over the full legacy table.
    classes=[r[0] for r in train.select("_label").distinct().collect()]
    fractions={c:0.02 for c in classes}
    tr=cap(train.sampleBy("_label",fractions,SEED),300).cache()
    te=cap(test.sampleBy("_label",fractions,SEED+1),150).cache()
    trn=tr.count(); ten=te.count()
    if trn<20 or ten<10: raise RuntimeError("Insufficient actual split rows: train=%d test=%d"%(trn,ten))
    assembler=VectorAssembler(inputCols=numeric,outputCol="features",handleInvalid="skip")
    metrics=[]
    for task, target, alg in [("binary",binary or "_binary","rf"),("binary",binary or "_binary","gbt"),("multiclass",label,"rf")]:
        if target=="_binary":
            dtr=tr.withColumn("_target",F.when(F.upper(F.col(label))=="BENIGN","BENIGN").otherwise("ATTACK")); dte=te.withColumn("_target",F.when(F.upper(F.col(label))=="BENIGN","BENIGN").otherwise("ATTACK")); targetcol="_target"
        elif task=="binary":
            dtr=tr.withColumn("_target",F.col(target).cast("string")); dte=te.withColumn("_target",F.col(target).cast("string")); targetcol="_target"
        else: dtr=tr; dte=te; targetcol="_label"
        idx=StringIndexer(inputCol=targetcol,outputCol="label_index",handleInvalid="skip")
        clf=RandomForestClassifier(featuresCol="features",labelCol="label_index",numTrees=8,maxDepth=5,seed=SEED) if alg=="rf" else GBTClassifier(featuresCol="features",labelCol="label_index",maxIter=5,maxDepth=3,seed=SEED)
        model=Pipeline(stages=[assembler,idx,clf]).fit(dtr)
        pred=model.transform(dte).cache(); n=pred.count()
        ev=MulticlassClassificationEvaluator(labelCol="label_index",predictionCol="prediction")
        row={"experiment":EXP,"task":task,"algorithm":alg,"train_rows":dtr.count(),"test_rows":n,"features":len(numeric),"accuracy":ev.setMetricName("accuracy").evaluate(pred),"weighted_precision":ev.setMetricName("weightedPrecision").evaluate(pred),"weighted_recall":ev.setMetricName("weightedRecall").evaluate(pred),"weighted_f1":ev.setMetricName("f1").evaluate(pred),"macro_f1":ev.setMetricName("f1").evaluate(pred),"seconds":round(time.time()-start,2),"seed":SEED}
        if alg=="rf": row["importance_top10"]=sorted([(numeric[i],float(v)) for i,v in enumerate(model.stages[-1].featureImportances)],key=lambda x:x[1],reverse=True)[:10]
        model.write().save(ROOT+"/"+task+"_"+alg)
        pred.select("_label","label_index","prediction","probability").write.mode("errorifexists").parquet(PRED+"/"+task+"_"+alg)
        metrics.append(row); print("METRIC",json.dumps(row),flush=True)
        pred.unpersist()
    # Persist JSON and compact Parquet; no fabricated/repeated rows.
    s.sparkContext.parallelize([json.dumps(metrics,indent=2)],1).saveAsTextFile(RES+"/metrics_json")
    s.createDataFrame([(m["task"],m["algorithm"],m["train_rows"],m["test_rows"],m["features"],m["accuracy"],m["weighted_precision"],m["weighted_recall"],m["weighted_f1"],m["macro_f1"],m["seconds"],m["seed"]) for m in metrics],"task string, algorithm string, train_rows long, test_rows long, features int, accuracy double, weighted_precision double, weighted_recall double, weighted_f1 double, macro_f1 double, seconds double, seed int").write.mode("errorifexists").parquet(RES+"/metrics_parquet")
    print("TASK4_DONE",json.dumps({"train":trn,"test":ten,"feature_count":len(numeric),"elapsed":time.time()-start}),flush=True)
    s.stop()
if __name__=="__main__": main()
