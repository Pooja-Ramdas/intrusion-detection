import csv, json
from pathlib import Path
import numpy as np
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from sklearn.metrics import (accuracy_score, balanced_accuracy_score, cohen_kappa_score,
    confusion_matrix, f1_score, matthews_corrcoef, precision_score, recall_score,
    roc_auc_score, roc_curve, precision_recall_curve, average_precision_score,
    log_loss, brier_score_loss)
from sklearn.preprocessing import label_binarize

BASE="hdfs://namenode:9000/bda"
EXP="task4_20261027_quick1"
OUT=Path("/tmp/task4_evaluation")
OUT.mkdir(parents=True, exist_ok=True)
RUNS=[("binary_rf","Binary","Random Forest"),("binary_gbt","Binary","Gradient-Boosted Trees"),("multiclass_rf","Multiclass","Random Forest")]

def maybe_auc(y,s):
    try: return float(roc_auc_score(y,s))
    except ValueError: return None

def plot_confusion(key,title,labels,cm):
    fig=go.Figure(go.Heatmap(z=cm,x=labels,y=labels,colorscale="Blues",text=cm,texttemplate="%{text}",hovertemplate="Actual %{y}<br>Predicted %{x}<br>Count %{z}<extra></extra>"))
    fig.update_layout(title=title,xaxis_title="Predicted class",yaxis_title="Actual class",yaxis_autorange="reversed")
    fig.write_html(OUT/f"confusion_matrix_{key}.html",include_plotlyjs="cdn")

def plot_curves(key,title,curves,kind):
    fig=go.Figure()
    for c in curves:
        pts=c["points"]; score=c.get("auc") if kind=="roc" else c.get("average_precision")
        name=c["label"]+(f" ({score:.3f})" if score is not None else "")
        fig.add_trace(go.Scatter(x=[p["x"] for p in pts],y=[p["y"] for p in pts],mode="lines",name=name))
    if kind=="roc":
        fig.add_trace(go.Scatter(x=[0,1],y=[0,1],mode="lines",name="Chance",line={"dash":"dash","color":"gray"}))
        fig.update_layout(xaxis_title="False positive rate",yaxis_title="True positive rate")
    else:
        fig.update_layout(xaxis_title="Recall",yaxis_title="Precision")
    fig.update_layout(title=title)
    fig.write_html(OUT/f"{kind}_curve_{key}.html",include_plotlyjs="cdn")

def evaluate(spark,key,task,algo):
    pipe=PipelineModel.load(f"{BASE}/models/{EXP}/{key}")
    labels=[str(x) for x in pipe.stages[1].labels]
    rows=spark.read.parquet(f"{BASE}/results/predictions/{EXP}/{key}").select("_label","label_index","prediction","probability").collect()
    if not rows: raise RuntimeError("No saved predictions for "+key)
    y=np.array([int(r.label_index) for r in rows],dtype=int)
    pred=np.array([int(r.prediction) for r in rows],dtype=int)
    probs=np.array([r.probability.toArray().tolist() for r in rows],dtype=float)
    ids=list(range(len(labels)))
    cm=confusion_matrix(y,pred,labels=ids)
    item={"experiment":EXP,"model_key":key,"task":task,"algorithm":algo,"test_rows":len(rows),"class_count":len(labels),
        "accuracy":float(accuracy_score(y,pred)),"balanced_accuracy":float(balanced_accuracy_score(y,pred)),
        "precision_micro":float(precision_score(y,pred,labels=ids,average="micro",zero_division=0)),
        "precision_macro":float(precision_score(y,pred,labels=ids,average="macro",zero_division=0)),
        "precision_weighted":float(precision_score(y,pred,labels=ids,average="weighted",zero_division=0)),
        "recall_micro":float(recall_score(y,pred,labels=ids,average="micro",zero_division=0)),
        "recall_macro":float(recall_score(y,pred,labels=ids,average="macro",zero_division=0)),
        "recall_weighted":float(recall_score(y,pred,labels=ids,average="weighted",zero_division=0)),
        "f1_micro":float(f1_score(y,pred,labels=ids,average="micro",zero_division=0)),
        "f1_macro":float(f1_score(y,pred,labels=ids,average="macro",zero_division=0)),
        "f1_weighted":float(f1_score(y,pred,labels=ids,average="weighted",zero_division=0)),
        "matthews_corrcoef":float(matthews_corrcoef(y,pred)),"cohen_kappa":float(cohen_kappa_score(y,pred)),
        "classes":labels,"confusion_matrix":{"labels":labels,"matrix":cm.tolist()},"per_class":[],"curves":{"roc":[],"precision_recall":[]}}
    try:item["log_loss"]=float(log_loss(y,probs,labels=ids))
    except ValueError:item["log_loss"]=None
    if len(labels)==2:
        pos=labels.index("ATTACK") if "ATTACK" in labels else 1
        ybin=(y==pos).astype(int); score=probs[:,pos]
        item["positive_class"]=labels[pos]
        item["roc_auc"]=maybe_auc(ybin,score)
        item["pr_auc_average_precision"]=float(average_precision_score(ybin,score))
        item["brier_score"]=float(brier_score_loss(ybin,score))
        if len(np.unique(ybin))>1:
            x,ycurve,t=roc_curve(ybin,score)
            item["curves"]["roc"].append({"label":labels[pos]+" vs rest","auc":item["roc_auc"],"points":[{"x":float(a),"y":float(b)} for a,b in zip(x,ycurve)]})
        p,r,t=precision_recall_curve(ybin,score)
        item["curves"]["precision_recall"].append({"label":labels[pos]+" vs rest","average_precision":item["pr_auc_average_precision"],"points":[{"x":float(a),"y":float(b)} for a,b in zip(r,p)]})
        tn,fp,fn,tp=[int(v) for v in cm.ravel()]
        item.update({"sensitivity":tp/(tp+fn) if tp+fn else None,"specificity":tn/(tn+fp) if tn+fp else None,
            "false_positive_rate":fp/(fp+tn) if fp+tn else None,"false_negative_rate":fn/(fn+tp) if fn+tp else None})
    else:
        onehot=label_binarize(y,classes=ids)
        for keym,avg in [("roc_auc_ovr_macro","macro"),("roc_auc_ovr_weighted","weighted")]:
            try:item[keym]=float(roc_auc_score(onehot,probs,average=avg))
            except ValueError:item[keym]=None
        for keym,avg in [("pr_auc_ovr_macro","macro"),("pr_auc_ovr_weighted","weighted")]:
            item[keym]=float(average_precision_score(onehot,probs,average=avg))
        item["brier_score_multiclass"]=float(np.mean(np.sum((probs-onehot)**2,axis=1)))
    for i,label in enumerate(labels):
        tp=int(cm[i,i]); support=int(cm[i,:].sum()); predicted=int(cm[:,i].sum())
        fp=predicted-tp; fn=support-tp; tn=int(cm.sum())-tp-fp-fn
        pc={"class":label,"support":support,"precision":tp/predicted if predicted else 0.0,
            "recall":tp/support if support else 0.0,"f1":2*tp/(2*tp+fp+fn) if 2*tp+fp+fn else 0.0,
            "specificity":tn/(tn+fp) if tn+fp else None}
        actual=(y==i).astype(int); score=probs[:,i]
        pc["roc_auc_ovr"]=maybe_auc(actual,score)
        pc["average_precision_ovr"]=float(average_precision_score(actual,score)) if actual.sum() else None
        item["per_class"].append(pc)
        if len(labels)>2 and len(np.unique(actual))>1:
            x,yc,t=roc_curve(actual,score)
            item["curves"]["roc"].append({"label":label,"auc":pc["roc_auc_ovr"],"points":[{"x":float(a),"y":float(b)} for a,b in zip(x,yc)]})
            p,r,t=precision_recall_curve(actual,score)
            item["curves"]["precision_recall"].append({"label":label,"average_precision":pc["average_precision_ovr"],"points":[{"x":float(a),"y":float(b)} for a,b in zip(r,p)]})
    plot_confusion(key,f"{task} {algo} — confusion matrix",labels,cm.tolist())
    plot_curves(key,f"{task} {algo} — ROC curve",item["curves"]["roc"],"roc")
    plot_curves(key,f"{task} {algo} — precision–recall curve",item["curves"]["precision_recall"],"pr")
    return item

def main():
    spark=SparkSession.builder.appName("BDA-Task4-Evaluation").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    models={}
    for key,task,algo in RUNS:
        models[key]=evaluate(spark,key,task,algo)
        print(key,json.dumps({k:v for k,v in models[key].items() if k not in ("classes","confusion_matrix","per_class","curves")}),flush=True)
    allclasses=[{"model_key":k,"task":v["task"],"algorithm":v["algorithm"],**c} for k,v in models.items() for c in v["per_class"]]
    result={"experiment":EXP,"source_predictions":f"{BASE}/results/predictions/{EXP}",
        "note":"Reduced seeded held-out prediction samples; not full-dataset estimates. Binary positive class is ATTACK; multiclass AUC curves are one-vs-rest.",
        "models":models}
    (OUT/"evaluation_metrics.json").write_text(json.dumps(result,indent=2,allow_nan=False),encoding="utf-8")
    (OUT/"confusion_matrices.json").write_text(json.dumps({k:v["confusion_matrix"] for k,v in models.items()},indent=2),encoding="utf-8")
    with (OUT/"model_metrics.csv").open("w",newline="",encoding="utf-8") as f:
        rows=[{k:v for k,v in m.items() if k not in ("classes","confusion_matrix","per_class","curves")} for m in models.values()]
        fields=list(dict.fromkeys(k for row in rows for k in row));w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)
    with (OUT/"per_class_metrics.csv").open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=list(allclasses[0]));w.writeheader();w.writerows(allclasses)
    spark.stop()
    print("EVALUATION_OUTPUT",str(OUT),flush=True)

if __name__=="__main__": main()
