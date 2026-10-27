import json
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="BDA | Network Defense", page_icon="🛡️", layout="wide")
st.markdown("<style>.block-container{padding-top:1.4rem}.stMetric{background:#101c2c;padding:14px;border-radius:10px}</style>", unsafe_allow_html=True)
st.title("🛡️ Network Traffic Intelligence")
st.caption("Distributed intrusion detection · CIC-IDS2017 · UNSW-NB15")
pages=["Overview","Data Profiling","Detection Results","Traffic Analysis"]
page=st.sidebar.radio("Workspace",pages)
st.sidebar.caption("HDFS · Spark 3.5 · Streamlit")

profile=Path(os.getenv("BDA_PROFILE_DIR","/profile"))
def artifact_dir():
    configured=os.getenv("BDA_ARTIFACT_DIR")
    if configured:
        return Path(configured)
    if Path("/artifacts/evaluation").is_dir():
        return Path("/artifacts")
    return Path.cwd()/"artifacts"

artifacts=artifact_dir()
def table(name):
    p=profile/(name+".csv")
    try: return pd.read_csv(p) if p.exists() else pd.DataFrame()
    except Exception: return pd.DataFrame()
def chart(df,x,y,title,kind="bar"):
    if df.empty or x not in df or y not in df:
        st.info("No verified table available for this chart.")
        return
    fig=(px.line if kind=="line" else px.bar)(df,x=x,y=y,title=title,template="plotly_dark")
    fig.update_layout(margin=dict(l=10,r=10,t=45,b=10))
    st.plotly_chart(fig,use_container_width=True)
def evaluation():
    path=artifacts/"evaluation"/"evaluation_metrics.json"
    try:
        with path.open(encoding="utf-8") as f: return json.load(f)
    except (OSError,ValueError):
        return None
def curve_figure(curves,title,x_title,y_title,baseline=False):
    fig=go.Figure()
    for item in curves:
        points=item.get("points",[])
        if not points: continue
        score=item.get("auc") if y_title=="True positive rate" else item.get("average_precision")
        name=item.get("label","class")
        if score is not None: name+=f" ({score:.3f})"
        fig.add_trace(go.Scatter(x=[p["x"] for p in points],y=[p["y"] for p in points],mode="lines",name=name))
    if baseline:
        fig.add_trace(go.Scatter(x=[0,1],y=[0,1],mode="lines",name="Chance",line={"dash":"dash","color":"gray"}))
    fig.update_layout(title=title,xaxis_title=x_title,yaxis_title=y_title,template="plotly_dark",margin=dict(l=10,r=10,t=45,b=10))
    return fig

if page=="Overview":
    st.subheader("Distributed analytics architecture")
    st.code("Source files → HDFS raw → Spark profiling/features → MLlib → Streamlit",language="text")
    a,b,c=st.columns(3)
    a.metric("HDFS raw","22.71 GB logical","replication 1 · report snapshot")
    b.metric("HDFS /bda","24.43 GB physical","last measured snapshot")
    c.metric("D: free","72.36 GB","last disk guard snapshot")
    st.info("Evaluation metrics use the saved seeded held-out sample; see Detection Results for scores, confusion matrices, and ROC/PR curves.")
    st.write("**Data scope:** CIC-IDS2017 packet working slice and ingested flow files; UNSW-NB15 remains external validation. See phase reports for incomplete processing scope.")
elif page=="Data Profiling":
    st.subheader("Measured bounded EDA")
    df=table("rows_by_dataset_day_label")
    st.dataframe(df,use_container_width=True)
    chart(df,"label_multi","rows","Rows by observed label")
    q=table("column_quality")
    st.dataframe(q,use_container_width=True)
    chart(q,"column_name","null_rate","Missing-value rate")
    st.caption("Values are from the bounded EDA recorded in Phase 2; do not generalize to full dataset balance.")
elif page=="Detection Results":
    st.subheader("Held-out model evaluation")
    result=evaluation()
    if not result:
        st.info("Evaluation results are not available yet.")
    else:
        st.caption(result.get("note","Metrics are calculated from saved held-out predictions."))
        models=result.get("models",{})
        summary_path=artifacts/"evaluation"/"model_metrics.csv"
        try:
            summary=pd.read_csv(summary_path)
            st.dataframe(summary,use_container_width=True,hide_index=True)
        except (OSError,ValueError):
            pass
        if models:
            key=st.selectbox("Model",list(models),format_func=lambda k:f"{models[k]['task']} · {models[k]['algorithm']}")
            item=models[key]
            cm=item["confusion_matrix"]
            fig=go.Figure(go.Heatmap(z=cm["matrix"],x=cm["labels"],y=cm["labels"],colorscale="Blues",text=cm["matrix"],texttemplate="%{text}",hovertemplate="Actual %{y}<br>Predicted %{x}<br>Count %{z}<extra></extra>"))
            fig.update_layout(title="Confusion matrix",xaxis_title="Predicted class",yaxis_title="Actual class",yaxis_autorange="reversed",template="plotly_dark")
            st.plotly_chart(fig,use_container_width=True)
            roc=item.get("curves",{}).get("roc",[])
            pr=item.get("curves",{}).get("precision_recall",[])
            left,right=st.columns(2)
            left.plotly_chart(curve_figure(roc,"ROC curve","False positive rate","True positive rate",baseline=True),use_container_width=True)
            right.plotly_chart(curve_figure(pr,"Precision–recall curve","Recall","Precision"),use_container_width=True)
            class_path=artifacts/"evaluation"/"per_class_metrics.csv"
            try:
                details=pd.read_csv(class_path)
                st.subheader("Per-class metrics")
                st.dataframe(details[details["model_key"]==key],use_container_width=True,hide_index=True)
            except (OSError,ValueError):
                pass
        st.caption("Evaluation uses a reduced seeded sample. Multiclass AUC is one-vs-rest; see docs/phase_4_report.md for scope details.")
elif page=="Traffic Analysis":
    st.subheader("Protocol and packet profile")
    d=table("protocol_distribution")
    chart(d,"protocol","rows","Observed protocols")
    d=table("packet_size_distribution")
    chart(d,"packet_size_bin","rows","Packet size distribution")
    st.info("Payload entropy and top-talker tables are not available in verified Task 3 outputs.")
