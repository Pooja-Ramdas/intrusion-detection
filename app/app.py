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
def json_artifact(*parts):
    try:
        with artifacts.joinpath(*parts).open(encoding="utf-8") as f: return json.load(f)
    except (OSError,ValueError):
        return {}
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
    st.subheader("Dataset profiles")
    flow=json_artifact("profile","flow_profile.json")
    packet=json_artifact("profile","packet_profile.json")
    unsw=json_artifact("profile","unsw_profile.json")
    features=json_artifact("features","feature_stats.json")
    d1,d2,d3=st.columns(3)
    d1.metric("CIC-IDS2017 flow rows",f"{flow.get('row_count',0):,}",f"{flow.get('col_count',0)} columns")
    d2.metric("CIC-IDS2017 packet rows",f"{packet.get('row_count',0):,}",f"{packet.get('col_count',0)} columns")
    d3.metric("UNSW-NB15 rows",f"{unsw.get('row_count',0):,}",f"{unsw.get('col_count',0)} columns")

    st.markdown("#### CIC-IDS2017 flow labels")
    labels=flow.get("label_distribution",{})
    if labels:
        label_df=pd.DataFrame([{"Label":k,"Rows":v["count"],"Share (%)":v["percent"]} for k,v in labels.items()])
        st.plotly_chart(px.bar(label_df,x="Rows",y="Label",orientation="h",title="Observed flow records by attack label",template="plotly_dark").update_layout(yaxis={"categoryorder":"total ascending"}),use_container_width=True)
        st.dataframe(label_df.sort_values("Rows",ascending=False),use_container_width=True,hide_index=True)

    a,b=st.columns(2)
    packet_labels=packet.get("label_distribution",{})
    if packet_labels:
        packet_df=pd.DataFrame([{"Label":k,"Rows":v["count"],"Share (%)":v["percent"]} for k,v in packet_labels.items()])
        a.markdown("#### Packet label split")
        a.plotly_chart(px.pie(packet_df,names="Label",values="Rows",title="CIC-IDS2017 packet labels",template="plotly_dark"),use_container_width=True)
    feature_groups=features.get("feature_counts_by_group",{})
    if feature_groups:
        group_df=pd.DataFrame([{"Feature group":k,"Features":v} for k,v in feature_groups.items() if k!="total_model_features"])
        b.markdown("#### Model feature composition")
        b.plotly_chart(px.bar(group_df,x="Feature group",y="Features",title=f"{features.get('total_columns',0)} total columns · {feature_groups.get('total_model_features',0)} model features",template="plotly_dark"),use_container_width=True)
    st.caption("Profile counts are from the repository’s saved Spark profiling artifacts; CIC-IDS2017 packet data is a bounded working slice.")
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
    st.subheader("Traffic composition")
    flow=json_artifact("profile","flow_profile.json")
    protocols=flow.get("top_protocols",[])
    ports=flow.get("top_destination_ports",[])
    a,b=st.columns(2)
    if protocols:
        protocol_df=pd.DataFrame(protocols).rename(columns={"protocol":"Protocol","count":"Flows"})
        a.metric("Observed flow records",f"{flow.get('row_count',0):,}")
        a.plotly_chart(px.pie(protocol_df,names="Protocol",values="Flows",title="Flow share by transport protocol",template="plotly_dark"),use_container_width=True)
        a.dataframe(protocol_df,use_container_width=True,hide_index=True)
    if ports:
        ports_df=pd.DataFrame(ports).rename(columns={"port":"Destination port","count":"Flows"})
        b.markdown("#### Most common destination ports")
        b.plotly_chart(px.bar(ports_df,x="Destination port",y="Flows",title="Top 10 destination ports",template="plotly_dark"),use_container_width=True)
        b.dataframe(ports_df,use_container_width=True,hide_index=True)
    stats=flow.get("numeric_stats",{})
    if stats:
        st.markdown("#### Flow numeric profile")
        stats_df=pd.DataFrame.from_dict(stats,orient="index").rename_axis("Feature").reset_index()
        st.dataframe(stats_df,use_container_width=True,hide_index=True)
    st.caption("Traffic charts summarize the saved CIC-IDS2017 flow profile artifact.")
