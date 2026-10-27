import os
from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo import MongoClient

st.set_page_config(page_title="BDA | Network Defense", page_icon="🛡️", layout="wide")
st.markdown("<style>.block-container{padding-top:1.4rem}.stMetric{background:#101c2c;padding:14px;border-radius:10px}</style>", unsafe_allow_html=True)
st.title("🛡️ Network Traffic Intelligence")
st.caption("Distributed intrusion detection · CIC-IDS2017 · UNSW-NB15")
pages=["Overview","Data Profiling","Detection Results","Traffic Analysis","Investigation","Scalability"]
page=st.sidebar.radio("Workspace",pages)
st.sidebar.caption("HDFS · Spark 3.5 · MongoDB · Streamlit")
profile=Path(os.getenv("BDA_PROFILE_DIR","/profile"))
def table(name):
    p=profile/(name+".csv")
    try: return pd.read_csv(p) if p.exists() else pd.DataFrame()
    except Exception: return pd.DataFrame()
def chart(df,x,y,title,kind="bar"):
    if df.empty or x not in df or y not in df: st.info("No verified table available for this chart."); return
    fig=(px.line if kind=="line" else px.bar)(df,x=x,y=y,title=title,template="plotly_dark"); fig.update_layout(margin=dict(l=10,r=10,t=45,b=10)); st.plotly_chart(fig,use_container_width=True)

if page=="Overview":
    st.subheader("Distributed analytics architecture")
    st.code("Source files → HDFS raw → Spark profiling/features → MLlib → MongoDB investigation → Streamlit",language="text")
    a,b,c=st.columns(3); a.metric("HDFS raw", "22.71 GB logical", "replication 1 · report snapshot"); b.metric("HDFS /bda", "24.43 GB physical", "last measured snapshot"); c.metric("D: free", "72.36 GB", "last disk guard snapshot")
    st.warning("Task 4 training did not finish; no verified classifier metrics are available. Values above are the last report measurements, not live telemetry.")
    st.write("**Data scope:** CIC-IDS2017 packet working slice and ingested flow files; UNSW-NB15 remains external validation. See phase reports for incomplete processing scope.")
elif page=="Data Profiling":
    st.subheader("Measured bounded EDA")
    df=table("rows_by_dataset_day_label"); st.dataframe(df,use_container_width=True); chart(df,"label_multi","rows","Rows by observed label")
    q=table("column_quality"); st.dataframe(q,use_container_width=True); chart(q,"column_name","null_rate","Missing-value rate")
    st.caption("Values are from the bounded EDA recorded in Phase 2; do not generalize to full dataset balance.")
elif page=="Detection Results":
    st.subheader("Held-out model results")
    p=Path("/results/task4_20261027_quick1_metrics.json")
    if p.exists():
        rows=pd.read_json(p); cols=[c for c in ["task","algorithm","train_rows","test_rows","features","accuracy","weighted_precision","weighted_recall","weighted_f1","seconds"] if c in rows]
        st.dataframe(rows[cols],use_container_width=True)
        st.caption("Reduced stratified sample; see phase 4 report for metric limitations and mislabeled macro-F1 field.")
    else: st.info("No copied metrics summary is available. Check the phase 4 report.")
    st.warning("UNSW external validation, per-class metrics and ROC/PR AUC were not measured.")
elif page=="Traffic Analysis":
    st.subheader("Protocol and packet profile")
    d=table("protocol_distribution"); chart(d,"protocol","rows","Observed protocols")
    d=table("packet_size_distribution"); chart(d,"packet_size_bin","rows","Packet size distribution")
    st.info("Payload entropy and top-talker tables are not available in verified Task 3 outputs.")
elif page=="Investigation":
    st.subheader("Flow lookup")
    uri=os.getenv("MONGO_URI","mongodb://mongodb:27017"); label=st.text_input("Label contains","BENIGN"); ip=st.text_input("IP address (optional)")
    if st.button("Search MongoDB"):
        try:
            col=MongoClient(uri,serverSelectionTimeoutMS=1500).bda.flows
            filt={"label":{"$regex":label,"$options":"i"}} if label else {}
            if ip: filt["$or"]=[{"src_ip":ip},{"dst_ip":ip},{"metadata.src_ip":ip},{"metadata.dst_ip":ip}]
            rows=list(col.find(filt,limit=100).sort("_id",-1)); st.write("Returned",len(rows)); st.json(rows[:20])
        except Exception as e: st.error("Mongo query unavailable: "+str(e))
    st.caption("No flow documents were bulk-loaded in this phase; query results may be empty.")
elif page=="Scalability":
    st.subheader("Spark and MongoDB benchmarks")
    st.info("No verified Task 3 scaling CSV or MongoDB query benchmark exists.")
    d=table("profile_summary"); st.dataframe(d,use_container_width=True)
