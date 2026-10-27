# Distributed Intrusion Detection

A containerized big data project for profiling network traffic and exploring flow and packet level intrusion detection with HDFS, Apache Spark MLlib, MongoDB, and a Streamlit dashboard.

## Public dashboard

**Live app:** [BDA | Network Defense](https://intrusion-detection-aeaz7cbuxzpp8vsjza9id6.streamlit.app/)

The Streamlit Community Cloud app is deployed from repository Pooja-Ramdas/intrusion-detection, branch master, entry point app/app.py. Hosted Python dependencies are listed in the repository-root requirements.txt.

Evaluation outputs are in artifacts/evaluation/: per-model metric table, per-class metrics, confusion matrices, and interactive ROC/precision–recall HTML charts. The dashboard’s Detection Results page reads the JSON/CSV files there. These are reduced-sample results; see artifacts/evaluation/README.md for scope and interpretation.

The dashboard renders without Docker. Streamlit Community Cloud cannot resolve the local Compose hostname mongodb; Investigation requires an internet-reachable MongoDB URI in the app’s MONGO_URI secret. The database must contain documents in bda.flows. Keep credentials in Streamlit secrets, not Git.
