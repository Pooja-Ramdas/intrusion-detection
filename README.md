# Distributed Intrusion Detection

A containerized big data project for profiling network traffic and exploring flow and packet level intrusion detection with HDFS, Apache Spark MLlib, MongoDB, and a Streamlit dashboard.

## Public dashboard

**Live app:** [BDA | Network Defense](https://intrusion-detection-aeaz7cbuxzpp8vsjza9id6.streamlit.app/)

The Streamlit Community Cloud app is deployed from repository `Pooja-Ramdas/intrusion-detection`, branch `master`, entry point `app/app.py`. Hosted Python dependencies are listed in the repository-root `requirements.txt`.

The dashboard renders without Docker. MongoDB investigation requires a reachable MongoDB service and an `MONGO_URI` app secret. Local profile and result files are not included in the hosted runtime by default, so those sections may show their empty-state messages.
