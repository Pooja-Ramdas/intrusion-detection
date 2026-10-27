# Distributed Intrusion Detection

A containerized big data project for profiling network traffic and exploring flow and packet level intrusion detection with HDFS, Apache Spark MLlib, MongoDB, and a Streamlit dashboard.

## Public dashboard

The Streamlit entry point is `app/app.py`; its hosted Python dependencies are listed in the repository-root `requirements.txt`. To publish it, sign in to [Streamlit Community Cloud](https://share.streamlit.io/) with GitHub, create an app using repository `Pooja-Ramdas/intrusion-detection`, branch `master`, and file path `app/app.py`, then choose a public app URL. After deployment, Community Cloud provides a shareable `https://<app-name>.streamlit.app` URL.

The dashboard can render without Docker. MongoDB investigation requires a reachable MongoDB service and an `MONGO_URI` app secret. Local profile and result files are not included in the hosted runtime by default, so those sections may show their empty-state messages.
