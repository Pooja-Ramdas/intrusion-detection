"""
app.py — BDA Streamlit Dashboard (placeholder for Phase 1; expanded in later phases).
"""
import streamlit as st
import json
from pathlib import Path

st.set_page_config(page_title="BDA IDS Dashboard", layout="wide")
st.title("🛡️ Distributed Intrusion Detection — BDA Dashboard")
st.caption("Distributed Multiclass Intrusion Detection and Traffic Profiling")

st.info("Pipeline phases will populate this dashboard. Check back after each phase.")

# Show dataset manifest if available
manifest_path = Path("/artifacts/dataset_manifest.json")
if manifest_path.exists():
    st.subheader("Dataset Manifest")
    manifest = json.loads(manifest_path.read_text())
    for cat, info in manifest.items():
        with st.expander(f"📦 {cat}"):
            if info.get("repo_id"):
                st.write(f"**Repo:** [{info['repo_id']}]({info.get('repo_url','')})")
                st.write(f"**Files:** {info.get('total_files', 0)},  "
                         f"**Total:** {info.get('total_mb', 0)} MB")
                if info.get("files"):
                    import pandas as pd
                    st.dataframe(pd.DataFrame(info["files"]), use_container_width=True)
            else:
                st.warning("Not discovered yet — run discover.py")

# Show staging manifest if available
staging_path = Path("/artifacts/staging_manifest.json")
if staging_path.exists():
    st.subheader("Staged Data")
    staging = json.loads(staging_path.read_text())
    st.metric("Total staged", f"{staging.get('total_mb', 0)} MB")

st.sidebar.markdown("### Navigation")
st.sidebar.markdown("- Phase 1: Ingest & HDFS")
st.sidebar.markdown("- Phase 2: MongoDB (coming)")
st.sidebar.markdown("- Phase 3: Features (coming)")
st.sidebar.markdown("- Phase 4: Models (coming)")
