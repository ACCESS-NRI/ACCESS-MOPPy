import os
import shlex
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

DB_PATH = Path(
    os.getenv("CMOR_TRACKER_DB", Path.home() / ".moppy" / "db" / "cmor_tasks.db")
)

st.set_page_config(page_title="CMORisation Tracker", layout="wide")
st.title("🧼 ACCESS CMORisation Dashboard")


@st.cache_data(ttl=10)
def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM cmor_tasks", conn)
    conn.close()
    return df


df = load_data()

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    statuses = df["status"].unique().tolist()
    selected_statuses = st.multiselect("Status", options=statuses, default=statuses)
    experiments = df["experiment"].unique().tolist()
    selected_experiments = st.multiselect(
        "Experiment", options=experiments, default=experiments
    )

# Apply filters
filtered_df = df[
    df["status"].isin(selected_statuses) & df["experiment"].isin(selected_experiments)
]

st.markdown(f"### Showing {len(filtered_df)} task(s)")
st.dataframe(filtered_df, use_container_width=True)

# Summary stats
st.markdown("### 📊 Summary")
summary = df["status"].value_counts().rename_axis("status").reset_index(name="count")
st.table(summary)

# Errors
if "failed" in df["status"].values:
    st.markdown("### ❌ Failed Tasks")
    st.dataframe(
        df[df["status"] == "failed"][["variable", "experiment", "error_message"]],
        use_container_width=True,
    )


def main():
    import os
    import subprocess
    from pathlib import Path

    db_path = Path(
        os.getenv("CMOR_TRACKER_DB", Path.home() / ".moppy" / "db" / "cmor_tasks.db")
    )
    # Security: escape __file__ to prevent injection
    escaped_file = shlex.quote(__file__)
    escaped_db_path = shlex.quote(str(db_path))
    subprocess.run(  # noqa: S603  # nosec B603
        ["streamlit", "run", escaped_file],
        env={**os.environ, "CMOR_TRACKER_DB": escaped_db_path},
    )
