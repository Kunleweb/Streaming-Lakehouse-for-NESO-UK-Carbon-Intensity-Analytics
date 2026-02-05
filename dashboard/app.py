import time
import pandas as pd
import streamlit as st
from deltalake import DeltaTable

SILVER_PATH = "data/delta/silver_carbon_intensity"

st.set_page_config(page_title="UK Carbon Intensity Live", layout="wide")
st.title("UK Carbon Intensity — Live Streaming Dashboard")

refresh_seconds = st.sidebar.slider("Refresh every (seconds)", 5, 60, 15)

def load_silver():
    dt = DeltaTable(SILVER_PATH)
    df = dt.to_pandas()
    # Convert timestamps
    df["from_ts"] = pd.to_datetime(df["from_ts"], utc=True)
    df["to_ts"] = pd.to_datetime(df["to_ts"], utc=True)
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)
    return df.sort_values("from_ts")

placeholder = st.empty()

while True:
    try:
        df = load_silver()

        with placeholder.container():
            col1, col2, col3 = st.columns(3)

            latest = df.iloc[-1] if len(df) else None
            if latest is not None:
                col1.metric("Latest Forecast (gCO₂/kWh)", int(latest["forecast_gco2_kwh"]) if pd.notnull(latest["forecast_gco2_kwh"]) else None)
                col2.metric("Latest Actual (gCO₂/kWh)", int(latest["actual_gco2_kwh"]) if pd.notnull(latest["actual_gco2_kwh"]) else None)
                col3.metric("Index", latest["index"])

            st.subheader("Last 24 hours")
            cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=24)
            last24 = df[df["from_ts"] >= cutoff]

            st.line_chart(
                last24.set_index("from_ts")[["forecast_gco2_kwh", "actual_gco2_kwh"]]
            )

            st.subheader("Index Distribution (last 24 hours)")
            if len(last24):
                st.bar_chart(last24["index"].value_counts())

            st.caption(f"Rows in Silver: {len(df)}")

    except Exception as e:
        st.error(f"Dashboard error: {e}")

    time.sleep(refresh_seconds)
