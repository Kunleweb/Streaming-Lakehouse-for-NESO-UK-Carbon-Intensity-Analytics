import time
import pandas as pd
import streamlit as st
from deltalake import DeltaTable

SILVER_PATH = "data/delta/silver_carbon_intensity"

DATA_SOURCE_NAME = "UK National Grid ESO Carbon Intensity API"
DATA_SOURCE_ENDPOINT = "https://api.carbonintensity.org.uk/intensity"
STREAM_DESCRIPTION = (
    "Streaming half-hour carbon intensity windows for the UK national grid "
    "(forecast gCO₂/kWh, actual gCO₂/kWh when available, and intensity index)."
)
PRODUCER_POLL_SECONDS = 60  # keep in sync with producer/poller.py

st.set_page_config(page_title="UK Carbon Intensity Live", layout="wide")
st.title("UK National Grid ESO Carbon Intensity API — Live Streaming Dashboard")

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
        
        # Calculate stats
        coverage_text = "No data yet."
        latest_fetch_text = "N/A"
        source_value = "N/A"

        if len(df):
            coverage_start = df["from_ts"].min()
            coverage_end = df["to_ts"].max()
            coverage_text = f"{coverage_start} → {coverage_end}"

            latest_fetch = df["fetched_at"].max()
            latest_fetch_text = f"{latest_fetch}"
            
            source_value = df["source"].iloc[-1]

        with placeholder.container():
            if len(df) < 10:
                st.info(
                    f"Streaming warm-up in progress. "
                    f"Collected {len(df)} half-hour window(s). "
                    "Charts will populate as data accumulates."
                )

            # Calculate Delta
            df["forecast_actual_delta"] = df["actual_gco2_kwh"] - df["forecast_gco2_kwh"]

            col1, col2, col3, col4 = st.columns(4)

            latest = df.iloc[-1] if len(df) else None
            if latest is not None:
                col1.metric("Latest Forecast (gCO₂/kWh)", int(latest["forecast_gco2_kwh"]) if pd.notnull(latest["forecast_gco2_kwh"]) else None)
                col2.metric("Latest Actual (gCO₂/kWh)", int(latest["actual_gco2_kwh"]) if pd.notnull(latest["actual_gco2_kwh"]) else None)
                
                delta_val = latest["forecast_actual_delta"]
                col3.metric(
                    "Forecast vs Actual Δ",
                    f"{delta_val:+.1f}" if pd.notnull(delta_val) else "N/A"
                )
                
                col4.metric("Index", latest["index"])

            st.subheader("Last 24 hours")
            cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=24)
            last24 = df[df["from_ts"] >= cutoff]

            if len(last24) < 2:
                st.warning("Not enough data points yet for trend analysis.")
            else:
                st.line_chart(
                    last24.set_index("from_ts")[["forecast_gco2_kwh", "actual_gco2_kwh"]]
                )

            st.subheader("Index Distribution (last 24 hours)")
            if len(last24):
                st.bar_chart(last24["index"].value_counts())

            st.divider()

            st.caption(
                f"**Data Source:** {DATA_SOURCE_NAME}  |  "
                f"**Endpoint:** {DATA_SOURCE_ENDPOINT}"
            )
            
            st.caption(f"**Stream source tag:** {source_value}")

            st.caption(
                f"**Stream:** {STREAM_DESCRIPTION}"
            )

            st.caption(
                f"**Streaming window coverage (Silver):** {coverage_text}  |  "
                f"**Latest fetched_at:** {latest_fetch_text}  |  "
                f"**Producer poll:** {PRODUCER_POLL_SECONDS}s  |  "
                f"**Dashboard refresh:** {refresh_seconds}s"
            )

    except Exception as e:
        st.error(f"Dashboard error: {e}")

    time.sleep(refresh_seconds)
