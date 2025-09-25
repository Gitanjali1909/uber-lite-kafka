import streamlit as st
from kfk_consumer import consume_topic
from utils import prepare_dataframe

def show():
    st.title("Alerts - Surge & Hot Zones")

    rides = consume_topic("rides", max_messages=100)
    df = prepare_dataframe(rides)

    if df.empty:
        st.info("No data yet.")
        return

    avg_eta = df["eta_min"].mean()
    if avg_eta > 10:
        st.error(f"⚠️ Surge Alert: Average ETA is high ({avg_eta:.1f} min)")

    top_zone = df.groupby("lat")["cab_id"].count().idxmax()
    st.warning(f"🔥 Hot Zone detected near latitude {top_zone:.2f}")
