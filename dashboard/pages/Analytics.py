import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from kfk_consumer import consume_topic
from utils import prepare_dataframe

def show():
    st.title("Analytics - Fleet Trends")

    rides = consume_topic("rides", max_messages=200)
    df = prepare_dataframe(rides)

    if df.empty:
        st.info("No ride data available yet.")
        return

    st.subheader("Trips per Zone (lat buckets)")
    df["zone"] = pd.cut(df["lat"], bins=5)
    trips_per_zone = df["zone"].value_counts()

    fig, ax = plt.subplots()
    trips_per_zone.plot(kind="bar", ax=ax)
    ax.set_ylabel("Trips")
    st.pyplot(fig)

    st.subheader("ETA Distribution")
    fig2, ax2 = plt.subplots()
    df["eta_min"].plot(kind="hist", bins=10, ax=ax2)
    ax2.set_xlabel("ETA (minutes)")
    st.pyplot(fig2)
