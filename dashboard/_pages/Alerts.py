import streamlit as st
import pandas as pd
from kfk_consumer import KafkaDataConsumer
from util import create_rides_dataframe, create_eta_dataframe, get_surge_alerts_table, highlight_hot_zones
import time

# Initialize Kafka consumer (singleton pattern)
@st.cache_resource
def get_kafka_consumer():
    consumer = KafkaDataConsumer()
    consumer.start_consuming()
    return consumer

kafka_consumer = get_kafka_consumer()

def show():
    st.title("Alerts Dashboard")

    # Create placeholders for real-time updates
    surge_kpi_placeholder = st.empty()
    hot_zones_placeholder = st.empty()
    surge_table_placeholder = st.empty()

    while True:
        rides_data = kafka_consumer.get_latest_data('rides')
        eta_data = kafka_consumer.get_latest_data('eta')

        rides_df = create_rides_dataframe(rides_data)
        eta_df = create_eta_dataframe(eta_data)

        with surge_kpi_placeholder.container():
            st.subheader("Surge Alerts Metrics")
            # Assuming a simple surge alert count from utils.calculate_kpis
            # For a dedicated metric, we can refine this.
            surge_count = eta_df[eta_df['eta_min'] > 10].shape[0] if not eta_df.empty else 0
            st.metric(label="Cabs with ETA > 10 min", value=surge_count)

        with hot_zones_placeholder.container():
            st.subheader("Hot Zones (High Cab Density)")
            hot_zones_str = highlight_hot_zones(rides_df)
            st.write(f"**Current Hot Zones:** {hot_zones_str}")

        with surge_table_placeholder.container():
            st.subheader("Cabs with High ETA (Surge Threshold)")
            st.dataframe(get_surge_alerts_table(eta_df))

        time.sleep(1) # Refresh every second
