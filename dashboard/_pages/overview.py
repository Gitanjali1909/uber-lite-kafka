# overview.py
import streamlit as st
import pandas as pd
from kfk_consumer import KafkaDataConsumer
from util import create_rides_dataframe, create_eta_dataframe, render_heatmap, calculate_kpis, get_nearby_cabs_table
import time

@st.cache_resource
def get_kafka_consumer():
    consumer = KafkaDataConsumer()
    consumer.start_consuming()
    return consumer

kafka_consumer = get_kafka_consumer()

def show():  
    st.title("Overview Dashboard")

    # Create placeholders for real-time updates
    kpi_placeholder = st.empty()
    heatmap_placeholder = st.empty()
    table_placeholder = st.empty()

    while True:
        rides_data = kafka_consumer.get_latest_data('rides')
        eta_data = kafka_consumer.get_latest_data('eta')

        rides_df = create_rides_dataframe(rides_data)
        eta_df = create_eta_dataframe(eta_data)

        with kpi_placeholder.container():
            st.subheader("Key Performance Indicators")
            kpis = calculate_kpis(rides_df, eta_df)
            cols = st.columns(len(kpis))
            for i, (kpi_name, kpi_value) in enumerate(kpis.items()):
                with cols[i]:
                    st.metric(label=kpi_name, value=kpi_value)

        with heatmap_placeholder.container():
            st.subheader("Real-time Cab Locations Heatmap")
            render_heatmap(rides_df)

        with table_placeholder.container():
            st.subheader("Nearby Cabs")
            st.dataframe(get_nearby_cabs_table(rides_df))

        time.sleep(1)  # Refresh every second
