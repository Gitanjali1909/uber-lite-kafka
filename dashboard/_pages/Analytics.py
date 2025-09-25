import streamlit as st
import pandas as pd
from kfk_consumer import KafkaDataConsumer
from util import create_rides_dataframe, create_eta_dataframe, plot_trips_per_zone, plot_eta_trends, plot_top_active_cabs
import time

# Initialize Kafka consumer (singleton pattern)
@st.cache_resource
def get_kafka_consumer():
    consumer = KafkaDataConsumer()
    consumer.start_consuming()
    return consumer

kafka_consumer = get_kafka_consumer()

def show():
    st.title("Analytics Dashboard")

    # Create placeholders for real-time updates
    trips_chart_placeholder = st.empty()
    eta_trends_chart_placeholder = st.empty()
    top_cabs_chart_placeholder = st.empty()

    while True:
        rides_data = kafka_consumer.get_latest_data('rides')
        eta_data = kafka_consumer.get_latest_data('eta')

        rides_df = create_rides_dataframe(rides_data)
        eta_df = create_eta_dataframe(eta_data)

        with trips_chart_placeholder.container():
            st.subheader("Trips per Zone")
            plot_trips_per_zone(rides_df)

        with eta_trends_chart_placeholder.container():
            st.subheader("ETA Trends")
            plot_eta_trends(eta_df)

        with top_cabs_chart_placeholder.container():
            st.subheader("Top Active Cabs")
            plot_top_active_cabs(rides_df)

        time.sleep(1) # Refresh every second
