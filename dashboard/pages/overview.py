import streamlit as st
from kfk_consumer import consume_topic
from utils import prepare_dataframe, create_heatmap

def show():
    st.title("Overview - Live Fleet Map")

    rides = consume_topic("rides", max_messages=50)
    df = prepare_dataframe(rides)

    col1, col2, col3 = st.columns([1,3,1])
    with col1:
        st.metric("Active Cabs", df["cab_id"].nunique())
    with col2:
        st.markdown("### Real-time Fleet Heatmap")
    with col3:
        st.metric("Total Trips", len(df))

    st.pydeck_chart(create_heatmap(df))
    st.subheader("Sample Cabs")
    st.dataframe(df.head(10), use_container_width=True)
