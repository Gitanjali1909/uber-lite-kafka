import pandas as pd
import pydeck as pdk
import streamlit as st
import numpy as np

def create_rides_dataframe(data):
    if not data:
        return pd.DataFrame(columns=['cab_id', 'latitude', 'longitude', 'timestamp'])
    df = pd.DataFrame(data)
    for col in ['cab_id', 'latitude', 'longitude', 'timestamp']:
        if col not in df.columns:
            df[col] = None
    df['zone'] = df.apply(lambda x: f"{round(x['latitude'],2)}-{round(x['longitude'],2)}" if x['latitude'] and x['longitude'] else None, axis=1)
    return df

def create_eta_dataframe(data):
    if not data:
        return pd.DataFrame(columns=['cab_id', 'eta_min', 'timestamp'])
    df = pd.DataFrame(data)
    for col in ['cab_id', 'eta_min', 'timestamp']:
        if col not in df.columns:
            df[col] = None
    df['zone'] = df.apply(lambda x: f"{round(x.get('latitude',0),2)}-{round(x.get('longitude',0),2)}" if 'latitude' in x and 'longitude' in x else None, axis=1)
    return df

def render_heatmap(df):
    if df.empty or 'latitude' not in df.columns or 'longitude' not in df.columns:
        st.write("No cab data available for heatmap.")
        return
    df_filtered = df.dropna(subset=['latitude', 'longitude'])
    if df_filtered.empty:
        st.write("No valid cab locations for heatmap.")
        return
    st.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v9",
        initial_view_state=pdk.ViewState(
            latitude=df_filtered['latitude'].mean(),
            longitude=df_filtered['longitude'].mean(),
            zoom=11,
            pitch=50,
        ),
        layers=[
            pdk.Layer(
                "HeatmapLayer",
                data=df_filtered,
                get_position=["longitude", "latitude"],
                threshold=0.1,
                get_weight=1,
                radius_pixels=30,
                intensity=1,
            ),
        ],
    ))

def calculate_kpis(rides_df, eta_df):
    active_cabs = rides_df['cab_id'].nunique() if not rides_df.empty else 0
    avg_eta = eta_df['eta_min'].mean() if not eta_df.empty else 0.0
    total_trips = len(rides_df)
    hot_zone = "N/A"
    if not rides_df.empty:
        cab_counts = rides_df.groupby('zone')['cab_id'].nunique().reset_index(name='count')
        if not cab_counts.empty:
            hot_zone = cab_counts.loc[cab_counts['count'].idxmax()]['zone']
    surge_alerts = eta_df[eta_df['eta_min'] > 10].shape[0] if not eta_df.empty else 0
    return {
        "Active Cabs": active_cabs,
        "Avg ETA": f"{avg_eta:.2f} min",
        "Total Trips": total_trips,
        "Hot Zone": hot_zone,
        "Surge Alerts": surge_alerts,
    }

def plot_trips_per_zone(rides_df):
    if rides_df.empty:
        st.write("No ride data available for trips per zone chart.")
        return
    df_filtered = rides_df.dropna(subset=['zone'])
    if df_filtered.empty:
        st.write("No valid zone data for trips per zone chart.")
        return
    trips_per_zone = df_filtered.groupby('zone').size().reset_index(name='trips')
    st.bar_chart(trips_per_zone.set_index('zone'))

def plot_eta_trends(eta_df):
    if eta_df.empty:
        st.write("No ETA data available for ETA trends chart.")
        return
    df_filtered = eta_df.dropna(subset=['timestamp', 'eta_min'])
    if df_filtered.empty:
        st.write("No valid ETA data for ETA trends chart.")
        return
    df_filtered['timestamp'] = pd.to_datetime(df_filtered['timestamp'])
    eta_trends = df_filtered.set_index('timestamp').resample('1min')['eta_min'].mean().reset_index()
    st.line_chart(eta_trends.set_index('timestamp'))

def plot_top_active_cabs(rides_df):
    if rides_df.empty:
        st.write("No ride data available for top active cabs chart.")
        return
    df_filtered = rides_df.dropna(subset=['cab_id'])
    if df_filtered.empty:
        st.write("No valid cab ID data for top active cabs chart.")
        return
    top_cabs = df_filtered['cab_id'].value_counts().head(10).reset_index(name='activity')
    top_cabs.columns = ['cab_id', 'activity']
    st.bar_chart(top_cabs.set_index('cab_id'))

def get_nearby_cabs_table(rides_df, num_cabs=5):
    if rides_df.empty:
        return pd.DataFrame(columns=['Cab ID', 'Latitude', 'Longitude'])
    df_filtered = rides_df.dropna(subset=['cab_id', 'latitude', 'longitude'])
    if df_filtered.empty:
        return pd.DataFrame(columns=['Cab ID', 'Latitude', 'Longitude'])
    latest_cabs = df_filtered.drop_duplicates(subset=['cab_id'], keep='last')
    return latest_cabs[['cab_id', 'latitude', 'longitude']].head(num_cabs).rename(columns={
        'cab_id': 'Cab ID',
        'latitude': 'Latitude',
        'longitude': 'Longitude'
    })

def get_surge_alerts_table(eta_df, surge_threshold=10):
    if eta_df.empty:
        return pd.DataFrame(columns=['Cab ID', 'ETA (min)', 'Zone'])
    df_filtered = eta_df.dropna(subset=['cab_id', 'eta_min', 'zone'])
    if df_filtered.empty:
        return pd.DataFrame(columns=['Cab ID', 'ETA (min)', 'Zone'])
    surge_cabs = df_filtered[df_filtered['eta_min'] > surge_threshold]
    return surge_cabs[['cab_id', 'eta_min', 'zone']].rename(columns={
        'cab_id': 'Cab ID',
        'eta_min': 'ETA (min)',
        'zone': 'Zone'
    })

def highlight_hot_zones(rides_df, num_zones=3):
    if rides_df.empty:
        return "N/A"
    df_filtered = rides_df.dropna(subset=['zone'])
    if df_filtered.empty:
        return "N/A"
    zone_counts = df_filtered['zone'].value_counts()
    if zone_counts.empty:
        return "N/A"
    hot_zones = zone_counts.head(num_zones).index.tolist()
    return ", ".join(hot_zones)
