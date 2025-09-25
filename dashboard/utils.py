import pandas as pd
import pydeck as pdk

def prepare_dataframe(rides):
    if not rides:
        return pd.DataFrame([{"cab_id": "none", "lat": 37.7749, "lon": -122.4194, "eta_min": 0}])
    df = pd.DataFrame(rides)
    df = df[["cab_id", "lat", "lon", "eta_min"]].dropna()
    return df

def create_heatmap(df):
    if df.empty:
        return pdk.Deck()
    view_state = pdk.ViewState(
        latitude=df["lat"].mean(),
        longitude=df["lon"].mean(),
        zoom=12,
        pitch=0
    )
    layer = pdk.Layer(
        "HeatmapLayer",
        data=df,
        get_position="[lon, lat]",
        radiusPixels=60,
    )
    return pdk.Deck(layers=[layer], initial_view_state=view_state)
