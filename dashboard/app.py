import streamlit as st
import importlib.util
import os
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Uber-Lite Dashboard", layout="wide")

# Auto-refresh every 5 sec
st_autorefresh(interval=5000, key="refresh")

# Dynamically load pages
def load_page(path):
    spec = importlib.util.spec_from_file_location("page", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

# Page registry
base_path = os.path.join(os.path.dirname(__file__), "pages")
pages = {
    "Overview": os.path.join(base_path, "overview.py"),
    "Analytics": os.path.join(base_path, "analytics.py"),
    "Alerts": os.path.join(base_path, "alerts.py")
}

# Horizontal nav → no sidebar
page_name = st.radio("Navigate", list(pages.keys()), horizontal=True)
page_module = load_page(pages[page_name])
page_module.show()
