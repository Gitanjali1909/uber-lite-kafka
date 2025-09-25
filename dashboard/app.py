import importlib.util
import sys
from pathlib import Path
import streamlit as st

def import_module_from_path(path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[path.stem] = mod
    spec.loader.exec_module(mod)
    return mod

pages_path = Path(__file__).parent / "_pages"
overview = import_module_from_path(pages_path / "overview.py")
analytics = import_module_from_path(pages_path / "analytics.py")
alerts = import_module_from_path(pages_path / "alerts.py")

PAGES = {
    "Overview": overview,
    "Analytics": analytics,
    "Alerts": alerts
}

st.set_page_config(layout="wide")

st.markdown("""
<style>
    .stRadio > label { font-size: 1.2em; font-weight: bold; padding: 0.5em 1em; border-radius: 0.5em; margin-right: 1em; cursor: pointer; transition: all 0.2s ease-in-out; }
    .stRadio > label:hover { background-color: #f0f2f6; }
    .stRadio > label[data-baseweb="radio"] > div:first-child { display: none; }
    .stRadio > label[data-baseweb="radio"] { border: 1px solid #e0e0e0; }
    .stRadio > label[data-baseweb="radio"][aria-checked="true"] { background-color: #007bff; color: white; border-color: #007bff; }
</style>
""", unsafe_allow_html=True)

st.write("## Uber-Lite Dashboard")
selection = st.radio("Go to", list(PAGES.keys()), horizontal=True)
page = PAGES[selection]
page.show()
