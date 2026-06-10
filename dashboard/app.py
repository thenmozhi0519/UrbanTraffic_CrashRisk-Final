import streamlit as st
import pandas as pd
import requests
import json
from collections import Counter

# ================= PAGE CONFIG =================
st.set_page_config(
    page_title="Chicago Crash Analytics",
    page_icon="🚦",
    layout="wide"
)

# ================= CUSTOM STYLE =================
st.markdown("""
<style>
    [data-testid="stAppViewContainer"] { background-color: #f8fafc; }
    [data-testid="stSidebar"] { background-color: #ffffff; border-right: 1px solid #e2e8f0; }
    .section-title {
        font-size: 1rem;
        font-weight: 700;
        color: #dc2626;
        text-transform: uppercase;
        letter-spacing: 2px;
        border-left: 4px solid #dc2626;
        padding-left: 10px;
        margin: 1.5rem 0 1rem 0;
    }
    .stDataFrame { border-radius: 10px; }
    div[data-testid="stMetricValue"] { color: #dc2626 !important; }
    div[data-testid="stMetricLabel"] { color: #64748b !important; }
    h1, h2, h3 { color: #0f172a !important; }
</style>
""", unsafe_allow_html=True)

# ================= HEADER =================
st.markdown("## 🚦 Chicago Crash Analytics Dashboard")
st.markdown("<span style='color:#64748b;font-size:0.9rem'>Urban Traffic Risk & Public Sentiment · Live Data from Chicago Open Data API</span>", unsafe_allow_html=True)
st.markdown("---")

# ================= FETCH DATA =================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_chicago_data(limit=5000):
    url = f"https://data.cityofchicago.org/resource/85ca-t3if.json?$limit={limit}&$order=crash_date DESC"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())
        return df, None
    except Exception as e:
        return None, str(e)

with st.spinner("⏳ Fetching live crash data from Chicago Open Data API..."):
    df_raw, err = fetch_chicago_data()

if err or df_raw is None:
    st.error(f"❌ Could not fetch data: {err}")
    st.stop()

# ================= CLEAN DATA =================
df = df_raw.copy()

# Fix types
for col_name in ["latitude", "longitude"]:
    if col_name in df.columns:
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce")

if "crash_date" in df.columns:
    df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce")
    df["year"]  = df["crash_date"].dt.year
    df["month"] = df["crash_date"].dt.month
    df["hour"]  = df["crash_date"].dt.hour

df = df.dropna(subset=["crash_date"])

# ================= SIDEBAR FILTERS =================
st.sidebar.markdown("### 🔧 Filters")

years = sorted(df["year"].dropna().unique(), reverse=True)
selected_year = st.sidebar.selectbox("Year", years)

months_map = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
              7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
months_available = sorted(df[df["year"] == selected_year]["month"].dropna().unique())
selected_month = st.sidebar.selectbox("Month", months_available, format_func=lambda x: months_map.get(int(x), x))

df_filtered = df[(df["year"] == selected_year) & (df["month"] == selected_month)]

st.sidebar.markdown("---")
st.sidebar.markdown(f"<span style='color:#64748b;font-size:0.8rem'>📦 {len(df_filtered):,} records loaded for {months_map.get(int(selected_month), selected_month)} {int(selected_year)}</span>", unsafe_allow_html=True)
st.sidebar.markdown(f"<span style='color:#64748b;font-size:0.8rem'>🗃️ Total in cache: {len(df):,} records</span>", unsafe_allow_html=True)

# ================= KPIs =================
st.markdown('<div class="section-title">Key Metrics</div>', unsafe_allow_html=True)

total_crashes   = len(df_filtered)
has_injury      = df_filtered.get("injuries_total", pd.Series(dtype=float))
total_injuries  = pd.to_numeric(has_injury, errors="coerce").sum() if "injuries_total" in df_filtered.columns else 0
fatal_col       = df_filtered.get("injuries_fatal", pd.Series(dtype=float))
total_fatalities = pd.to_numeric(fatal_col, errors="coerce").sum() if "injuries_fatal" in df_filtered.columns else 0
map_ready       = df_filtered.dropna(subset=["latitude","longitude"])
hotspot_count   = len(map_ready)

k1, k2, k3, k4 = st.columns(4)
k1.metric("🚗 Total Crashes",     f"{total_crashes:,}")
k2.metric("🏥 Total Injuries",    f"{int(total_injuries):,}")
k3.metric("💀 Fatalities",        f"{int(total_fatalities):,}")
k4.metric("📍 Mappable Locations",f"{hotspot_count:,}")

st.markdown("---")

# ================= ROW 2: CHARTS =================
col_left, col_right = st.columns(2)

# --- Weather Breakdown ---
with col_left:
    st.markdown('<div class="section-title">Crashes by Weather Condition</div>', unsafe_allow_html=True)
    if "weather_condition" in df_filtered.columns:
        weather_counts = (
            df_filtered["weather_condition"]
            .dropna()
            .value_counts()
            .head(8)
            .reset_index()
        )
        weather_counts.columns = ["Weather Condition", "Crashes"]
        st.bar_chart(weather_counts.set_index("Weather Condition"), color="#f97316")
    else:
        st.info("Weather data not available")

# --- Crash Type Breakdown ---
with col_right:
    st.markdown('<div class="section-title">Crashes by Type</div>', unsafe_allow_html=True)
    if "first_crash_type" in df_filtered.columns:
        type_counts = (
            df_filtered["first_crash_type"]
            .dropna()
            .value_counts()
            .head(8)
            .reset_index()
        )
        type_counts.columns = ["Crash Type", "Count"]
        st.bar_chart(type_counts.set_index("Crash Type"), color="#38bdf8")
    else:
        st.info("Crash type data not available")

st.markdown("---")

# ================= ROW 3: HOURLY + ROAD SURFACE =================
col_a, col_b = st.columns(2)

with col_a:
    st.markdown('<div class="section-title">Crashes by Hour of Day</div>', unsafe_allow_html=True)
    if "hour" in df_filtered.columns:
        hourly = df_filtered["hour"].value_counts().sort_index().reset_index()
        hourly.columns = ["Hour", "Crashes"]
        st.line_chart(hourly.set_index("Hour"), color="#a78bfa")
    else:
        st.info("Hour data not available")

with col_b:
    st.markdown('<div class="section-title">Road Surface Conditions</div>', unsafe_allow_html=True)
    if "roadway_surface_cond" in df_filtered.columns:
        road_counts = (
            df_filtered["roadway_surface_cond"]
            .dropna()
            .value_counts()
            .head(6)
            .reset_index()
        )
        road_counts.columns = ["Surface Condition", "Count"]
        st.bar_chart(road_counts.set_index("Surface Condition"), color="#34d399")
    else:
        st.info("Road surface data not available")

st.markdown("---")

# ================= TOP CRASH HOTSPOTS TABLE =================
st.markdown('<div class="section-title">Top 10 Crash Hotspot Locations</div>', unsafe_allow_html=True)

if not map_ready.empty:
    hotspots = (
        map_ready
        .groupby(["latitude", "longitude"])
        .size()
        .reset_index(name="accident_count")
        .sort_values("accident_count", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )
    hotspots.index += 1

    # Simple risk score: normalize accident_count to 0–100
    max_count = hotspots["accident_count"].max()
    hotspots["risk_score"] = (hotspots["accident_count"] / max_count * 100).round(1)
    hotspots["risk_level"] = hotspots["risk_score"].apply(
        lambda x: "🔴 High" if x >= 70 else ("🟡 Medium" if x >= 40 else "🟢 Low")
    )

    st.dataframe(
        hotspots[["latitude","longitude","accident_count","risk_score","risk_level"]],
        use_container_width=True
    )
else:
    st.warning("No coordinate data available for this period")

st.markdown("---")

# ================= MAP =================
st.markdown('<div class="section-title">Accident Map — Chicago</div>', unsafe_allow_html=True)

if not map_ready.empty:
    map_df = map_ready[["latitude","longitude"]].copy()
    # Filter to valid Chicago coords
    map_df = map_df[
        (map_df["latitude"].between(41.6, 42.1)) &
        (map_df["longitude"].between(-87.9, -87.5))
    ]
    if not map_df.empty:
        st.map(map_df, zoom=10)
    else:
        st.warning("No valid Chicago coordinates found for this period")
else:
    st.warning("Location data not available")

# ================= LIGHTING CONDITIONS =================
st.markdown("---")
st.markdown('<div class="section-title">Lighting Conditions at Time of Crash</div>', unsafe_allow_html=True)

if "lighting_condition" in df_filtered.columns:
    lighting = (
        df_filtered["lighting_condition"]
        .dropna()
        .value_counts()
        .reset_index()
    )
    lighting.columns = ["Lighting Condition", "Count"]
    st.bar_chart(lighting.set_index("Lighting Condition"), color="#fb7185")
else:
    st.info("Lighting data not available")

# ================= FOOTER =================
st.markdown("---")
st.markdown(
    "<div style='text-align:center;color:#94a3b8;font-size:0.8rem'>"
    "Data sourced live from <b>Chicago Open Data API</b> · "
    "Built for Urban Traffic Risk & Public Sentiment Data Engineering Project"
    "</div>",
    unsafe_allow_html=True
)