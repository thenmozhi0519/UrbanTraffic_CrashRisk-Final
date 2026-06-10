import streamlit as st
import pandas as pd
import glob
import os

# streamlit run pipeline_dashboard.py

# ================= PAGE CONFIG =================
st.set_page_config(
    page_title="Chicago Pipeline Dashboard",
    page_icon="🏗️",
    layout="wide"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');

    * { font-family: 'IBM Plex Sans', sans-serif; }
    code, .mono { font-family: 'IBM Plex Mono', monospace !important; }

    [data-testid="stAppViewContainer"] { background-color: #eef2f7; }
    [data-testid="stSidebar"] {
        background-color: #ffffff;
        border-right: 1px solid #cbd5e1;
    }
    [data-testid="stSidebar"] * { color: #334155 !important; }
    [data-testid="stSidebar"] .stSelectbox label { color: #64748b !important; font-size: 0.75rem !important; text-transform: uppercase; letter-spacing: 1px; }

    h1, h2, h3 { color: #1e293b !important; }
    p, li { color: #475569; }

    .section-title {
        font-size: 0.7rem;
        font-weight: 700;
        color: #1d4ed8;
        text-transform: uppercase;
        letter-spacing: 3px;
        border-left: 3px solid #1d4ed8;
        padding-left: 10px;
        margin: 2rem 0 1rem 0;
    }

    .layer-box {
        border-radius: 8px;
        padding: 1.1rem 1.3rem;
        margin-bottom: 0.5rem;
        font-size: 0.82rem;
        font-family: 'IBM Plex Mono', monospace;
    }
    .bronze-box { background: #fff7ed; border: 1px solid #f97316; color: #7c2d12; }
    .silver-box { background: #f0f7ff; border: 1px solid #3b82f6; color: #1e3a5f; }
    .gold-box   { background: #fefce8; border: 1px solid #eab308; color: #713f12; }
    .layer-title { font-size: 0.9rem; font-weight: 700; margin-bottom: 6px; letter-spacing: 1px; }

    div[data-testid="stMetricValue"] { color: #1d4ed8 !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 1.6rem !important; }
    div[data-testid="stMetricLabel"] { color: #64748b !important; font-size: 0.72rem !important; text-transform: uppercase; letter-spacing: 1px; }
    div[data-testid="stMetricDelta"] { color: #10b981 !important; }

    [data-testid="stDataFrame"] { border: 1px solid #e2e8f0 !important; border-radius: 8px; }

    .stSelectbox > div > div {
        background-color: #ffffff !important;
        border: 1px solid #cbd5e1 !important;
        color: #334155 !important;
        border-radius: 6px;
    }

    hr { border-color: #e2e8f0 !important; }

    .status-ok  { color: #16a34a; font-weight: 700; }
    .status-err { color: #dc2626; font-weight: 700; }

    .header-band {
        background: linear-gradient(135deg, #1e3a5f 0%, #1e40af 100%);
        border: none;
        border-radius: 10px;
        padding: 1.5rem 2rem;
        margin-bottom: 1.5rem;
    }
    .header-title { font-size: 1.6rem; font-weight: 700; color: #ffffff; margin: 0; }
    .header-sub   { font-size: 0.8rem; color: #93c5fd; margin-top: 4px; font-family: 'IBM Plex Mono', monospace; }
</style>
""", unsafe_allow_html=True)

# ================= PATHS =================
BASE = r"C:\Users\LENOVO\OneDrive\Desktop\pipline"

SILVER_CHICAGO = os.path.join(BASE, "data_lake", "silver", "traffic_crashes")
SILVER_YOUTUBE = os.path.join(BASE, "data_lake", "silver", "youtube")
GOLD_HOTSPOTS  = os.path.join(BASE, "data_lake", "gold", "crash_hotspots")
GOLD_RISK      = os.path.join(BASE, "data_lake", "gold", "road_risk_index")
GOLD_MONTHLY   = os.path.join(BASE, "data_lake", "gold", "youtube_chicago_monthly")


# ================= KEY FIX: USE pd.read_parquet(path) =================
# OLD (BROKEN): glob individual .parquet files → loses year=/month= folder names
# NEW (FIXED):  pd.read_parquet(folder) → Pandas auto-reads partitions + restores year/month columns

def read_parquet_folder(path):
    """
    Read a Hive-partitioned parquet folder using pd.read_parquet().
    This preserves partition columns like year= and month= as proper DataFrame columns.
    Falls back to glob if the folder structure isn't recognised.
    """
    if not os.path.exists(path):
        return None, f"Path does not exist: {path}"
    try:
        df = pd.read_parquet(path)
        if df.empty:
            return None, "Dataframe is empty"
        return df, None
    except Exception as e:
        # Fallback: manual glob (loses partition cols, but better than crashing)
        files = glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True)
        files = [f for f in files if not os.path.basename(f).startswith(".")]
        if not files:
            return None, f"No parquet files found in {path}"
        dfs = []
        for f in files:
            try:
                dfs.append(pd.read_parquet(f))
            except Exception:
                continue
        if not dfs:
            return None, f"Could not read any parquet files: {e}"
        return pd.concat(dfs, ignore_index=True), None


# ================= LOAD ALL LAYERS =================
@st.cache_data(show_spinner=False)
def load_all():
    silver_chicago, e1 = read_parquet_folder(SILVER_CHICAGO)
    silver_youtube,  e2 = read_parquet_folder(SILVER_YOUTUBE)
    gold_hotspots,   e3 = read_parquet_folder(GOLD_HOTSPOTS)
    gold_risk,       e4 = read_parquet_folder(GOLD_RISK)
    gold_monthly,    e5 = read_parquet_folder(GOLD_MONTHLY)
    return (silver_chicago, e1), (silver_youtube, e2), \
           (gold_hotspots, e3), (gold_risk, e4), (gold_monthly, e5)

with st.spinner("Loading Bronze → Silver → Gold layers..."):
    (df_silver_c, e1), (df_silver_y, e2), \
    (df_gold_h, e3), (df_gold_r, e4), (df_gold_m, e5) = load_all()


# ================= HEADER =================
st.markdown("""
<div class="header-band">
  <div class="header-title">🏗️ Urban Traffic Risk — Pipeline Dashboard</div>
  <div class="header-sub">Bronze (JSON) → Silver (Parquet) → Gold (Aggregated) &nbsp;·&nbsp; Chicago Crash Data + YouTube Sentiment</div>
</div>
""", unsafe_allow_html=True)


# ================= PIPELINE STATUS BAR =================
st.markdown('<div class="section-title">Pipeline Layer Status</div>', unsafe_allow_html=True)
b_col, s_col, g_col = st.columns(3)

with b_col:
    bronze_path  = os.path.join(BASE, "data_lake", "bronze")
    bronze_files = glob.glob(os.path.join(bronze_path, "**", "*.json"), recursive=True)
    b_count = f"{len(bronze_files):,} JSON files" if bronze_files else "Files collected"
    st.markdown(f"""
    <div class="layer-box bronze-box">
        <div class="layer-title">🟤 BRONZE LAYER</div>
        chicago.py · youtube.py<br>
        Path: data_lake/bronze/<br><br>
        <span class="status-ok">✓</span> {b_count}<br>
        <span style='font-size:0.72rem;color:#78350f'>Raw JSON · Incremental · Date-partitioned</span>
    </div>""", unsafe_allow_html=True)

with s_col:
    c_cnt = f"{len(df_silver_c):,}" if df_silver_c is not None else "<span class='status-err'>❌ Missing</span>"
    y_cnt = f"{len(df_silver_y):,}" if df_silver_y is not None else "<span class='status-err'>❌ Missing</span>"
    st.markdown(f"""
    <div class="layer-box silver-box">
        <div class="layer-title">⚪ SILVER LAYER</div>
        silver_chicago.py · silver_youtube.py<br>
        Path: data_lake/silver/<br><br>
        <span class="status-ok">✓</span> Chicago: <b>{c_cnt}</b> records<br>
        <span class="status-ok">✓</span> YouTube: <b>{y_cnt}</b> records<br>
        <span style='font-size:0.72rem;color:#1e3a5f'>Snappy Parquet · Deduped · year/month partitions</span>
    </div>""", unsafe_allow_html=True)

with g_col:
    h_cnt = f"{len(df_gold_h):,}" if df_gold_h is not None else "<span class='status-err'>❌</span>"
    r_cnt = f"{len(df_gold_r):,}" if df_gold_r is not None else "<span class='status-err'>❌</span>"
    m_cnt = f"{len(df_gold_m):,}" if df_gold_m is not None else "<span class='status-err'>❌</span>"
    st.markdown(f"""
    <div class="layer-box gold-box">
        <div class="layer-title">🥇 GOLD LAYER</div>
        gold_crash_hotspots · road_risk_index · gold_combined<br>
        Path: data_lake/gold/<br><br>
        <span class="status-ok">✓</span> Hotspots: <b>{h_cnt}</b> rows<br>
        <span class="status-ok">✓</span> Risk Index: <b>{r_cnt}</b> rows<br>
        <span class="status-ok">✓</span> Monthly: <b>{m_cnt}</b> rows
    </div>""", unsafe_allow_html=True)

st.markdown("---")

if df_silver_c is None:
    st.error(f"❌ Silver Chicago layer missing: {e1}")
    st.stop()


# ================= FIX TYPES =================
for col in ["latitude", "longitude"]:
    if col in df_silver_c.columns:
        df_silver_c[col] = pd.to_numeric(df_silver_c[col], errors="coerce")

if "crash_date" in df_silver_c.columns:
    df_silver_c["crash_date"] = pd.to_datetime(df_silver_c["crash_date"], errors="coerce")

# year/month will already exist as columns when pd.read_parquet reads partitioned folder
# but we derive them as fallback just in case
if "year" not in df_silver_c.columns:
    df_silver_c["year"] = df_silver_c["crash_date"].dt.year
if "month" not in df_silver_c.columns:
    df_silver_c["month"] = df_silver_c["crash_date"].dt.month

df_silver_c["year"]  = pd.to_numeric(df_silver_c["year"],  errors="coerce")
df_silver_c["month"] = pd.to_numeric(df_silver_c["month"], errors="coerce")
df_silver_c = df_silver_c.dropna(subset=["year", "month"])
df_silver_c["year"]  = df_silver_c["year"].astype(int)
df_silver_c["month"] = df_silver_c["month"].astype(int)


# ================= SIDEBAR =================
months_map = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
              7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}

st.sidebar.markdown("## 🔧 Filters")

years = sorted(df_silver_c["year"].unique(), reverse=True)
selected_year = st.sidebar.selectbox("Year", years)

months_avail = sorted(df_silver_c[df_silver_c["year"] == selected_year]["month"].unique())
selected_month = st.sidebar.selectbox(
    "Month", months_avail,
    format_func=lambda x: months_map.get(x, x)
)

df_filtered = df_silver_c[
    (df_silver_c["year"]  == selected_year) &
    (df_silver_c["month"] == selected_month)
].copy()

st.sidebar.markdown("---")
bronze_count = len(bronze_files) if bronze_files else 0
st.sidebar.markdown(f"🟤 Bronze JSONs: **{bronze_count:,}**")
st.sidebar.markdown(f"⚪ Silver Chicago: **{len(df_silver_c):,}**")
if df_silver_y is not None:
    st.sidebar.markdown(f"⚪ Silver YouTube: **{len(df_silver_y):,}**")
st.sidebar.markdown(f"📦 Filtered: **{len(df_filtered):,}** ({months_map.get(selected_month)} {selected_year})")

# ─── Debug expander (remove after confirming years look correct) ───────────────
with st.sidebar.expander("🛠 Debug — year counts"):
    yc = df_silver_c["year"].value_counts().sort_index()
    st.dataframe(pd.DataFrame({"year": yc.index, "records": yc.values}))


# =====================================================
# SILVER — CRASH ANALYSIS
# =====================================================
st.markdown('<div class="section-title">⚪ Silver Layer — Crash Analysis</div>', unsafe_allow_html=True)

k1, k2, k3, k4 = st.columns(4)
map_ready   = df_filtered.dropna(subset=["latitude","longitude"]) if "latitude" in df_filtered.columns else pd.DataFrame()
injuries    = int(pd.to_numeric(df_filtered.get("injuries_total",  pd.Series(dtype=float)), errors="coerce").sum())
fatalities  = int(pd.to_numeric(df_filtered.get("injuries_fatal",  pd.Series(dtype=float)), errors="coerce").sum())

k1.metric("🚗 Total Crashes",  f"{len(df_filtered):,}")
k2.metric("🏥 Total Injuries", f"{injuries:,}")
k3.metric("💀 Fatalities",     f"{fatalities:,}")
k4.metric("📍 Mappable",       f"{len(map_ready):,}")

col_l, col_r = st.columns(2)

with col_l:
    st.markdown('<div class="section-title">Crashes by Weather</div>', unsafe_allow_html=True)
    if "weather_condition" in df_filtered.columns:
        wc = df_filtered["weather_condition"].dropna().value_counts().head(8).reset_index()
        wc.columns = ["Weather", "Crashes"]
        st.bar_chart(wc.set_index("Weather"), color="#f97316")
    else:
        st.info("weather_condition column not found")

with col_r:
    st.markdown('<div class="section-title">Crashes by Type</div>', unsafe_allow_html=True)
    if "first_crash_type" in df_filtered.columns:
        ct = df_filtered["first_crash_type"].dropna().value_counts().head(8).reset_index()
        ct.columns = ["Crash Type", "Count"]
        st.bar_chart(ct.set_index("Crash Type"), color="#38bdf8")
    else:
        st.info("first_crash_type column not found")

col_a, col_b = st.columns(2)

with col_a:
    st.markdown('<div class="section-title">Road Surface Conditions</div>', unsafe_allow_html=True)
    if "roadway_surface_cond" in df_filtered.columns:
        rc = df_filtered["roadway_surface_cond"].dropna().value_counts().head(6).reset_index()
        rc.columns = ["Surface", "Count"]
        st.bar_chart(rc.set_index("Surface"), color="#34d399")
    else:
        st.info("roadway_surface_cond column not found")

with col_b:
    st.markdown('<div class="section-title">Lighting Conditions</div>', unsafe_allow_html=True)
    if "lighting_condition" in df_filtered.columns:
        lc = df_filtered["lighting_condition"].dropna().value_counts().reset_index()
        lc.columns = ["Lighting", "Count"]
        st.bar_chart(lc.set_index("Lighting"), color="#fb7185")
    else:
        st.info("lighting_condition column not found")

# Crash location map (silver)
if not map_ready.empty:
    st.markdown('<div class="section-title">📍 Crash Locations Map</div>', unsafe_allow_html=True)
    valid_map = map_ready[
        map_ready["latitude"].between(41.6, 42.1) &
        map_ready["longitude"].between(-87.9, -87.5)
    ][["latitude", "longitude"]]
    if not valid_map.empty:
        st.map(valid_map, zoom=10)

st.markdown("---")


# =====================================================
# GOLD: CRASH HOTSPOTS
# =====================================================
st.markdown('<div class="section-title">🥇 Gold — Crash Hotspots</div>', unsafe_allow_html=True)

if df_gold_h is not None:
    for col in ["latitude", "longitude", "accident_count"]:
        if col in df_gold_h.columns:
            df_gold_h[col] = pd.to_numeric(df_gold_h[col], errors="coerce")

    # year/month now exist thanks to pd.read_parquet partition reading
    gh_filtered = df_gold_h.copy()
    if "year" in df_gold_h.columns and "month" in df_gold_h.columns:
        df_gold_h["year"]  = pd.to_numeric(df_gold_h["year"],  errors="coerce").astype("Int64")
        df_gold_h["month"] = pd.to_numeric(df_gold_h["month"], errors="coerce").astype("Int64")
        gh_filtered = df_gold_h[
            (df_gold_h["year"]  == selected_year) &
            (df_gold_h["month"] == selected_month)
        ]

    top10 = gh_filtered.sort_values("accident_count", ascending=False).head(10).reset_index(drop=True)
    top10.index += 1

    if not top10.empty:
        max_c = top10["accident_count"].max()
        top10["risk_score"] = (top10["accident_count"] / max_c * 100).round(1)
        top10["risk_level"]  = top10["risk_score"].apply(
            lambda x: "🔴 High" if x >= 70 else ("🟡 Medium" if x >= 40 else "🟢 Low")
        )
        st.dataframe(
            top10[["latitude", "longitude", "accident_count", "risk_score", "risk_level"]],
            use_container_width=True
        )
        map_gold = top10[["latitude","longitude"]].dropna()
        map_gold = map_gold[
            map_gold["latitude"].between(41.6, 42.1) &
            map_gold["longitude"].between(-87.9, -87.5)
        ]
        if not map_gold.empty:
            st.map(map_gold, zoom=10)
    else:
        st.info(f"No hotspot data for {months_map.get(selected_month)} {selected_year}")
else:
    st.warning(f"Gold hotspots not loaded: {e3}")

st.markdown("---")


# =====================================================
# GOLD: ROAD RISK INDEX
# =====================================================
st.markdown('<div class="section-title">🥇 Gold — Road Risk Index</div>', unsafe_allow_html=True)

if df_gold_r is not None:
    for col in ["latitude", "longitude", "risk_score", "accident_count"]:
        if col in df_gold_r.columns:
            df_gold_r[col] = pd.to_numeric(df_gold_r[col], errors="coerce")

    top_risk = df_gold_r.sort_values("risk_score", ascending=False).head(10).reset_index(drop=True)
    top_risk.index += 1

    rk1, rk2, rk3 = st.columns(3)
    rk1.metric("📍 Total Locations",  f"{len(df_gold_r):,}")
    rk2.metric("🚗 Total Accidents",  f"{int(df_gold_r['accident_count'].sum()):,}" if "accident_count" in df_gold_r.columns else "N/A")
    rk3.metric("⚠️ Avg Risk Score",   f"{df_gold_r['risk_score'].mean():.2f}"        if "risk_score"     in df_gold_r.columns else "N/A")

    st.dataframe(top_risk, use_container_width=True)

    if "risk_score" in df_gold_r.columns:
        st.markdown('<div class="section-title">Risk Score Distribution (Top 20)</div>', unsafe_allow_html=True)
        risk_chart = df_gold_r.sort_values("risk_score", ascending=False).head(20).copy()
        risk_chart["location"] = risk_chart.apply(
            lambda r: f"{round(r['latitude'],3)},{round(r['longitude'],3)}"
            if pd.notnull(r.get("latitude")) else "unknown", axis=1
        )
        st.bar_chart(risk_chart.set_index("location")["risk_score"], color="#dc2626")
else:
    st.warning(f"Gold risk index not loaded: {e4}")

st.markdown("---")


# =====================================================
# GOLD: MONTHLY COMBINED
# =====================================================
st.markdown('<div class="section-title">🥇 Gold — Monthly: Crashes vs YouTube Videos</div>', unsafe_allow_html=True)

if df_gold_m is not None:
    for col in ["accident_count", "youtube_video_count", "videos_per_100_accidents"]:
        if col in df_gold_m.columns:
            df_gold_m[col] = pd.to_numeric(df_gold_m[col], errors="coerce")

    if "year" in df_gold_m.columns and "month" in df_gold_m.columns:
        df_gold_m = df_gold_m.sort_values(["year", "month"])
        df_gold_m["period"] = df_gold_m.apply(
            lambda r: f"{months_map.get(int(r['month']),'')} {int(r['year'])}", axis=1
        )
        chart_cols = [c for c in ["accident_count", "youtube_video_count"] if c in df_gold_m.columns]
        if chart_cols:
            st.line_chart(df_gold_m.set_index("period")[chart_cols], color=["#dc2626","#a78bfa"])
            st.caption("🔴 Crash count  &nbsp; 🟣 YouTube road safety video uploads")

        if "videos_per_100_accidents" in df_gold_m.columns:
            st.markdown('<div class="section-title">Videos per 100 Accidents</div>', unsafe_allow_html=True)
            st.bar_chart(df_gold_m.set_index("period")["videos_per_100_accidents"], color="#f59e0b")

    st.dataframe(df_gold_m.drop(columns=["period"], errors="ignore"), use_container_width=True)
else:
    st.warning(f"Gold monthly not loaded: {e5}")

st.markdown("---")


# =====================================================
# RAW DATA PREVIEW
# =====================================================
with st.expander("🔍 Silver — Raw Chicago Crashes (first 100 rows)"):
    st.caption(f"Total: {len(df_silver_c):,} records · Columns: {list(df_silver_c.columns)}")
    st.dataframe(df_silver_c.head(100), use_container_width=True)

if df_silver_y is not None:
    with st.expander("🔍 Silver — Raw YouTube Records (first 100 rows)"):
        st.caption(f"Total: {len(df_silver_y):,} records · Columns: {list(df_silver_y.columns)}")
        st.dataframe(df_silver_y.head(100), use_container_width=True)


# ================= FOOTER =================
st.markdown("---")
st.markdown(
    "<div style='text-align:center;color:#94a3b8;font-size:0.78rem;font-family:IBM Plex Mono,monospace'>"
    "🟤 Bronze (JSON) → ⚪ Silver (Parquet) → 🥇 Gold (Aggregated) &nbsp;·&nbsp; "
    "Urban Traffic Risk & Public Sentiment"
    "</div>",
    unsafe_allow_html=True
)