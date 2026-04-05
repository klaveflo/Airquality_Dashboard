import streamlit as st
import duckdb
import pandas as pd
import datetime
import pydeck as pdk
import altair as alt
import time
from query_llm import ask_llm_about_peak
import os
import streamlit as st

# Securely inject the API key from Streamlit's secrets into the environment
if "GEMINI_API_KEY" in st.secrets:
    os.environ["GEMINI_API_KEY"] = st.secrets["GEMINI_API_KEY"]

# Set up the dashboard page
st.set_page_config(page_title="Air Quality Dashboard", layout="wide")

# Custom CSS to reduce top spacing and make everything more compact to prevent scrolling
st.markdown("""
    <style>
        .block-container {
            padding-top: 1rem;
            padding-bottom: 0rem;
        }
        h1 {
            margin-top: -2rem;
        }
    </style>
""", unsafe_allow_html=True)

st.title("Air Quality Map Dashboard")

# Connect to DuckDB (cached so it doesn't reconnect on every interaction)
@st.cache_resource
def get_connection():
    # adjust the path to your database if it's located elsewhere
    return duckdb.connect("eeaopt.db", read_only=True)

conn = get_connection()

tab1, tab2 = st.tabs(["Historic Data", "Live Data"])

# 2. Query functions
def apply_styling(df):
    if not df.empty and "Value" in df.columns:
        val = df["Value"].fillna(0)
        max_val = 240
        val_capped = val.clip(lower=0, upper=max_val)
        
        is_missing = df["Value"].isna() | (df["Value"] == 0)
        
        # Calculate size based on 10 percentiles (deciles)
        df["size"] = 0
        valid_mask = ~is_missing
        if valid_mask.sum() > 0:
            try:
                # 10 percentiles: pd.qcut gives values 0-9
                deciles = pd.qcut(df.loc[valid_mask, "Value"], q=10, labels=False, duplicates='drop')
                # Ensure visibility with incremental size based on where it falls in the decile
                df.loc[valid_mask, "size"] = 3000 + (deciles * 2000)
            except ValueError:
                # Fallback if not enough data to create quantiles
                df.loc[valid_mask, "size"] = 5000
        
        norm = val_capped / max_val
        df["color_r"] = 255
        df["color_g"] = (255 * (1 - norm)).astype(int)
        df["color_b"] = (255 * (1 - norm)).astype(int)
        df["color_a"] = 180
        df.loc[is_missing, "color_a"] = 0
    return df

@st.cache_data
def get_master_stations(table_name, start, end):
    query = f"""
        SELECT DISTINCT "Latitude" as lat, "Longitude" as lon
        FROM {table_name}
        WHERE DATE("Date") BETWEEN '{start}' AND '{end}'
            AND "Latitude" IS NOT NULL
            AND "Longitude" IS NOT NULL
    """
    df = conn.execute(query).fetch_df()
    return df.sort_values(by=['lat', 'lon']).reset_index(drop=True)

@st.cache_data
def get_map_data(date_str, table_name, _master_df=None):
    query = f"""
        SELECT
            "Value",
            "Latitude" as lat,
            "Longitude" as lon
        FROM {table_name}
        WHERE DATE("Date") = '{date_str}'
            AND "Latitude" IS NOT NULL
            AND "Longitude" IS NOT NULL
    """
    df = conn.execute(query).fetch_df()
    if _master_df is not None and not _master_df.empty:
        df = df.groupby(['lat', 'lon'], as_index=False)['Value'].mean()
        merged = pd.merge(_master_df, df, on=['lat', 'lon'], how='left')
        return apply_styling(merged)
        
    return apply_styling(df)

@st.cache_data
def get_daily_averages(table_name, start, end):
    query = f"""
        SELECT 
            DATE("Date") as "Date", 
            AVG("Value") as AvgValue
        FROM {table_name}
        WHERE "Value" IS NOT NULL AND DATE("Date") BETWEEN '{start}' AND '{end}'
        GROUP BY DATE("Date")
        ORDER BY "Date"
    """
    df = conn.execute(query).fetch_df()
    df["Date"] = pd.to_datetime(df["Date"])
    return df

def render_map(df_to_render, default_lat=50.0, default_lon=10.0):
    static_view_state = pdk.ViewState(
        latitude=default_lat,
        longitude=default_lon,
        zoom=3.5,
        pitch=0,
    )
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df_to_render,
        id="air-quality-layer",
        get_position=["lon", "lat"],
        get_radius="size" if ("size" in df_to_render.columns) else 10000,
        get_fill_color="[color_r, color_g, color_b, color_a]" if ("color_r" in df_to_render.columns) else [255, 75, 75, 150],
        pickable=True,
        # We overshoot the frame duration slightly so the animation target keeps moving 
        # seamlessly without pausing at its destination
        transitions={"getRadius": 800, "getFillColor": 800}
    )
    return pdk.Deck(layers=[layer] if not df_to_render.empty else [], map_style=None, initial_view_state=static_view_state, tooltip={"text": "Value: {Value:.2f}"})

def render_chart(df_avg, current_date):
    if df_avg.empty:
        return alt.Chart(pd.DataFrame({'Date': [], 'AvgValue': []})).mark_line().properties(height=280)
        
    base = alt.Chart(df_avg).mark_line(color='gray', strokeWidth=2).encode(
        x=alt.X('Date:T', title='Date'),
        y=alt.Y('AvgValue:Q', title='Daily Average Value')
    ).properties(height=190)
    
    current_dt = pd.to_datetime(str(current_date))
    dot_data = df_avg[df_avg['Date'] == current_dt]
    
    if dot_data.empty:
        return base
        
    dot = alt.Chart(dot_data).mark_circle(color='red', size=100, opacity=1).encode(
        x='Date:T',
        y='AvgValue:Q'
    )
    return alt.layer(base, dot).resolve_scale(y='shared')

with tab1:
    # Let's use two main columns for the entire view to prevent deep nesting and scrolling
    col_map, col_data = st.columns([2, 1])
    
    with col_data:
        # Place the metric dropdown dynamically next to the title
        title_col, filter_col = st.columns([1, 1])
        with title_col:
            st.subheader("Historic Data")
        with filter_col:
            selected_metric = st.selectbox("Pollutant", ["PM10", "PM2.5"], label_visibility="collapsed")
            
        table_name = "airquality_5" if selected_metric == "PM10" else "airquality_6001"
        
        if selected_metric == "PM10":
            min_date = datetime.date(2012, 12, 31)
            max_date = datetime.date(2025, 1, 1)
        else:
            min_date = datetime.date(2013, 6, 13)
            max_date = datetime.date(2025, 1, 1)
            
        col_start, col_end = st.columns(2)
        with col_start:
            # We explicitly define the initial setup bounds properly.
            start_date = st.date_input("Start Bound", min_value=min_date, max_value=max_date, value=min_date)
        with col_end:
            end_date = st.date_input("End Bound", min_value=min_date, max_value=max_date, value=max_date)

        if start_date <= end_date:
            # We initialize the scrub dot strictly at exactly Jan 1st 2013 (or mathematically closest based on dataset min setup limits) if no other input overrides it.
            default_start = datetime.date(2013, 1, 1)
            init_val = default_start if default_start >= start_date and default_start <= end_date else start_date
            
            selected_date = st.slider("Scrub through dates", min_value=start_date, max_value=end_date, value=init_val)
            
            if "playing" not in st.session_state:
                st.session_state.playing = False
                
            # Place the play buttons right above the chart
            btn_col1, btn_col2 = st.columns([1, 2])
            with btn_col1:
                play_pressed = st.button("▶️ Play")
            with btn_col2:
                stop_pressed = st.button("⏹️ Stop")
                
            if play_pressed:
                st.session_state.playing = True
            if stop_pressed:
                st.session_state.playing = False
        else:
            st.error("Error: Start Date must be before End Date.")
            selected_date = None

    # Fetch Data & Plot
    if selected_date and start_date <= end_date:
        master_stations = get_master_stations(table_name, str(start_date), str(end_date))
        df_averages = get_daily_averages(table_name, str(start_date), str(end_date))
        
        if st.session_state.playing:
            # Empty placeholders to allow inplace updating for animation
            with col_map:
                map_placeholder = st.empty()
                chart_placeholder = st.empty()
            with col_data:
                status_text = st.empty()
                
            current_date = selected_date
            while current_date <= end_date:
                if not st.session_state.playing:
                    break
                    
                status_text.write(f"**Animating:** {current_date}")
                df = get_map_data(str(current_date), table_name, master_stations)
                
                # We limit map height here to make it a bit smaller and fit the chart nicely below
                map_placeholder.pydeck_chart(render_map(df), use_container_width=True, height=450)
                chart_placeholder.altair_chart(render_chart(df_averages, current_date), use_container_width=True)
                
                # Sleep is slightly less than the pydeck transition duration (800ms vs ~500ms + server overhead)
                # This guarantees that the layer updates with new targets just BEFORE the CSS transition finishes,
                # ensuring that dots never abruptly halt mid-animation.
                time.sleep(0.5)
                current_date += datetime.timedelta(days=1)
                
            st.session_state.playing = False
            
        else:
            # Static display for the current slider selection
            df = get_map_data(str(selected_date), table_name, master_stations)
            
            with col_map:
                st.pydeck_chart(render_map(df), use_container_width=True, height=450)
                st.altair_chart(render_chart(df_averages, selected_date), use_container_width=True)
                
            with col_data:
                st.write(f"**Viewing:** {selected_date}")
                
                # --- NEW LLM QUERY BOX FEATURE ---
                st.markdown("---")
                st.subheader("Ask AI about peaks")
                user_query = st.text_input("Curious about a peak? Ask here:", value=f"What could be the cause of the {selected_metric} peak on {selected_date}?")
                if st.button("Ask AI"):
                    with st.spinner("Analyzing with LLM..."):
                        response = ask_llm_about_peak(str(selected_date), selected_metric)
                        st.info(response)

with tab2:
    st.header("Live Data")
    st.info("Live data view integration coming soon...")