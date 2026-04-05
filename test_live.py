import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import altair as alt

# Set up the page for our isolated test
st.set_page_config(page_title="Live Data Test", layout="wide")
st.title("Live Air Quality Data (Last 7 Days)")

@st.cache_data(ttl=3600, show_spinner=False)
def get_live_data(country_code, pollutant):
    """Fetches the last 7 days of hourly data for a given country and pollutant."""
    api_url = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/ParquetFile/urls"
    
    # [FIX 1] Replaced deprecated utcnow() with timezone-aware UTC datetime
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=7)
    
    payload = {
        "countries": [country_code],
        "cities": [], 
        "pollutants": [pollutant],
        "dataset": 1, # 1 = E2a (Up-To-Date Live Data)
        "dateTimeStart": start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "dateTimeEnd": end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "compress": False
    }
    
    response = requests.post(api_url, json=payload)
    response.raise_for_status() 
    
    lines = response.text.strip().split('\n')
    urls = [line.strip() for line in lines if line.strip() and "ParquetFileUrl" not in line]
    
    if not urls:
        return None, "No data found for this combination."
        
    # For this simple prototype, we just grab the very first URL/Station
    download_url = urls[0]
    
    df = pd.read_parquet(download_url)
    time_col = "Start"
    val_col = "Value"
    
    # Safely parse the dates
    if pd.api.types.is_numeric_dtype(df[time_col]):
        df[time_col] = pd.to_datetime(df[time_col], unit='ns', errors='coerce')
    else:
        df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
        
    df[time_col] = df[time_col].dt.tz_localize(None) 
    
    # Filter for the last 7 days
    # (Removing timezone from our start_date for the comparison)
    mask = (df[time_col] >= start_date.replace(tzinfo=None))
    df_filtered = df.loc[mask].copy()
    
    # [FIX 2] Force the Value column to be numeric, drop missing values, and reset the index for Altair
    df_filtered[val_col] = pd.to_numeric(df_filtered[val_col], errors='coerce')
    df_filtered = df_filtered[df_filtered[val_col] >= 0]
    df_filtered = df_filtered.dropna(subset=[time_col, val_col]).reset_index(drop=True)
    
    # Extract the station ID for display
    station_id = df_filtered["Samplingpoint"].iloc[0] if not df_filtered.empty else "Unknown Station"
    
    return df_filtered, station_id

# --- USER INTERFACE ---
with st.form("live_data_form"):
    col1, col2 = st.columns(2)
    with col1:
        selected_country = st.selectbox("Country", ["AT", "DE", "FR", "IT", "ES"])
    with col2:
        selected_pollutant = st.selectbox("Pollutant", ["PM10", "PM2.5", "NO2", "O3"])
        
    submit_btn = st.form_submit_button("Fetch Live Data")

if submit_btn:
    with st.spinner("Fetching data from EEA API..."):
        df, station_info = get_live_data(selected_country, selected_pollutant)
        
        if df is None or df.empty:
            st.warning("No recent data found. Try another combination.")
        else:
            st.success(f"Successfully loaded {len(df)} records for Station: **{station_info}**")
            
            # Create a simple, interactive Altair line chart
            chart = alt.Chart(df).mark_line(point=True, color='#1f77b4').encode(
                x=alt.X('Start:T', title='Date & Time'),
                y=alt.Y('Value:Q', title=f'{selected_pollutant} Concentration'),
                tooltip=[
                    alt.Tooltip('Start:T', title='Time', format='%Y-%m-%d %H:%M'),
                    alt.Tooltip('Value:Q', title='Value')
                ]
            ).properties(
                height=400
            ).interactive()
            
            # [FIX 3] Updated parameter to fix the Streamlit deprecation warning
            st.altair_chart(chart, width='stretch')