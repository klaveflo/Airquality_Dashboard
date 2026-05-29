# Airquality_Dashboard

A Shiny/Python dashboard for exploring air quality data, featuring both live and historic data visualizations. The dashboard provides interactive maps, time series charts, and data exploration tools for air quality monitoring stations across multiple countries.

## Features

- **Live Data Tab**:  
	- Explore real-time air quality data for pollutants such as PM2.5, PM10, NO2, O3.
    - View real-time air quality data on an interactive map.
	- Select specific stations and visualize pollutant levels.
    - Animated the latest time period for a selected country.

- **Historic Data Tab**:  
	- Explore historical air quality data for PM2.5 and PM10.
    - View historic air quality data on an interactive map.
	- Detailed charts and tooltips for data exploration.
    - Ask AI questions about the data with an integrated chatbot.

## Project Structure

- `app.py` — Main entry point for the dashboard.
- `live_tab.py` — UI and server logic for the live data tab.
- `hist_tab.py` — UI and server logic for the historic data tab.
- `shared.py` — Shared functions and constants.
- `requirements.txt` — Python dependencies.
- `station_metadata_clean.csv` — Station metadata.
- `eeaopt.db` — DuckDB database with air quality data.

## Running the dashboard

### Local Setup
1. Clone the repository and navigate to the project folder.
2. Install dependencies:
	 ```
	 pip install -r requirements.txt
	 ```
3. start the dashboard with:
     ```
     shiny run app.py
     ```

### On hosted website
Simply go to the following link:

[https://airquality-dashboard.shinyapps.io/airquality_dashboard/](https://airquality-dashboard.shinyapps.io/airquality_dashboard/)

## Requirements

- Python 3.9+
- See `requirements.txt` for all dependencies if you wanna run it locally.

## Data Sources
- Air quality data is sourced from the European Environment Agency (EEA).
    - Historic data is stored in a DuckDB database (`eeaopt.db`).
    - Live data is fetched from the EEA API.