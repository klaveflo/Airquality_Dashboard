from shiny.express import input, render, ui
from shinywidgets import render_widget, output_widget
from shiny.ui import page_navbar
from matplotlib import pyplot as plt
import duckdb
from functools import partial
from ipyleaflet import Map, GeoJSON
import json
import pandas as pd

# Define the list of countries for the dropdown menu
country_list = ['CH', 'GB','AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE']


# Define the main page UI
ui.page_opts(
    title="App with navbar",  
    page_fn=partial(page_navbar, id="page"), 
)

#---------------------------FIRST PAGE---------------------------#
with ui.nav_panel("Historical Map"):
    with ui.layout_sidebar():
        with ui.sidebar():
            # Create a date input for date selection
            ui.input_date(
                "select_date", "Select Date",
                value="2020-01-17")
            
            # Create a selectize input for country selection to highlight in scatter plot
            ui.input_selectize(
                "highlight_country", "Highlight Country",
                choices=country_list)

            # Create a selectize input for pollution variable selection
            ui.input_selectize(
                "pollution_var", "Select Pollution Variable",
                choices={'airquality_5': 'Air Quality PM10', 'airquality_6001': 'Air Quality PM2.5'},
                selected='airquality_5')
        
        with ui.card():
            @render_widget
            def map():
                # Fixed center and zoom to show majority of Europe
                m = Map(center=(54, 15), zoom=4, scroll_wheel_zoom=True)

                # Add markers based on the selected date using GeoJSON (much faster!)
                conn = duckdb.connect("eeaopt.db")
                query = f"""
                    SELECT
                        "Date",
                        "Value",
                        "Latitude",
                        "Longitude"
                    FROM {input.pollution_var()}
                    WHERE DATE("Date") = '{input.select_date()}'
                        AND "Latitude" IS NOT NULL
                        AND "Longitude" IS NOT NULL
                """
                df = conn.execute(query).fetch_df()
                conn.close()

                # Convert to GeoJSON format for efficient rendering
                features = []
                for _, row in df.iterrows():
                    features.append({
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [row["Longitude"], row["Latitude"]]
                        },
                        "properties": {
                            "value": row["Value"]
                        }
                    })
                
                geojson_data = {
                    "type": "FeatureCollection",
                    "features": features
                }
                
                # Add GeoJSON layer with custom styling
                geo = GeoJSON(data=geojson_data, style={
                    'color': 'red',
                    'opacity': 0.6,
                    'weight': 1,
                    'fillColor': 'lightcoral'
                })
                m.add(geo)

                return m
        
        with ui.card():
            @render.plot
            def scatter():
                # Get time series data for the selected country
                conn = duckdb.connect("eeaopt.db")
                
                if input.highlight_country():
                    query = f"""
                        SELECT
                            "Date",
                            "Value",
                            "Country"
                        FROM {input.pollution_var()}
                        WHERE "Country" = '{input.highlight_country()}'
                            AND "Value" IS NOT NULL
                        ORDER BY "Date"
                    """
                else:
                    # If no country selected, show all countries for the selected date
                    query = f"""
                        SELECT
                            "Date",
                            "Value",
                            "Country"
                        FROM {input.pollution_var()}
                        WHERE DATE("Date") = '{input.select_date()}'
                            AND "Value" IS NOT NULL
                        ORDER BY "Date"
                    """
                
                df = conn.execute(query).fetch_df()
                conn.close()
                
                # Create line plot
                plt.figure(figsize=(12, 5))
                
                if input.highlight_country():
                    # Plot time series for selected country
                    plt.plot(df['Date'], df['Value'], color='red', linewidth=2, marker='o', markersize=4)
                    plt.title(f"Air Quality Time Series for {input.highlight_country()}")
                else:
                    # Plot all countries for the selected date
                    for country in df['Country'].unique():
                        country_data = df[df['Country'] == country]
                        plt.plot(country_data['Date'], country_data['Value'], marker='o', markersize=4, label=country, alpha=0.7)
                    plt.title(f"Air Quality Measurements on {input.select_date()}")
                    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8, ncol=1)
                
                plt.xlabel("Date")
                plt.ylabel("Value")
                plt.xticks(rotation=45)
                plt.tight_layout()

#---------------------------SECOND PAGE---------------------------#
with ui.nav_panel("Historical Plots"):  
    'site2'

#---------------------------THIRD PAGE---------------------------#
with ui.nav_panel("Live Data"):  
    'site3'


