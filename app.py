from shiny.express import input, render, ui
from shinywidgets import render_widget, output_widget
from shiny.ui import page_navbar
from matplotlib import pyplot as plt
import duckdb
from functools import partial
from ipyleaflet import Map, CircleMarker
import ipyleaflet


# Define the main page UI
ui.page_opts(
    title="App with navbar",  
    page_fn=partial(page_navbar, id="page"), 
)

#---------------------------FIRST PAGE---------------------------#
with ui.nav_panel("Historical Plots"):  
    
    # Define the list of countries for the dropdown menu
    country_list = ['CH', 'GB','AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE']

    # Create a selectize input for country selection
    ui.input_selectize(
        "country", "Select Country",
        choices=country_list)  
    
    # Render the plot based on the selected country
    @render.plot
    def plot():
        # Connect to DuckDB and fetch data for the selected country
        conn = duckdb.connect("eeach.db")
        query = f"""
            SELECT
                "Date",
                "Value"
            FROM airquality_data
            WHERE "Country" = '{input.country()}'
        """
        df = conn.execute(query).fetch_df()

        plt.figure(figsize=(10, 6))
        plt.plot(df['Date'], df['Value'])
        plt.xlabel("Date")
        plt.ylabel("Value")
        plt.title(f"Air Quality in {input.country()}")
        plt.xticks(rotation=45)
        plt.tight_layout()

#---------------------------SECOND PAGE---------------------------#
with ui.nav_panel("Historical Map"):  
    
    # Create a date input for date selection
    ui.input_date(
        "select_date", "Select Date",
        value="2020-01-17")
    
    output_widget("map")

    @render_widget
    def map():
        # Fixed center and zoom to show majority of Europe
        m = Map(center=(54, 15), zoom=4, scroll_wheel_zoom=True)

        # Add all markers based on the selected date
        conn = duckdb.connect("eeach.db")
        query = f"""
            SELECT
                "Date",
                "Value",
                "Latitude",
                "Longitude"
            FROM airquality_data
            WHERE DATE("Date") = '{input.select_date()}'
                AND "Latitude" IS NOT NULL
                AND "Longitude" IS NOT NULL
        """
        df = conn.execute(query).fetch_df()
        conn.close()

        # Add all individual markers
        for _, row in df.iterrows():
            marker = CircleMarker(
                location=(row["Latitude"], row["Longitude"]),
                radius=6,
                color='red',
                fill_color='lightcoral',
                fill_opacity=0.6,
                weight=1
            )
            m.add(marker)

        return m

#---------------------------THIRD PAGE---------------------------#
with ui.nav_panel("Live Data"):  
    output_widget("map2")
    
    @render_widget
    def map2():
        # Fixed center and zoom to show majority of Europe
        m2 = Map(center=(54, 15), zoom=4, scroll_wheel_zoom=True)
        return m2


