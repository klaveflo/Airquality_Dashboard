from shiny.express import input, render, ui
from shinywidgets import render_widget, output_widget
from shiny.ui import page_navbar
from matplotlib import pyplot as plt
import duckdb
from functools import partial
from ipyleaflet import Map, CircleMarker
import ipyleaflet

# Define the list of countries for the dropdown menu
country_list = ['CH', 'GB','AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE']


# Define the main page UI
ui.page_opts(
    title="App with navbar",  
    page_fn=partial(page_navbar, id="page"), 
)

#---------------------------THIRD PAGE---------------------------#
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

                # Add all markers based on the selected date
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
        
        with ui.card():
            @render.plot
            def scatter():
                # Get data for the selected date
                conn = duckdb.connect("eeaopt.db")
                query = f"""
                    SELECT
                        "Date",
                        "Value",
                        "Country"
                    FROM {input.pollution_var()}
                    WHERE DATE("Date") = '{input.select_date()}'
                        AND "Value" IS NOT NULL
                """
                df = conn.execute(query).fetch_df()
                conn.close()
                
                # Create stem plot
                plt.figure(figsize=(10, 4))
                
                # Plot all countries in gray
                plt.stem(df['Date'], df['Value'], linefmt='gray', markerfmt='o', basefmt=' ')
                plt.setp(plt.gca().collections[0], color='lightgray', alpha=0.5)
                
                # Highlight selected country in red
                if input.highlight_country():
                    highlighted = df[df['Country'] == input.highlight_country()]
                    plt.stem(highlighted['Date'], highlighted['Value'], linefmt='red', markerfmt='o', basefmt=' ')
                    plt.setp(plt.gca().collections[-1], color='red', alpha=0.8)
                
                plt.xlabel("Date")
                plt.ylabel("Value")
                plt.title(f"Air Quality Measurements on {input.select_date()}")
                plt.xticks(rotation=45)
                plt.legend(['All countries', input.highlight_country()] if input.highlight_country() else ['All countries'])
                plt.tight_layout()

#---------------------------SECOND PAGE---------------------------#
with ui.nav_panel("Historical Plots"):  
    'site2'

#---------------------------THIRD PAGE---------------------------#
with ui.nav_panel("Live Data"):  
    'site3'


