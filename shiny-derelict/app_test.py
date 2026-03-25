from shiny.express import input, render, ui
from shiny import reactive, Session
from shinywidgets import render_widget, output_widget, register_widget
from shiny.ui import page_navbar
import duckdb
from functools import partial
from ipyleaflet import Map, CircleMarker, LayerGroup
from ipywidgets import Layout

# Define the main page UI
ui.page_opts(
    title="App with navbar",  
    page_fn=partial(page_navbar, id="page"), 
)

#---------------------------First PAGE---------------------------#
with ui.nav_panel("Historical Map"):  
    
    # Create a date input for date selection
    ui.input_date(
        "select_date", "Select Date",
        value="2020-01-17")

    @render_widget
    def map():
        # Fixed center and zoom to show majority of Europe
        m = Map(center=(54, 15), zoom=4, scroll_wheel_zoom=True, layout=Layout(width="100%", height="600px"))
        
        # Create a layer group for markers to update reactively
        marker_layer = LayerGroup(name="markers")
        m.add_layer(marker_layer)
        
        return m

    @reactive.Calc
    def filtered_data():
        """Get data for the selected date"""
        conn = duckdb.connect("eeaopt.db")
        query = f"""
            SELECT
                "Date",
                "Value",
                "Latitude",
                "Longitude"
            FROM airquality_5
            WHERE DATE("Date") = '{input.select_date()}'
                AND "Latitude" IS NOT NULL
                AND "Longitude" IS NOT NULL
        """
        df = conn.execute(query).fetch_df()
        conn.close()
        return df

    @reactive.Effect
    def update_markers():
        """Update map markers based on selected date"""
        df = filtered_data()
        
        # In shinywidgets with ipyleaflet, you must interact with the underlying .widget property
        # when accessing from a reactive effect
        widget_map = map.widget
        
        # Access or recreate the marker layer from the map widget layer list
        marker_layer = None
        for layer in widget_map.layers:
            if getattr(layer, 'name', '') == "markers":
                marker_layer = layer
                break
                
        if marker_layer is None:
            marker_layer = LayerGroup(name="markers")
            widget_map.add_layer(marker_layer)
            
        # Clear existing layers first
        marker_layer.clear_layers()
        
        # Add new markers for the selected date
        for _, row in df.iterrows():
            marker = CircleMarker(
                location=(row["Latitude"], row["Longitude"]),
                radius=6,
                color='red',
                fill_color='lightcoral',
                fill_opacity=0.6,
                weight=1
            )
            marker_layer.add_layer(marker)

