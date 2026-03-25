from shiny.express import ui
from shinywidgets import render_widget
import duckdb
from ipyleaflet import Map, CircleMarker
from ipywidgets import Layout

date = "2020-01-17"

# Basic data querying
conn = duckdb.connect("eeaopt.db")
query = f"""
    SELECT
        "Date",
        "Value",
        "Latitude",
        "Longitude"
    FROM airquality_5
    WHERE DATE("Date") = '{date}'
        AND "Latitude" IS NOT NULL
        AND "Longitude" IS NOT NULL
"""
df = conn.execute(query).fetch_df()
conn.close()

# Print the number of records retrieved
print(len(df))
# Print the unique number of locations
print(len(df[["Latitude", "Longitude"]].drop_duplicates()))
# Print the value of those locations with duplicates
duplicates = df[df.duplicated(subset=["Latitude", "Longitude"], keep=False)]
print(duplicates[["Latitude", "Longitude", "Value"]].sort_values(by=["Latitude", "Longitude"]))

ui.h3("Air Quality Map Diagnostics")
ui.p(f"Total markers on map: {len(df)}")

@render_widget
def map():
    # Fixed center and zoom to show majority of Europe
    m = Map(center=(54, 15), zoom=4, scroll_wheel_zoom=True, layout=Layout(width="100%", height="600px"))
    
    # Add markers for each data point
    marker_count = 0
    for _, row in df.iterrows():
        marker = CircleMarker(
            location=(row["Latitude"], row["Longitude"]),
            radius=5,
            color="blue",
            fill=True,
            fill_color="blue",
            fill_opacity=0.6,
            tooltip=f"Value: {row['Value']}"
        )
        m.add_layer(marker)
        marker_count += 1
        
    print(f"Added {marker_count} markers to the map.")
    
    return m