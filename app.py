"""
app.py — entry point for the combined Air Quality Dashboard.

Run with:
    shiny run --reload app.py

Tabs:
  * Historic Data  (hist_tab.py)
  * Live Data      (live_tab.py)

JS message channels:
  Live tab:  'update_map_data', 'update_selected_stations'
             input 'map_click'  (deck-map container)
  Hist tab:  'update_hist_map_data'
             (deck-hist-map container, no click-back to server)

There are no overlapping IDs between tabs.
"""

import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from shiny import App, ui

from live_tab import live_ui, live_server
from hist_tab import hist_ui, hist_server


# ── Shared CSS ─────────────────────────────────────────────────────────────────

_APP_CSS = """
body { background-color: #0e1117; color: #fafafa; }
.card { background-color: #1a1a1a; border: 1px solid #333; }
.shiny-input-container label { color: #fafafa; }
h1, h2, h3, h4, h5, h6 { color: #fafafa; }
.well { background-color: #1a1a1a; border: 1px solid #333; }
.form-control, .selectize-input {
    background-color: #262730 !important;
    color: #fafafa !important;
    border-color: #444 !important;
}
.selectize-dropdown, .selectize-dropdown-content {
    background-color: #262730 !important; color: #fafafa !important;
}
.selectize-dropdown .active { background-color: #ff4b4b !important; color: #fff !important; }
.btn-primary { background-color: #ff4b4b; border-color: #ff4b4b; }
.btn-primary:hover { background-color: #d43d3d; border-color: #d43d3d; }
.irs--shiny .irs-bar { background: #ff4b4b; border-top-color: #ff4b4b; border-bottom-color: #ff4b4b; }
.irs--shiny .irs-from, .irs--shiny .irs-to, .irs--shiny .irs-single { background-color: #ff4b4b; }
.irs--shiny .irs-line { background: #333; border-color: #333; }
.irs--shiny .irs-grid-text { color: #aaa; }
.shiny-notification { background-color: #1a1a1a; color: #fafafa; border-color: #333; }
#deck-map, #deck-hist-map {
    width: 100%;
    height: 450px;
    border-radius: 4px;
    overflow: hidden;
    position: relative;
    background: #0e1117;
}
#deck-map canvas, #deck-hist-map canvas { outline: none; }
"""


# ── App UI ─────────────────────────────────────────────────────────────────────

app_ui = ui.page_fluid(
    ui.head_content(
        ui.tags.link(rel="stylesheet",
                     href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css"),
        ui.tags.script(src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"),
        ui.tags.script(src="https://unpkg.com/deck.gl@9.0.33/dist.min.js"),
    ),
    ui.tags.style(_APP_CSS),
    ui.h1("Air Quality Map Dashboard"),
    ui.navset_tab(
        live_ui(),
        hist_ui(),
        selected="Live Data",
    ),
)


# ── App server ─────────────────────────────────────────────────────────────────

def server(input, output, session):
    live_server(input, output, session)
    hist_server(input, output, session)


app = App(app_ui, server)
