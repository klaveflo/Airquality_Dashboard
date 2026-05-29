"""
app.py - entry point for the combined Air Quality Dashboard.

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

from dotenv import load_dotenv
load_dotenv()

import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from shiny import App, ui

from live_tab import live_ui, live_server
from hist_tab import hist_ui, hist_server


# ── Shared CSS ─────────────────────────────────────────────────────────────────

_APP_CSS = """
/* ── Dark mode (default) ───────────────────────────────────────────────── */
body { background-color: #0e1117; color: #fafafa; }
.card { background-color: #1a1a1a; border: 1px solid #333; margin-bottom: 8px !important; }
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
.selectize-dropdown .active { background-color: #4477AA !important; color: #fff !important; }
/* Selected tags/chips inside multi-select inputs */
.selectize-input .item {
    background-color: #4477AA !important;
    color: #fff !important;
    border-radius: 3px !important;
    padding: 1px 6px !important;
}
.btn-primary { background-color: #4477AA; border-color: #4477AA; color: #fff; }
.btn-primary:hover { background-color: #2d5a8e; border-color: #2d5a8e; color: #fff; }
.btn-outline-secondary {
    background-color: transparent !important;
    border: 1.5px solid #888 !important;
    color: #fafafa !important;
}
.btn-outline-secondary:hover {
    background-color: #333 !important;
    border-color: #aaa !important;
    color: #fafafa !important;
}
.irs--shiny .irs-bar { background: #4477AA; border-top-color: #4477AA; border-bottom-color: #4477AA; }
.irs--shiny .irs-from, .irs--shiny .irs-to, .irs--shiny .irs-single { background-color: #4477AA; }
.irs--shiny .irs-line { background: #333; border-color: #333; }
.irs--shiny .irs-grid-text { color: #aaa; }
.shiny-notification { background-color: #1a1a1a; color: #fafafa; border-color: #333; }
.row { margin-bottom: 0 !important; }
#deck-map, #deck-hist-map {
    width: 100%;
    height: 450px;
    border-radius: 4px;
    overflow: hidden;
    position: relative;
    background: #0e1117;
}
#deck-map canvas, #deck-hist-map canvas { outline: none; }

/* ── Stats boxes ───────────────────────────────────────────────────────── */
.stats-box {
    background-color: #1a1a1a;
    border: 1px solid #333;
    border-radius: 6px;
    padding: 10px 14px;
    margin: 6px 0;
    font-size: 13px;
    line-height: 1.8;
    color: #fafafa;
}
.stats-box b { color: #fafafa; }

/* ── Light mode overrides ──────────────────────────────────────────────── */
@media (prefers-color-scheme: light) {
    .stats-box {
        background-color: #f5f5f5;
        border-color: #ddd;
        color: #222;
    }
    .stats-box b { color: #222; }
    .stats-box span[style*="color:#aaa"] { color: #666 !important; }
    .stats-box span[style*="color:#bbb"] { color: #555 !important; }
    .stats-box span[style*="color:#ccc"] { color: #444 !important; }
    body { background-color: #f5f7fa; color: #1a1a1a; }
    .card { background-color: #ffffff; border: 1px solid #ddd; }
    .shiny-input-container label { color: #1a1a1a; }
    h1, h2, h3, h4, h5, h6 { color: #1a1a1a; }
    .well { background-color: #ffffff; border: 1px solid #ddd; }
    .form-control, .selectize-input {
        background-color: #ffffff !important;
        color: #1a1a1a !important;
        border-color: #ccc !important;
    }
    .selectize-dropdown, .selectize-dropdown-content {
        background-color: #ffffff !important; color: #1a1a1a !important;
    }
    .irs--shiny .irs-line { background: #ddd; border-color: #ddd; }
    .irs--shiny .irs-grid-text { color: #555; }
    .shiny-notification { background-color: #ffffff; color: #1a1a1a; border-color: #ddd; }
    #deck-map, #deck-hist-map { background: #e8eaf0; }
    .btn-outline-secondary {
        border-color: #555 !important;
        color: #1a1a1a !important;
    }
    .btn-outline-secondary:hover {
        background-color: #e0e0e0 !important;
        border-color: #333 !important;
        color: #1a1a1a !important;
    }
}
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
    ui.tags.p(
        "How clean is the air you breathe? This dashboard lets you explore air quality " 
        "across Europe, powered by open data from the European Environment Agency (EEA)."
        "Dive into live measurements from hundreds of monitoring stations or explore over " 
        "a decade of historical trends. Compare cities, track seasonal patterns, and discover "
        "how pollution levels respond to weather, traffic, and policy changes.",
        style="font-size:14px; margin-bottom:16px"
    ),
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
