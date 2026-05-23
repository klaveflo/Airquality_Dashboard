"""
hist_tab.py — Historic Data tab UI and server logic.

Exposes:
  hist_ui()                          → NavPanel / ui fragment
  hist_server(input, output, session) → registers all outputs and effects
"""

import datetime as _dt

import pandas as pd

from shiny import ui, render, reactive
from shinywidgets import output_widget, render_altair

from shared import (
    get_aqi_label, EAQI_COLOURS,
    hist_get_master_stations, hist_get_map_data, hist_get_daily_averages,
    hist_get_yoy_data, build_hist_map_payload,
    build_hist_avg_chart, build_yoy_chart, build_hist_eaqi_legend,
)
from query_llm import ask_llm_about_peak


# ── Client-side deck.gl bootstrap (historic map) ──────────────────────────────

_HIST_MAP_INIT_JS = r"""
(function () {
  var deckInst  = null;
  var mapInst   = null;
  var pending   = null;
  var tries     = 0;

  function fillColor(d) {
    return [d.color_r || 200, d.color_g || 200, d.color_b || 200, d.color_a || 180];
  }

  function buildLayers(data) {
    if (!data) data = {urban: [], suburban: [], rural: []};
    var stamp = data.stamp || 0;
    var layers = [];

    // Urban: solid filled circle, no stroke
    layers.push(new deck.ScatterplotLayer({
      id: 'hist-urban-layer',
      data: data.urban || [],
      getPosition: function(d) { return [d.lon, d.lat]; },
      getRadius: 7000, radiusMinPixels: 4, radiusMaxPixels: 14,
      getFillColor: fillColor, pickable: true,
      updateTriggers: { getFillColor: [stamp] }
    }));

    // Suburban: filled circle + thin white border
    layers.push(new deck.ScatterplotLayer({
      id: 'hist-suburban-layer',
      data: data.suburban || [],
      getPosition: function(d) { return [d.lon, d.lat]; },
      getRadius: 6000, radiusMinPixels: 4, radiusMaxPixels: 14,
      getFillColor: fillColor, pickable: true,
      updateTriggers: { getFillColor: [stamp] }
    }));
    layers.push(new deck.ScatterplotLayer({
      id: 'hist-suburban-ring',
      data: data.suburban || [],
      getPosition: function(d) { return [d.lon, d.lat]; },
      getRadius: 6000, radiusMinPixels: 4, radiusMaxPixels: 14,
      stroked: true, filled: false,
      getLineColor: [255, 255, 255, 200], lineWidthMinPixels: 2, pickable: false
    }));

    // Rural: hollow ring, EAQI colour as stroke
    layers.push(new deck.ScatterplotLayer({
      id: 'hist-rural-layer',
      data: data.rural || [],
      getPosition: function(d) { return [d.lon, d.lat]; },
      getRadius: 5000, radiusMinPixels: 4, radiusMaxPixels: 14,
      getFillColor: function(d) { return [d.color_r || 128, d.color_g || 128, d.color_b || 128, 0]; },
      pickable: true,
      updateTriggers: { getFillColor: [stamp] }
    }));
    layers.push(new deck.ScatterplotLayer({
      id: 'hist-rural-ring',
      data: data.rural || [],
      getPosition: function(d) { return [d.lon, d.lat]; },
      getRadius: 5000, radiusMinPixels: 4, radiusMaxPixels: 14,
      stroked: true, filled: false,
      getLineColor: function(d) { return [d.color_r || 128, d.color_g || 128, d.color_b || 128, 220]; },
      lineWidthMinPixels: 3, pickable: false,
      updateTriggers: { getLineColor: [stamp] }
    }));

    return layers;
  }

  function applyData(data) {
    if (data) pending = data;
    if (deckInst && pending) {
      deckInst.setProps({ layers: buildLayers(pending) });
    }
  }

  function buildTooltip(info) {
    if (!info || !info.object) return null;
    var o = info.object;
    return {
      html: '<div style="font-family:sans-serif;font-size:12px;color:#fafafa">'
          + '<b>' + (o.station_name || '—') + '</b><br>'
          + (o.aqi_label || '—') + '<br>'
          + 'Value: ' + (o.value_str || 'N/A') + ' µg/m³</div>',
      style: { backgroundColor:'#1a1a1a', border:'1px solid #444',
               padding:'6px 10px', borderRadius:'4px' }
    };
  }

  function initMap() {
    var el = document.getElementById('deck-hist-map');
    if (!el) return;
    if (typeof maplibregl === 'undefined' || typeof deck === 'undefined'
        || typeof deck.MapboxOverlay === 'undefined') {
      if (++tries < 100) { setTimeout(initMap, 100); }
      return;
    }
    mapInst = new maplibregl.Map({
      container: 'deck-hist-map',
      style: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
      center: [10, 50], zoom: 3.5, attributionControl: true
    });
    mapInst.addControl(new maplibregl.NavigationControl({visualizePitch:false}), 'top-right');
    deckInst = new deck.MapboxOverlay({
      interleaved: false,
      layers: buildLayers(pending),
      getTooltip: buildTooltip
    });
    mapInst.on('load', function() {
      mapInst.addControl(deckInst);
      if (pending) applyData(pending);
    });
    setTimeout(function() { if (mapInst) mapInst.resize(); }, 200);
  }

  function setupHandlers() {
    if (typeof Shiny === 'undefined' || !Shiny.addCustomMessageHandler) {
      setTimeout(setupHandlers, 60); return;
    }
    Shiny.addCustomMessageHandler('update_hist_map_data', function(msg) {
      applyData(msg);
    });
  }

  function boot() { initMap(); setupHandlers(); }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else { boot(); }
})();
"""


# ── UI ─────────────────────────────────────────────────────────────────────────

def hist_ui():
    return ui.nav_panel("Historic Data",
        ui.layout_columns(
            # Left: map + legend + charts
            ui.div(
                ui.output_ui("hist_map_label"),
                ui.tags.div(id="deck-hist-map",
                            style="width:100%;height:450px;border-radius:4px;"
                                  "overflow:hidden;position:relative;background:#0e1117;"),
                ui.tags.script(ui.HTML(_HIST_MAP_INIT_JS)),
                ui.output_ui("hist_eaqi_legend"),
                output_widget("hist_chart"),
                output_widget("hist_yoy_chart"),
            ),
            # Right: controls + stats
            ui.div(
                ui.layout_columns(
                    ui.h4("Historic Data"),
                    ui.input_select("hist_pollutant", None,
                                    {"PM10": "PM10", "PM2.5": "PM2.5"},
                                    selected="PM10"),
                    col_widths=(6, 6),
                ),
                ui.layout_columns(
                    ui.input_date("hist_start", "Start Bound",
                                  value="2013-01-01",
                                  min="2012-12-31", max="2025-01-01"),
                    ui.input_date("hist_end", "End Bound",
                                  value="2025-01-01",
                                  min="2012-12-31", max="2025-01-01"),
                    col_widths=(6, 6),
                ),
                ui.input_slider("hist_date", "Scrub through dates",
                                min=_dt.date(2013, 1, 1),
                                max=_dt.date(2025, 1, 1),
                                value=_dt.date(2013, 1, 1),
                                step=_dt.timedelta(days=1),
                                time_format="%Y-%m-%d"),
                ui.layout_columns(
                    ui.input_action_button("hist_play", "▶️ Play",
                                           class_="btn-primary"),
                    ui.input_action_button("hist_stop", "⏹️ Stop"),
                    col_widths=(4, 8),
                ),
                ui.input_slider("hist_speed", "Animation Speed",
                                min=1, max=5, value=3, step=1, ticks=True),
                ui.input_action_button("hist_jump_peak", "🎯 Jump to Peak",
                                       class_="btn-primary",
                                       style="width:100%;margin-bottom:8px"),
                ui.hr(),
                ui.output_ui("hist_stats_box"),
                ui.hr(),
                ui.h5("Ask AI about peaks"),
                ui.input_text("hist_llm_query", "Curious about a peak? Ask here:",
                              placeholder="What could cause this peak?"),
                ui.input_action_button("hist_ask", "Ask AI", class_="btn-primary"),
                ui.output_text_verbatim("hist_llm_response"),
            ),
            col_widths=(8, 4),
        ),
    )


# ── Server ─────────────────────────────────────────────────────────────────────

def hist_server(input, output, session):
    hist_playing   = reactive.Value(False)
    hist_anim_date = reactive.Value(None)
    hist_llm_resp  = reactive.Value("")

    # Speed map: slider value 1-5 → seconds per frame
    _SPEED_MAP = {1: 1.2, 2: 0.8, 3: 0.5, 4: 0.25, 5: 0.1}

    @reactive.calc
    def hist_table():
        return "airquality_5" if input.hist_pollutant() == "PM10" else "airquality_6001"

    @reactive.calc
    def hist_master():
        with ui.Progress(min=0, max=1) as p:
            p.set(message="Loading station list…")
            return hist_get_master_stations(
                hist_table(), str(input.hist_start()), str(input.hist_end()))

    @reactive.calc
    def hist_averages():
        with ui.Progress(min=0, max=1) as p:
            p.set(message="Loading daily averages…")
            return hist_get_daily_averages(
                hist_table(), str(input.hist_start()), str(input.hist_end()))

    @reactive.calc
    def hist_yoy():
        return hist_get_yoy_data(
            hist_table(), str(input.hist_start()), str(input.hist_end()))

    # ── Bug fix 1 & 2: use hist_playing() not hist_playing.get() ──────────────
    # Using .get() breaks reactive dependency — the effect never re-fires when
    # hist_playing changes, so Stop has no effect and speed is ignored.

    @reactive.effect
    @reactive.event(input.hist_play)
    def _hist_play():
        hist_playing.set(True)
        hist_anim_date.set(input.hist_date())

    @reactive.effect
    @reactive.event(input.hist_stop)
    def _hist_stop():
        hist_playing.set(False)

    @reactive.effect
    def _hist_advance():
        # Reading hist_playing() (callable form) registers a reactive dependency.
        # When hist_playing becomes False the effect re-fires and returns early,
        # effectively stopping the animation.
        if not hist_playing():
            return
        speed = _SPEED_MAP.get(input.hist_speed(), 0.5)
        reactive.invalidate_later(speed)
        # Use isolate() for the mutable animation date so advancing it doesn't
        # cause an infinite re-render loop.
        with reactive.isolate():
            cur = hist_anim_date() if hist_anim_date() is not None else input.hist_date()
        nxt = cur + _dt.timedelta(days=1)
        if nxt > input.hist_end():
            hist_playing.set(False)
        else:
            hist_anim_date.set(nxt)
            ui.update_slider("hist_date", value=nxt)

    # Jump to peak button
    @reactive.effect
    @reactive.event(input.hist_jump_peak)
    def _hist_jump():
        df = hist_averages()
        if df.empty:
            return
        peak_date = df.loc[df["AvgValue"].idxmax(), "Date"].date()
        hist_anim_date.set(None)
        ui.update_slider("hist_date", value=peak_date)

    # ── Bug fix 3: use hist_playing() / hist_anim_date() (callable form) ──────
    # Using .get() in a @reactive.calc means the calc doesn't re-run when those
    # values change, so the date label stays stale during animation.

    @reactive.calc
    def hist_current_date():
        if hist_playing() and hist_anim_date() is not None:
            return hist_anim_date()
        return input.hist_date()

    @output
    @render.ui
    def hist_map_label():
        state = "Animating" if hist_playing() else "Viewing"
        return ui.HTML(f"<b>{state}:</b> {hist_current_date()}")

    @output
    @render.ui
    def hist_eaqi_legend():
        return ui.HTML(build_hist_eaqi_legend(input.hist_pollutant()))

    @reactive.effect
    async def _push_hist_map():
        df = hist_get_map_data(str(hist_current_date()), hist_table(), hist_master())
        payload = build_hist_map_payload(df)
        await session.send_custom_message("update_hist_map_data", payload)

    @output
    @render_altair
    def hist_chart():
        return build_hist_avg_chart(hist_averages(), hist_current_date())

    @output
    @render_altair
    def hist_yoy_chart():
        return build_yoy_chart(hist_yoy(), input.hist_pollutant())

    @output
    @render.ui
    def hist_stats_box():
        df = hist_averages()
        if df.empty:
            return ui.tags.div()
        today_dt  = pd.to_datetime(str(hist_current_date()))
        today_row = df[df["Date"] == today_dt]
        today_val = f"{today_row['AvgValue'].iloc[0]:.1f} µg/m³" if not today_row.empty else "—"
        peak_row  = df.loc[df["AvgValue"].idxmax()]
        peak_val  = f"{peak_row['AvgValue']:.1f} µg/m³"
        peak_date = peak_row["Date"].strftime("%d %b %Y")
        period_mean = f"{df['AvgValue'].mean():.1f} µg/m³"
        poll = input.hist_pollutant()
        today_lbl = get_aqi_label(
            today_row["AvgValue"].iloc[0] if not today_row.empty else None, poll) or "—"
        colour = EAQI_COLOURS.get(today_lbl, "#888")
        return ui.HTML(f"""
<div style="background:#1a1a1a;border-radius:6px;padding:12px;font-size:13px;
            line-height:1.9;border:1px solid #333;">
  <b>📊 Period Statistics</b><br>
  <span style="color:#aaa">Today's mean:</span>
    <b style="color:{colour}">{today_val}</b>
    <span style="color:{colour};font-size:11px"> {today_lbl}</span><br>
  <span style="color:#aaa">Period mean:</span> <b>{period_mean}</b><br>
  <span style="color:#aaa">Period peak:</span>
    <b style="color:#EE7733">{peak_val}</b>
    <span style="color:#aaa;font-size:11px"> on {peak_date}</span>
</div>""")

    @reactive.effect
    @reactive.event(input.hist_ask)
    def _hist_ask():
        resp = ask_llm_about_peak(str(hist_current_date()), input.hist_pollutant())
        hist_llm_resp.set(resp)

    @output
    @render.text
    def hist_llm_response():
        return hist_llm_resp()
