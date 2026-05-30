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
    get_aqi_label, EAQI_COLOURS, COUNTRIES, COUNTRY_VIEWS,
    hist_get_master_stations, hist_get_map_data, hist_get_daily_averages,
    hist_get_yoy_data, hist_available_countries, build_hist_map_payload,
    build_hist_avg_chart, build_yoy_chart, build_eaqi_legend,
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
    Shiny.addCustomMessageHandler('update_hist_map_view', function(msg) {
      if (mapInst) {
        mapInst.flyTo({ center: msg.center, zoom: msg.zoom, duration: 800 });
      }
    });
  }

  function boot() { initMap(); setupHandlers(); }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else { boot(); }
})();
"""


# ── Client-side Vega embed for the daily-average chart ───────────────────────
# We call vegaEmbed ourselves so we hold result.view directly — no DOM walking,
# no patching.  Two Shiny message channels:
#   update_hist_avg_spec  → full Vega-Lite spec dict; re-embeds the chart
#   update_hist_dot_date  → {date_str} only; moves the dot via signal, no re-embed

_HIST_CHART_JS = r"""
(function () {
  var _view = null;

  // ── Chart embed ──────────────────────────────────────────────────────────────
  function doEmbed(spec) {
    var container = document.getElementById('hist_avg_chart_container');
    if (!container) { setTimeout(function () { doEmbed(spec); }, 100); return; }
    if (typeof vegaEmbed === 'undefined') { setTimeout(function () { doEmbed(spec); }, 100); return; }
    vegaEmbed(container, spec, { actions: true, renderer: 'canvas' })
      .then(function (result) {
        _view = result.view;
        // Resize to fill the container immediately after embed.
        _view.resize().run();
        // Click-to-jump via Vega-Lite's own selection system.  Vega internally
        // distinguishes clicks from pan/drag so no custom mouse handling needed.
        // The signal listener is re-registered on every re-embed so it always
        // binds to the current view instance.
        // The selection uses fields=["DateStr"] so value carries the
        // pre-formatted "YYYY-MM-DD" string — no timestamp conversion needed.
        _view.addSignalListener('date_click', function (name, value) {
          console.log('[hist_chart] date_click signal:', name, JSON.stringify(value));
          if (!value || typeof value !== 'object') return;
          var dateStr = null;
          // Vega-Lite 5: {vlPoint: {or: [{DateStr: "YYYY-MM-DD"}]}}
          if (value.vlPoint && value.vlPoint.or && value.vlPoint.or.length > 0) {
            dateStr = value.vlPoint.or[0].DateStr;
          } else if (typeof value.DateStr === 'string') {
            // Older compilation: {DateStr: "YYYY-MM-DD"}
            dateStr = value.DateStr;
          }
          if (!dateStr) return;
          if (typeof Shiny !== 'undefined') {
            Shiny.setInputValue('hist_chart_click_date', dateStr, {priority: 'event'});
          }
        });
      })
      .catch(function (err) { console.error('hist_chart embed error:', err); });
  }

  // ── Dot position update (signal only, no re-embed) ───────────────────────────
  function applyDate(dateStr, tries) {
    if (_view && typeof _view.signal === 'function') {
      try { _view.signal('histCurDate', dateStr).run(); return; }
      catch (e) { _view = null; }
    }
    if (tries < 20) setTimeout(function () { applyDate(dateStr, tries + 1); }, 100);
  }

  // ── Bootstrap ────────────────────────────────────────────────────────────────
  function setup() {
    if (typeof Shiny === 'undefined' || !Shiny.addCustomMessageHandler) {
      setTimeout(setup, 60); return;
    }
    Shiny.addCustomMessageHandler('update_hist_avg_spec', function (msg) {
      doEmbed(msg);
    });
    Shiny.addCustomMessageHandler('update_hist_dot_date', function (msg) {
      applyDate(msg.date_str, 0);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', setup);
  } else { setup(); }
})();
"""


# ── UI ─────────────────────────────────────────────────────────────────────────

def hist_ui():
    return ui.nav_panel("Historic Data",
        # Vega stack - must load before our JS runs vegaEmbed
        ui.tags.script(src="https://cdn.jsdelivr.net/npm/vega@5"),
        ui.tags.script(src="https://cdn.jsdelivr.net/npm/vega-lite@5"),
        ui.tags.script(src="https://cdn.jsdelivr.net/npm/vega-embed@6"),
        ui.tags.script(ui.HTML(_HIST_CHART_JS)),
        ui.layout_columns(
            # Left: map + legend + charts
            ui.div(
                ui.output_ui("hist_map_label"),
                ui.tags.div(id="deck-hist-map",
                            style="width:100%;height:450px;border-radius:4px;"
                                  "overflow:hidden;position:relative;background:#0e1117;"),
                ui.tags.script(ui.HTML(_HIST_MAP_INIT_JS)),
                ui.output_ui("hist_eaqi_legend"),
                ui.div(id="hist_avg_chart_container", style="width:100%;cursor:crosshair;"),
                output_widget("hist_yoy_chart"),
            ),
            # Right: controls + stats
            ui.div(
                ui.h4("Historic Data"),
                ui.accordion(
                    ui.accordion_panel(
                        "About this view",
                        ui.tags.p(
                            "Take a step back and look at the bigger picture. This tab lets "
                            "you explore years of daily air quality data from the EEA archive. "
                            "Select a pollutant, country, and date range to see how average "
                            "pollution levels have shifted over time. Hit Play to animate through "
                            "the timeline day by day and watch pollution patterns move across the "
                            "map, or click directly on the chart to jump to a date that catches your eye.",
                            style="font-size:14px; line-height:1.6;"
                        ),
                        ui.tags.p(
                            "The Year-over-Year comparison below reveals seasonal rhythms and long-term trends, "
                            "you might notice winter peaks from heating emissions or gradual improvements from "
                            "policy changes. Curious about a sudden spike? Use 'Jump to Peak' to find the worst "
                            "day in your selected range, then ask the AI assistant for possible explanations.",
                            style="font-size:14px; line-height:1.6;"
                        ),
                    ),
                    open=False,
                ),
                ui.layout_columns(
                    ui.input_select("hist_pollutant", "Pollutant",
                                    {"PM10": "PM10", "PM2.5": "PM2.5"},
                                    selected="PM10"),
                    ui.input_select("hist_country", "Country",
                                    choices={"ALL": "All Europe", **{
                                        k: v for k, v in COUNTRIES.items()
                                        if k in hist_available_countries()
                                    }},
                                    selected="ALL"),
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
                                time_format="%Y-%m-%d",
                                width="100%"),
                ui.input_date("hist_date_picker", "Jump to date:",
                              value=_dt.date(2013, 1, 1),
                              min=_dt.date(2013, 1, 1),
                              max=_dt.date(2025, 1, 1)),
                ui.layout_columns(
                    ui.input_action_button("hist_play", "Play",
                                           class_="btn-primary"),
                    ui.input_action_button("hist_stop", "Stop", class_="btn-outline-secondary"),
                    col_widths=(6, 6),
                ),
                ui.input_slider("hist_speed", "Animation Speed",
                                min=1, max=5, value=3, step=1, ticks=True),
                ui.input_action_button("hist_jump_peak", "Jump to Peak",
                                       style="width:100%;margin-bottom:8px;"
                                             "background-color:#EE7733;border-color:#EE7733;"
                                             "color:#fff;"),
                ui.hr(),
                ui.output_ui("hist_stats_box"),
                ui.hr(),
                ui.input_text("hist_llm_query", "Ask AI about the data:",
                              placeholder="Leave empty to ask about this date, or type your own question"),
                ui.input_action_button("hist_ask", "Ask AI", class_="btn-primary"),
                ui.output_ui("hist_llm_response"),
            ),
            col_widths=(8, 4),
        ),
    )


# ── Server ─────────────────────────────────────────────────────────────────────

def hist_server(input, output, session):
    hist_playing             = reactive.Value(False)
    hist_anim_date           = reactive.Value(None)
    hist_llm_resp            = reactive.Value("")
    click_in_flight          = reactive.Value(False)
    _slider_update_in_flight = reactive.Value(False)

    # Speed map: slider value 1-5 → seconds per frame
    _SPEED_MAP = {1: 1.5, 2: 0.8, 3: 0.4, 4: 0.15, 5: 0.05}

    @reactive.calc
    def hist_table():
        return "airquality_5" if input.hist_pollutant() == "PM10" else "airquality_6001"

    @reactive.calc
    def hist_master():
        with ui.Progress(min=0, max=1) as p:
            p.set(message="Loading station list…")
            return hist_get_master_stations(
                hist_table(), str(input.hist_start()), str(input.hist_end()),
                input.hist_country())

    @reactive.calc
    def hist_averages():
        with ui.Progress(min=0, max=1) as p:
            p.set(message="Loading daily averages…")
            return hist_get_daily_averages(
                hist_table(), str(input.hist_start()), str(input.hist_end()),
                input.hist_country())

    @reactive.calc
    def hist_yoy():
        return hist_get_yoy_data(
            hist_table(), str(input.hist_start()), str(input.hist_end()),
            input.hist_country())

    @reactive.effect
    @reactive.event(input.hist_country)
    async def _hist_fly_to_country():
        view = COUNTRY_VIEWS.get(input.hist_country(), COUNTRY_VIEWS["ALL"])
        await session.send_custom_message("update_hist_map_view", view)

    @reactive.effect
    @reactive.event(input.hist_play)
    def _hist_play():
        hist_playing.set(True)
        hist_anim_date.set(input.hist_date())

    @reactive.effect
    @reactive.event(input.hist_stop)
    def _hist_stop():
        # Read last animated date before flipping the flag so hist_current_date()
        # stays on hist_anim_date (no jump) until the slider round-trip completes.
        with reactive.isolate():
            last = hist_anim_date()
        hist_playing.set(False)
        if last is not None:
            _slider_update_in_flight.set(True)
            ui.update_slider("hist_date", value=last)

    @reactive.effect
    def _hist_advance():
        if not hist_playing():
            return
        speed = _SPEED_MAP.get(input.hist_speed(), 0.5)
        reactive.invalidate_later(speed)
        with reactive.isolate():
            cur = hist_anim_date() if hist_anim_date() is not None else input.hist_date()
        nxt = cur + _dt.timedelta(days=1)
        if nxt > input.hist_end():
            hist_playing.set(False)
            _slider_update_in_flight.set(True)
            ui.update_slider("hist_date", value=cur)
        else:
            hist_anim_date.set(nxt)
            _slider_update_in_flight.set(True)
            ui.update_slider("hist_date", value=nxt)

    # Sync hist_anim_date from the slider when the user manually scrubs (not playing).
    # This effect only fires on user interaction because the animation loop never
    # calls ui.update_slider during playback.
    @reactive.effect
    @reactive.event(input.hist_date)
    def _on_slider_change():
        with reactive.isolate():
            if _slider_update_in_flight():
                _slider_update_in_flight.set(False)
                return
            if not hist_playing():
                hist_anim_date.set(input.hist_date())

    # Jump to peak button
    @reactive.effect
    @reactive.event(input.hist_jump_peak)
    def _hist_jump():
        df = hist_averages()
        if df.empty:
            return
        peak_date = df.loc[df["AvgValue"].idxmax(), "Date"].date()
        hist_anim_date.set(peak_date)
        _slider_update_in_flight.set(True)
        ui.update_slider("hist_date", value=peak_date)

    # Chart click → jump to clicked date (same behaviour as slider scrub)
    @reactive.effect
    @reactive.event(input.hist_chart_click_date)
    def _on_chart_click():
        raw = input.hist_chart_click_date()
        if not raw:
            return
        try:
            clicked = _dt.date.fromisoformat(str(raw))
        except (ValueError, TypeError):
            return
        with reactive.isolate():
            start = input.hist_start()
            end   = input.hist_end()
        clicked = max(start, min(end, clicked))
        hist_playing.set(False)
        click_in_flight.set(True)
        hist_anim_date.set(clicked)
        _slider_update_in_flight.set(True)
        ui.update_slider("hist_date", value=clicked)

    # Manual date picker → jump to picked date
    @reactive.effect
    @reactive.event(input.hist_date_picker)
    def _on_date_picker():
        picked = input.hist_date_picker()
        if picked is None:
            return
        hist_playing.set(False)
        hist_anim_date.set(picked)
        _slider_update_in_flight.set(True)
        ui.update_slider("hist_date", value=picked)

    # Keep slider and date-picker min/max in sync with the date-range pickers
    @reactive.effect
    def update_slider_bounds():
        ui.update_slider("hist_date",
                         min=input.hist_start(),
                         max=input.hist_end())
        ui.update_date("hist_date_picker",
                       min=input.hist_start(),
                       max=input.hist_end())

    @reactive.calc
    def hist_current_date():
        # hist_anim_date is authoritative whenever it's set: during animation AND
        # in the brief window after Stop while the slider round-trip completes.
        # Only falls back to the slider input when hist_anim_date has never been set
        # (app start, before the first Play or manual scrub).
        anim = hist_anim_date()
        if anim is not None:
            return anim
        return input.hist_date()

    @output
    @render.ui
    def hist_map_label():
        state = "Animating" if hist_playing() else "Viewing"
        d = hist_current_date()
        label = d.strftime("%d %b %Y") if hasattr(d, "strftime") else str(d)
        return ui.HTML(f"<b>{state}:</b> {label}")

    @output
    @render.ui
    def hist_eaqi_legend():
        return ui.HTML(build_eaqi_legend(input.hist_pollutant()))

    @reactive.effect
    async def _push_hist_map():
        cur = hist_current_date()
        df = hist_get_map_data(str(cur), hist_table(), hist_master())
        payload = build_hist_map_payload(df)
        await session.send_custom_message("update_hist_map_data", payload)

    @reactive.effect
    async def _push_hist_chart_spec():
        # Fires only when the underlying data changes (pollutant or date range).
        # Does NOT read hist_current_date() so date selections never cause a
        # re-embed — zoom/pan state is preserved across all date changes.
        # The dot is baked at the first data row as a placeholder; _send_dot_update
        # fires immediately after and moves it to the correct position via signal.
        df = hist_averages()
        with reactive.isolate():
            default_date = df.iloc[0]["Date"].date() if not df.empty else input.hist_start()
        spec = build_hist_avg_chart(df, default_date, input.hist_pollutant())
        await session.send_custom_message("update_hist_avg_spec", spec)

    @reactive.effect
    async def _send_dot_update():
        # Fires on every hist_current_date() change — animation frames, slider
        # scrubs, chart clicks, date picker, jump-to-peak — all go through the
        # lightweight Vega signal path.  No re-embed, zoom/pan preserved.
        # Skip one update after a chart click: Vega's own selection already
        # placed the dot correctly, so the server round-trip would double-jump.
        date_str = str(hist_current_date())
        if click_in_flight():
            click_in_flight.set(False)
            return
        await session.send_custom_message("update_hist_dot_date", {"date_str": date_str})

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
<div class="stats-box">
  <b>Period Statistics</b><br>
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
        resp = ask_llm_about_peak(
            str(hist_current_date()),
            input.hist_pollutant(),
            country=input.hist_country(),
            user_query=input.hist_llm_query(),
        )
        hist_llm_resp.set(resp)

    @output
    @render.ui
    def hist_llm_response():
        txt = hist_llm_resp()
        if not txt:
            return ui.tags.div()
        return ui.HTML(
            f'<div style="background:rgba(68,119,170,0.12);color:inherit;'
            f'padding:12px;border-radius:6px;border:1px solid #4477AA;'
            f'font-size:13px;line-height:1.6;white-space:pre-wrap;margin-top:8px;">'
            f'{txt}</div>'
        )
