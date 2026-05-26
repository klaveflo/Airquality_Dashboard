"""
live_tab.py — Live Data tab UI and server logic.

Exposes:
  live_ui()                          → NavPanel / ui fragment
  live_server(input, output, session) → registers all outputs and effects
"""

from datetime import datetime, timedelta

import pandas as pd
import altair as alt

from shiny import ui, render, reactive, req
from shinywidgets import output_widget, render_altair

from shared import (
    COUNTRIES, POLLUTANTS, EAQI_THRESHOLDS, EAQI_LABELS, EAQI_COLOURS,
    get_aqi_label, render_legend,
    get_all_station_data_cached, build_hour_display, build_map_payload,
)


# ── Client-side deck.gl + maplibre-gl bootstrap (live map) ────────────────────

_MAP_INIT_JS = r"""
(function () {
  var deckInstance = null;
  var mapInstance  = null;
  var pendingData  = null;
  var selectedStations = [];
  var initTries = 0;

  function fillColor(d) {
    return [d.color_r || 128, d.color_g || 128, d.color_b || 128, d.color_a || 220];
  }

  function onStationClick(info) {
    if (!info || !info.object || !info.object.station_name) return;
    var name = info.object.station_name;
    if (typeof Shiny !== 'undefined' && Shiny.setInputValue) {
      Shiny.setInputValue('map_click', {name: name, nonce: Date.now()},
                          {priority: 'event'});
    }
  }

  function buildLayers(data) {
    if (!data) data = {urban: [], suburban: [], rural: []};
    var layers = [];

    layers.push(new deck.ScatterplotLayer({
      id: 'urban-layer',
      data: data.urban || [],
      getPosition: function (d) { return [d.lon, d.lat]; },
      getRadius: 7000, radiusMinPixels: 5, radiusMaxPixels: 16,
      getFillColor: fillColor, pickable: true, onClick: onStationClick,
      updateTriggers: {getFillColor: [data.stamp || 0]}
    }));

    layers.push(new deck.ScatterplotLayer({
      id: 'suburban-layer',
      data: data.suburban || [],
      getPosition: function (d) { return [d.lon, d.lat]; },
      getRadius: 6000, radiusMinPixels: 5, radiusMaxPixels: 16,
      getFillColor: fillColor, pickable: true, onClick: onStationClick,
      updateTriggers: {getFillColor: [data.stamp || 0]}
    }));
    layers.push(new deck.ScatterplotLayer({
      id: 'suburban-ring',
      data: data.suburban || [],
      getPosition: function (d) { return [d.lon, d.lat]; },
      getRadius: 6000, radiusMinPixels: 5, radiusMaxPixels: 16,
      stroked: true, filled: false,
      getLineColor: [255, 255, 255, 200], lineWidthMinPixels: 2, pickable: false
    }));

    // Rural: hollow — transparent fill (still pickable by radius), EAQI colour ring
    layers.push(new deck.ScatterplotLayer({
      id: 'rural-layer',
      data: data.rural || [],
      getPosition: function (d) { return [d.lon, d.lat]; },
      getRadius: 5000, radiusMinPixels: 5, radiusMaxPixels: 16,
      getFillColor: function (d) { return [d.color_r || 128, d.color_g || 128, d.color_b || 128, 0]; },
      pickable: true, onClick: onStationClick,
      updateTriggers: {getFillColor: [data.stamp || 0]}
    }));
    layers.push(new deck.ScatterplotLayer({
      id: 'rural-ring',
      data: data.rural || [],
      getPosition: function (d) { return [d.lon, d.lat]; },
      getRadius: 5000, radiusMinPixels: 5, radiusMaxPixels: 16,
      stroked: true, filled: false,
      getLineColor: function (d) { return [d.color_r || 128, d.color_g || 128, d.color_b || 128, 220]; },
      lineWidthMinPixels: 3, pickable: false,
      updateTriggers: {getLineColor: [data.stamp || 0]}
    }));

    if (selectedStations.length) {
      var pool = [].concat(data.urban || [], data.suburban || [], data.rural || []);
      var hits = pool.filter(function (d) {
        return selectedStations.indexOf(d.station_name) !== -1;
      });
      layers.push(new deck.ScatterplotLayer({
        id: 'selection-highlight',
        data: hits,
        getPosition: function (d) { return [d.lon, d.lat]; },
        getRadius: 9000, radiusMinPixels: 10, radiusMaxPixels: 22,
        stroked: true, filled: false,
        getLineColor: [255, 215, 0, 255], lineWidthMinPixels: 3, pickable: false
      }));
    }
    return layers;
  }

  function applyData(data) {
    if (data) pendingData = data;
    if (deckInstance && pendingData) {
      deckInstance.setProps({layers: buildLayers(pendingData)});
    }
  }

  function buildTooltip(info) {
    if (!info || !info.object) return null;
    var o = info.object;
    return {
      html: '<div style="font-family:sans-serif;font-size:12px">'
          + '<b>' + (o.station_name || '—') + '</b><br>'
          + (o.aqi_label || '—') + '<br>'
          + 'Value: ' + (o.value_str || 'N/A') + ' µg/m³</div>',
      style: {backgroundColor:'#1a1a1a', color:'#fafafa',
              border:'1px solid #444', padding:'6px 10px', borderRadius:'4px'}
    };
  }

  function initMap() {
    var el = document.getElementById('deck-map');
    if (!el) { return; }
    if (typeof maplibregl === 'undefined' || typeof deck === 'undefined'
        || typeof deck.MapboxOverlay === 'undefined') {
      if (++initTries < 100) { setTimeout(initMap, 100); }
      else { console.error('[deck-map] deck.gl / maplibre-gl failed to load'); }
      return;
    }
    mapInstance = new maplibregl.Map({
      container: 'deck-map',
      style: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
      center: [10, 50], zoom: 3.5, attributionControl: true
    });
    mapInstance.addControl(new maplibregl.NavigationControl({visualizePitch: false}), 'top-right');
    deckInstance = new deck.MapboxOverlay({
      interleaved: false, layers: buildLayers(pendingData), getTooltip: buildTooltip
    });
    mapInstance.on('load', function () {
      mapInstance.addControl(deckInstance);
      if (pendingData) applyData(pendingData);
    });
    setTimeout(function () { if (mapInstance) mapInstance.resize(); }, 200);
  }

  function setupShinyHandlers() {
    if (typeof Shiny === 'undefined' || !Shiny.addCustomMessageHandler) {
      setTimeout(setupShinyHandlers, 60); return;
    }
    Shiny.addCustomMessageHandler('update_map_data', function (msg) {
      applyData(msg);
    });
    Shiny.addCustomMessageHandler('update_selected_stations', function (msg) {
      selectedStations = (msg && msg.stations) ? msg.stations : [];
      if (deckInstance && pendingData) {
        deckInstance.setProps({layers: buildLayers(pendingData)});
      }
    });
  }

  function boot() { initMap(); setupShinyHandlers(); }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else { boot(); }
})();
"""


# ── UI ─────────────────────────────────────────────────────────────────────────

def live_ui():
    return ui.nav_panel("Live Data",
        ui.layout_columns(
            ui.div(
                ui.output_ui("map_label"),
                ui.output_ui("map_status"),
                ui.tags.div(id="deck-map"),
                ui.tags.script(ui.HTML(_MAP_INIT_JS)),
                ui.output_ui("map_legend"),
                ui.output_ui("comparison_section"),
            ),
            ui.div(
                ui.layout_columns(
                    ui.h4("Live Data"),
                    ui.input_select("pollutant", None, choices=POLLUTANTS, selected="PM10"),
                    col_widths=(6, 6),
                ),
                ui.input_select("country", "Country", choices=COUNTRIES, selected="CH",
                                width="100%"),
                ui.input_action_button("load", "Load / Refresh", class_="btn-primary"),
                ui.output_ui("hour_slider_ui"),
                ui.layout_columns(
                    ui.input_action_button("play", "▶️ Play", class_="btn-primary"),
                    ui.input_action_button("stop", "⏹️ Stop"),
                    col_widths=(4, 8),
                ),
                ui.hr(),
                ui.output_ui("station_count"),
                ui.output_ui("selected_list"),
                ui.hr(),
                ui.tags.b("Or pick stations manually:"),
                ui.output_ui("manual_picker_ui"),
            ),
            col_widths=(8, 4),
        ),
    )


# ── Server ─────────────────────────────────────────────────────────────────────

def live_server(input, output, session):
    selected_stations = reactive.Value([])
    playing           = reactive.Value(False)
    anim_idx          = reactive.Value(0)

    @reactive.calc
    def fetched():
        country   = input.country()
        pollutant = input.pollutant()
        load_n    = input.load()
        with ui.Progress(min=1, max=1) as p:
            p.set(message=f"Fetching live data for {country} / {pollutant}…")
            return get_all_station_data_cached(country, pollutant, load_n)

    @reactive.calc
    def df_meta():
        return fetched()[0]

    @reactive.calc
    def df_all():
        return fetched()[1]

    @reactive.calc
    def all_hours():
        d = df_all()
        if d.empty:
            return []
        return [h.to_pydatetime() for h in sorted(d["Start"].dt.floor("h").unique())]

    @reactive.effect
    @reactive.event(input.country, input.pollutant, input.load)
    def _reset_on_change():
        selected_stations.set([])
        playing.set(False)
        anim_idx.set(0)

    @reactive.effect
    @reactive.event(input.play)
    def _start_playing():
        hours = all_hours()
        if not hours:
            return
        try:
            cur = input.hour_slider()
        except Exception:
            cur = None
        try:
            start_idx = hours.index(cur) if cur in hours else 0
        except Exception:
            start_idx = 0
        anim_idx.set(start_idx)
        playing.set(True)

    @reactive.effect
    @reactive.event(input.stop)
    def _stop_playing():
        playing.set(False)

    @reactive.effect
    def _animate():
        if not playing():
            return
        reactive.invalidate_later(0.5)
        hours = all_hours()
        if not hours:
            playing.set(False)
            return
        with reactive.isolate():
            cur = anim_idx()
        nxt = cur + 1
        if nxt >= len(hours):
            playing.set(False)
            return
        anim_idx.set(nxt)
        ui.update_slider("hour_slider", value=hours[nxt])

    @output
    @render.ui
    def hour_slider_ui():
        hours = all_hours()
        if len(hours) > 1:
            return ui.input_slider(
                "hour_slider", "Scrub through time",
                min=hours[0], max=hours[-1], value=hours[-1],
                step=timedelta(hours=1), time_format="%d %b, %H:%M",
            )
        if len(hours) == 1:
            return ui.tags.div(
                ui.tags.b("Time: "),
                hours[0].strftime("%d %b %Y, %H:%M"),
                ui.tags.div(
                    ui.input_slider("hour_slider", "", min=hours[0], max=hours[0],
                                    value=hours[0], step=timedelta(hours=1)),
                    style="display:none;",
                ),
            )
        now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        return ui.tags.div(
            ui.input_slider("hour_slider", "", min=now, max=now, value=now,
                            step=timedelta(hours=1)),
            style="display:none;",
        )

    @reactive.calc
    def display_hour():
        hours = all_hours()
        if not hours:
            return None
        if playing():
            idx = anim_idx() % len(hours)
            return hours[idx]
        try:
            return input.hour_slider()
        except Exception:
            return hours[-1]

    @reactive.calc
    def df_display():
        return build_hour_display(display_hour(), df_meta(), df_all(), input.pollutant())

    @reactive.effect
    async def _push_map_data():
        df = df_display()
        payload = build_map_payload(df)
        payload["stamp"] = datetime.utcnow().timestamp()
        await session.send_custom_message("update_map_data", payload)

    @reactive.effect
    async def _push_selection():
        sel = list(selected_stations())
        await session.send_custom_message("update_selected_stations", {"stations": sel})

    @output
    @render.ui
    def map_label():
        if df_meta().empty:
            return ui.tags.div()
        hours = all_hours()
        h = display_hour()
        if playing() and hours:
            idx = anim_idx() % len(hours)
            return ui.tags.div(ui.HTML(
                f"<b>Animating:</b> {h.strftime('%d %b %Y, %H:%M')} "
                f"<i>(frame {idx + 1} / {len(hours)})</i>"
            ))
        label = h.strftime("%d %b %Y, %H:%M") if h else "—"
        return ui.tags.div(ui.HTML(f"<b>Viewing:</b> {label}"))

    @output
    @render.ui
    def map_status():
        meta = df_meta()
        if meta.empty:
            return ui.tags.div(
                "No data found for this country/pollutant combination. "
                "Try another selection.",
                style="padding:10px;background:#332b00;border:1px solid #8a7500;"
                      "border-radius:4px;color:#ffd966;margin:6px 0;",
            )
        if meta["lat"].isna().all():
            return ui.tags.div(
                "Station coordinates could not be resolved from EEA metadata. "
                "Use the manual picker on the right to compare stations.",
                style="padding:10px;background:#1f2a44;border:1px solid #3a4a7a;"
                      "border-radius:4px;color:#9cc1ff;margin:6px 0;",
            )
        return ui.tags.div()

    @output
    @render.ui
    def map_legend():
        if df_meta().empty or df_meta()["lat"].isna().all():
            return ui.tags.div()
        return ui.HTML(render_legend(input.pollutant()))

    @output
    @render.ui
    def station_count():
        return ui.HTML(f"<b>Stations found:</b> {len(df_meta())}")

    @output
    @render.ui
    def selected_list():
        sel = selected_stations()
        if sel:
            rows = "".join(f"<div style='padding-left:8px'>• {s}</div>" for s in sel)
            return ui.tags.div(
                ui.HTML(f"<b>Selected for comparison:</b>{rows}"),
                ui.input_action_button("clear_sel", "Clear selection"),
            )
        return ui.tags.div(
            "Click up to 2 stations on the map (or pick below) to compare them.",
            style="padding:10px;background:#1f2a44;border:1px solid #3a4a7a;"
                  "border-radius:4px;color:#9cc1ff;font-size:13px;margin-top:6px;",
        )

    @reactive.effect
    @reactive.event(input.clear_sel)
    def _clear_selection():
        selected_stations.set([])

    @output
    @render.ui
    def manual_picker_ui():
        meta = df_meta()
        opts = sorted(meta["station_name"].dropna().tolist()) if not meta.empty else []
        with reactive.isolate():
            current = [s for s in selected_stations() if s in opts]
        return ui.input_selectize(
            "manual_pick", None,
            choices=opts,
            selected=current,
            multiple=True,
            options={"maxItems": 2, "placeholder": "Select up to 2 stations"},
        )

    @reactive.effect
    @reactive.event(input.manual_pick, ignore_init=True)
    def _sync_from_manual_pick():
        val = input.manual_pick() or ()
        new_list = list(val)[:2]
        with reactive.isolate():
            if new_list != selected_stations():
                selected_stations.set(new_list)

    @reactive.effect
    @reactive.event(input.map_click)
    def _on_map_click():
        if playing():
            return
        click = input.map_click()
        if not click:
            return
        name = click.get("name") if isinstance(click, dict) else click
        if not name:
            return
        with reactive.isolate():
            current = list(selected_stations())
        if name in current:
            return
        if len(current) < 2:
            current.append(name)
        else:
            current = [current[1], name]
        selected_stations.set(current)

    @reactive.effect
    def _push_selection_to_selectize():
        sel = selected_stations()
        with reactive.isolate():
            try:
                current = list(input.manual_pick() or ())
            except Exception:
                current = []
        if sorted(sel) != sorted(current):
            ui.update_selectize("manual_pick", selected=sel)

    @output
    @render.ui
    def comparison_section():
        if not selected_stations() or df_all().empty:
            return ui.tags.div()
        return ui.tags.div(
            ui.hr(),
            ui.tags.style("""
                #breakdown_chart { margin-bottom: 0 !important; }
                #breakdown_chart .widget-subarea { margin-bottom: 0 !important; }
                #summary_cards { margin-top: 4px !important; }
                #summary_cards > div { margin-top: 0 !important; }
            """),
            ui.layout_columns(
                output_widget("line_chart"),
                ui.div(
                    output_widget("breakdown_chart"),
                    ui.output_ui("summary_cards"),
                    style="display:flex;flex-direction:column;gap:4px;",
                ),
                col_widths=(7, 5),
            ),
        )

    @reactive.calc
    def df_sel():
        sel = selected_stations()
        d   = df_all()
        if not sel or d.empty:
            return pd.DataFrame()
        out = d[d["station_name"].isin(sel)].copy()
        if out.empty:
            return out
        pollutant = input.pollutant()
        out["aqi_label"] = out["Value"].apply(
            lambda val: get_aqi_label(val, pollutant) or "Unknown"
        )
        return out

    @output
    @render_altair
    def line_chart():
        sel_df = df_sel()
        req(not sel_df.empty)
        pollutant  = input.pollutant()
        thresholds = EAQI_THRESHOLDS.get(pollutant, EAQI_THRESHOLDS["PM10"])
        y_max = max(float(sel_df["Value"].max()) * 1.15, thresholds[1][0] + 1)

        band_data, prev = [], 0
        for upper, label, colour in thresholds:
            y2 = min(upper, y_max) if upper != float("inf") else y_max
            if prev >= y_max:
                break
            band_data.append({"y1": float(prev), "y2": float(y2),
                               "label": label, "colour": colour})
            prev = upper if upper != float("inf") else y_max

        bands_df      = pd.DataFrame(band_data)
        aqi_col_scale = alt.Scale(
            domain=[r["label"] for r in band_data],
            range=[r["colour"] for r in band_data],
        )
        bands = (
            alt.Chart(bands_df).mark_rect(opacity=0.15)
            .encode(
                y=alt.Y("y1:Q", scale=alt.Scale(domain=[0, y_max])),
                y2=alt.Y2("y2:Q"),
                color=alt.Color("label:N", scale=aqi_col_scale, legend=None),
            )
        )
        line = (
            alt.Chart(sel_df)
            .mark_line(point=alt.OverlayMarkDef(size=30))
            .encode(
                x=alt.X("Start:T", title="Date & Time"),
                y=alt.Y("Value:Q", title=f"{pollutant} (µg/m³)",
                        scale=alt.Scale(domain=[0, y_max])),
                color=alt.Color("station_name:N", title="Station"),
                tooltip=[
                    alt.Tooltip("Start:T", format="%d %b, %H:%M", title="Time"),
                    alt.Tooltip("Value:Q", format=".2f", title="µg/m³"),
                    alt.Tooltip("aqi_label:N", title="Air Quality"),
                    alt.Tooltip("station_name:N", title="Station"),
                ],
            )
            .properties(height=300, title="Hourly Readings with Air Quality Zones")
            .interactive()
        )
        return (alt.layer(bands, line)
                .resolve_scale(color="independent")
                .properties(width="container"))

    @output
    @render_altair
    def breakdown_chart():
        sel_df = df_sel()
        req(not sel_df.empty)
        order_map = {lbl: i for i, lbl in enumerate(EAQI_LABELS + ["Unknown"])}
        breakdown = (sel_df.groupby(["station_name", "aqi_label"])
                     .size().reset_index(name="hours"))
        breakdown["order_idx"] = breakdown["aqi_label"].map(order_map).fillna(99).astype(int)
        full_scale = alt.Scale(
            domain=EAQI_LABELS,
            range=[EAQI_COLOURS[lbl] for lbl in EAQI_LABELS],
        )
        return (
            alt.Chart(breakdown).mark_bar(height=28)
            .encode(
                x=alt.X("hours:Q", title="% of hours", stack="normalize",
                         axis=alt.Axis(format="%")),
                y=alt.Y("station_name:N", title=""),
                color=alt.Color("aqi_label:N", scale=full_scale,
                                sort=EAQI_LABELS, title="Air Quality"),
                order=alt.Order("order_idx:Q", sort="ascending"),
                tooltip=[
                    alt.Tooltip("station_name:N", title="Station"),
                    alt.Tooltip("aqi_label:N", title="Air Quality"),
                    alt.Tooltip("hours:Q", title="Hours"),
                ],
            )
            .properties(height=120, title="Air Quality Breakdown (share of hours)",
                        width="container")
        )

    @output
    @render.ui
    def summary_cards():
        sel_df = df_sel()
        if sel_df.empty:
            return ui.tags.div()
        pollutant = input.pollutant()
        cards = []
        for station in selected_stations():
            sdf = sel_df[sel_df["station_name"] == station]
            if sdf.empty:
                continue
            dominant = sdf["aqi_label"].value_counts().index[0]
            colour   = EAQI_COLOURS.get(dominant, "#888")
            worst    = sdf.loc[sdf["Value"].idxmax()]
            best     = sdf.loc[sdf["Value"].idxmin()]
            worst_lbl = get_aqi_label(worst["Value"], pollutant) or "—"
            best_lbl  = get_aqi_label(best["Value"], pollutant) or "—"
            cards.append(ui.HTML(f"""
<div style="border-left:4px solid {colour};padding:8px 14px;margin:8px 0;
            background:#1a1a1a;border-radius:4px;font-size:13px;line-height:1.8">
  <b>{station}</b><br>
  <span style="color:{colour}">● Mostly {dominant} this week</span><br>
  <span style="color:#bbb">⬆ Worst: {worst["Start"].strftime("%a %d %b, %H:%M")} —
    {worst["Value"]:.1f} µg/m³
    <span style="color:{EAQI_COLOURS.get(worst_lbl,'#888')}">({worst_lbl})</span></span><br>
  <span style="color:#bbb">⬇ Best:&nbsp; {best["Start"].strftime("%a %d %b, %H:%M")} —
    {best["Value"]:.1f} µg/m³
    <span style="color:{EAQI_COLOURS.get(best_lbl,'#888')}">({best_lbl})</span></span>
</div>
"""))
        return ui.tags.div(*cards)
