"""
Live Air Quality Dashboard — Shiny for Python port of the Streamlit original.

Key implementation notes for this version:

* The map is a **persistent** deck.gl instance rendered on top of a
  maplibre-gl basemap (Carto dark-matter, no API key needed). It is mounted
  **once** in the client on page load; subsequent updates flow in via Shiny
  custom messages, so the viewport (pan/zoom) is preserved across refreshes
  and animation frames — no iframe reload, no flicker.
* Clicks on stations are handled client-side by deck.gl's native `onClick`
  and bounced back to Python with `Shiny.setInputValue`. Map clicks and the
  manual multi-select dropdown are kept in sync bidirectionally.
* Everything else (EEA API, EAQI palette, Altair comparison charts, station
  fetching) is unchanged from the Streamlit original.

Run with:
    shiny run --reload test_live.py

Requires:
    pip install shiny shinywidgets requests pandas altair pyarrow
"""

import io
import gzip
import zipfile
import importlib
import json
from functools import lru_cache
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd
import altair as alt

from shiny import App, ui, render, reactive, req
from shinywidgets import output_widget, render_altair


# ── Constants ──────────────────────────────────────────────────────────────────
EEA_API_URL      = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/ParquetFile/urls"
EEA_METADATA_URL = ("https://discomap.eea.europa.eu/App/AQViewer/download"
                    "?fqn=Airquality_Dissem.b2g.measurements&f=csv")
COUNTRIES  = ["AT", "DE", "FR", "IT", "ES", "CH"]
POLLUTANTS = ["PM10", "PM2.5", "NO2", "O3"]

# European Air Quality Index (EAQI) — 6-tier system (upper µg/m³, label, hex colour)
# Blue-to-red colorblind-friendly palette
EAQI_THRESHOLDS = {
    "PM2.5": [(5, "Good", "#4477AA"), (15, "Fair", "#77AADD"), (50, "Moderate", "#DDCC77"),
              (90, "Poor", "#EE7733"), (140, "Very poor", "#CC3311"), (float("inf"), "Extremely poor", "#882255")],
    "PM10":  [(15, "Good", "#4477AA"), (45, "Fair", "#77AADD"), (120, "Moderate", "#DDCC77"),
              (195, "Poor", "#EE7733"), (270, "Very poor", "#CC3311"), (float("inf"), "Extremely poor", "#882255")],
    "NO2":   [(10, "Good", "#4477AA"), (25, "Fair", "#77AADD"), (60, "Moderate", "#DDCC77"),
              (100, "Poor", "#EE7733"), (150, "Very poor", "#CC3311"), (float("inf"), "Extremely poor", "#882255")],
    "O3":    [(60, "Good", "#4477AA"), (100, "Fair", "#77AADD"), (120, "Moderate", "#DDCC77"),
              (160, "Poor", "#EE7733"), (180, "Very poor", "#CC3311"), (float("inf"), "Extremely poor", "#882255")],
}
EAQI_LABELS  = ["Good", "Fair", "Moderate", "Poor", "Very poor", "Extremely poor"]
EAQI_COLOURS = {"Good": "#4477AA", "Fair": "#77AADD", "Moderate": "#DDCC77",
                "Poor": "#EE7733", "Very poor": "#CC3311", "Extremely poor": "#882255"}
_AREA_SYMBOLS = {"urban": "●", "suburban": "◆", "rural": "▲",
                 "rural-nearcity": "▲", "rural_nearcity": "▲"}


# ── EAQI helpers ───────────────────────────────────────────────────────────────

def get_aqi_label(value, pollutant):
    """Return the EAQI category label for a µg/m³ value."""
    if pd.isna(value) or value < 0:
        return None
    for upper, label, _ in EAQI_THRESHOLDS.get(pollutant, EAQI_THRESHOLDS["PM10"]):
        if value <= upper:
            return label
    return "Very Poor"


def apply_aqi_styling(df, pollutant):
    """Assign EAQI colour, area-type symbol, and tooltip string to each row."""
    if df.empty or "Value" not in df.columns:
        return df
    thresholds = EAQI_THRESHOLDS.get(pollutant, EAQI_THRESHOLDS["PM10"])

    def _tier(v):
        if pd.isna(v) or v <= 0:
            return None, (128, 128, 128)
        for upper, label, hex_col in thresholds:
            if v <= upper:
                return label, (int(hex_col[1:3], 16), int(hex_col[3:5], 16), int(hex_col[5:7], 16))
        return "Very Poor", (232, 65, 111)

    tiers           = df["Value"].apply(_tier)
    df["aqi_label"] = tiers.apply(lambda t: t[0]).fillna("No data")
    df["color_r"]   = tiers.apply(lambda t: t[1][0]).astype(int)
    df["color_g"]   = tiers.apply(lambda t: t[1][1]).astype(int)
    df["color_b"]   = tiers.apply(lambda t: t[1][2]).astype(int)
    df["color_a"]   = df["Value"].apply(lambda v: 0 if (pd.isna(v) or v <= 0) else 220).astype(int)
    df["symbol"]    = (df["area_type"].str.lower().map(_AREA_SYMBOLS).fillna("●")
                       if "area_type" in df.columns else "●")
    df["value_str"] = df["Value"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
    return df


def render_legend(pollutant):
    thresholds = EAQI_THRESHOLDS.get(pollutant, EAQI_THRESHOLDS["PM10"])
    prev, swatches = 0, []
    for upper, label, colour in thresholds:
        rng = f"{prev}–{upper}" if upper != float("inf") else f"{prev}+"
        swatches.append(
            f'<span style="background:{colour};color:#111;padding:2px 8px;'
            f'border-radius:3px;font-size:11px;white-space:nowrap">'
            f'{label}&nbsp;<span style="font-size:10px">{rng}</span></span>'
        )
        prev = upper if upper != float("inf") else prev
    return (
        '<div style="margin-top:8px;font-size:12px;color:#ccc;line-height:2.2">'
        f'<b>{pollutant} Air Quality Index (µg/m³)</b><br>'
        + " &thinsp;".join(swatches)
        + '<br><span style="color:#aaa;font-size:11px;margin-top:6px;display:block">'
        + '<b>Station types:</b> ● Large (urban) • ◯ Medium + ring (suburban) • ◯ Small + thick ring (rural)</span>'
        + "</div>"
    )


# ── Data helpers ───────────────────────────────────────────────────────────────

def _parse_timestamps(df, col="Start"):
    if pd.api.types.is_numeric_dtype(df[col]):
        df[col] = pd.to_datetime(df[col], unit="ns", errors="coerce")
    else:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df[col] = df[col].dt.tz_localize(None)
    return df


def _shorten_id(raw_id):
    s = str(raw_id).strip()
    return s.split("/")[-1] if "/" in s else s


def _normalize_key(s):
    return str(s).strip().lower()


# ── Station metadata (coordinates + area type) ─────────────────────────────────

@lru_cache(maxsize=1)
def get_station_coords():
    _EMPTY = pd.DataFrame(columns=["key", "lat", "lon", "station_name", "area_type"])

    def _norm_cols(df_in):
        df_in = df_in.copy()
        df_in.columns = (df_in.columns.astype(str)
                         .str.replace("\ufeff", "", regex=False).str.strip().str.lower()
                         .str.replace(r"[\s\-\.\/]+", "_", regex=True))
        return df_in

    def _extract_coords(df_in, label):
        if df_in is None or df_in.empty:
            return _EMPTY
        df_in = _norm_cols(df_in)
        id_cands   = ["samplingpoint_localid","sampling_point_localid","sampling_point_id",
                      "samplingpointid","samplingpoint","sampling_point","local_id","localid",
                      "air_quality_samplingpoint","air_quality_sampling_point","sampledsfeature",
                      "sampled_feature","feature_of_interest","foi"]
        name_cands = ["air_quality_station_name","stationname","station_name",
                      "airqualitystationname","name","samplingpoint_name"]
        area_cands = ["air_quality_station_area","station_area","stationarea","area_type","area"]

        id_col   = next((c for c in id_cands   if c in df_in.columns), None)
        lat_col  = next((c for c in df_in.columns if c in ("latitude","lat")       or "latitude"  in c), None)
        lon_col  = next((c for c in df_in.columns if c in ("longitude","lon","lng") or "longitude" in c), None)
        name_col = next((c for c in name_cands  if c in df_in.columns), None)
        area_col = next((c for c in area_cands  if c in df_in.columns), None)

        print(f"[get_station_coords] [{label}] id={id_col}, lat={lat_col}, lon={lon_col}, "
              f"name={name_col}, area={area_col}")
        if id_col is None or lat_col is None or lon_col is None:
            return _EMPTY

        raw_ids = df_in[id_col].astype(str).str.strip()
        out = pd.DataFrame({
            "key":          raw_ids.apply(_shorten_id).apply(_normalize_key),
            "lat":          pd.to_numeric(df_in[lat_col], errors="coerce"),
            "lon":          pd.to_numeric(df_in[lon_col], errors="coerce"),
            "station_name": df_in[name_col].astype(str).str.strip() if name_col else raw_ids.apply(_shorten_id),
            "area_type":    df_in[area_col].astype(str).str.strip().str.lower() if area_col else "unknown",
        })
        return out.dropna(subset=["lat","lon"]).drop_duplicates("key").reset_index(drop=True)

    def _try_csv(blob, label):
        for enc in ("utf-8-sig","utf-8","utf-16","utf-16le","utf-16be","latin-1","cp1252"):
            for sep in (",",";","\t","|"):
                try:
                    df = pd.read_csv(io.BytesIO(blob), sep=sep, encoding=enc,
                                     low_memory=False, on_bad_lines="skip")
                    if df.shape[1] >= 4 and not df.empty:
                        print(f"[get_station_coords] Parsed [{label}] sep='{sep}' enc='{enc}' "
                              f"cols={df.shape[1]} rows={len(df)}")
                        return df
                except Exception:
                    continue
        return None

    # ── Try local CSV first (fast, no network required) ──
    try:
        df_local = pd.read_csv("station_metadata_clean.csv", low_memory=False)
        result = _extract_coords(df_local, "local_csv")
        if not result.empty:
            print(f"[get_station_coords] Loaded {len(result)} stations from local CSV (station_metadata_clean.csv)")
            return result
    except Exception as exc:
        print(f"[get_station_coords] Local CSV loading failed: {exc}")

    # ── Fall back to EEA API ──
    try:
        raw = requests.get(EEA_METADATA_URL, timeout=30,
                           headers={"Accept": "text/csv,application/octet-stream,*/*"})
        raw.raise_for_status()
        content = raw.content
    except Exception as exc:
        print(f"[get_station_coords] Download failed: {exc}")
        return _EMPTY

    ct, ce = (raw.headers.get("content-type") or "").lower(), (raw.headers.get("content-encoding") or "").lower()
    print(f"[get_station_coords] HTTP {raw.status_code}, content-type='{ct}', "
          f"content-encoding='{ce}', bytes={len(content)}")

    payloads = [("raw", content)]
    if content.startswith(b"\x1f\x8b") or "gzip" in ce:
        try:
            payloads.append(("gzip", gzip.decompress(content)))
        except Exception as exc:
            print(f"[get_station_coords] Gzip decode failed: {exc}")
    if content.startswith(b"PK") or "zip" in ct:
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                names = [n for n in zf.namelist() if not n.endswith("/")]
                for name in names:
                    if name.lower().endswith(".csv"):
                        payloads.append((f"zip:{name}", zf.read(name)))
                if not any(lbl.startswith("zip:") for lbl, _ in payloads) and names:
                    payloads.append((f"zip:{names[0]}", zf.read(names[0])))
                print(f"[get_station_coords] Added {len(names)} zip members")
        except Exception as exc:
            print(f"[get_station_coords] Zip decode failed: {exc}")

    seen = set()
    for label, blob in payloads:
        if not blob:
            continue
        bh = hash(blob[:1024])
        if bh in seen:
            continue
        seen.add(bh)
        df = _try_csv(blob, label)
        if df is None:
            continue
        result = _extract_coords(df, f"eea:{label}")
        if not result.empty:
            print(f"[get_station_coords] Resolved {len(result)} stations from EEA metadata")
            return result

    print("[get_station_coords] EEA failed, trying airbase fallback")
    try:
        ab = importlib.import_module("airbase")
        client, frames = ab.AirbaseClient(), []
        for country in COUNTRIES:
            try:
                meta = client.get_metadata(country=country)
                if isinstance(meta, pd.DataFrame) and not meta.empty:
                    frames.append(meta)
            except Exception as exc:
                print(f"[get_station_coords] airbase {country} failed: {exc}")
        if not frames:
            try:
                meta_all = client.get_metadata()
                if isinstance(meta_all, pd.DataFrame) and not meta_all.empty:
                    frames.append(meta_all)
            except Exception as exc:
                print(f"[get_station_coords] airbase global failed: {exc}")
        if frames:
            result = _extract_coords(pd.concat(frames, ignore_index=True), "airbase")
            if not result.empty:
                print(f"[get_station_coords] Resolved {len(result)} stations via airbase")
                return result
    except Exception as exc:
        print(f"[get_station_coords] airbase unavailable: {exc}")

    print("[get_station_coords] Coordinate resolution failed (EEA + airbase)")
    return _EMPTY


# ── Data fetching ──────────────────────────────────────────────────────────────

def _get_station_urls(country_code, pollutant):
    end_dt   = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=7)
    payload  = {
        "countries": [country_code], "cities": [], "pollutants": [pollutant], "dataset": 1,
        "dateTimeStart": start_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "dateTimeEnd":   end_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "compress": False,
    }
    resp = requests.post(EEA_API_URL, json=payload)
    resp.raise_for_status()
    return [line.strip() for line in resp.text.strip().split("\n")
            if line.strip() and "ParquetFileUrl" not in line]


def _get_all_station_data(country_code, pollutant):
    urls   = _get_station_urls(country_code, pollutant)
    coords = get_station_coords()
    _EMPTY_META = pd.DataFrame(columns=["station_id", "station_name", "lat", "lon",
                                        "area_type", "url", "Value"])
    _EMPTY_ALL  = pd.DataFrame(columns=["station_id", "station_name", "Start", "Value"])
    if not urls:
        return _EMPTY_META, _EMPTY_ALL

    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).replace(tzinfo=None)

    def fetch_one(url):
        try:
            df = pd.read_parquet(url)
            if df.empty:
                return None
            df = _parse_timestamps(df, "Start")
            df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
            df = df[df["Value"] >= 0].dropna(subset=["Start", "Value"])
            df = df[df["Start"] >= cutoff].reset_index(drop=True)
            if df.empty:
                return None
            raw_id     = str(df["Samplingpoint"].iloc[0]) if "Samplingpoint" in df.columns else url
            station_id = _shorten_id(raw_id)
            ts = df[["Start", "Value"]].copy()
            ts["station_id"] = station_id
            return {"meta": {"station_id": station_id, "url": url, "Value": float(df["Value"].mean())}, "ts": ts}
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=20) as ex:
        results = [r for r in ex.map(fetch_one, urls) if r is not None]
    if not results:
        return _EMPTY_META, _EMPTY_ALL

    df_meta = pd.DataFrame([r["meta"] for r in results])
    df_all  = pd.concat([r["ts"] for r in results], ignore_index=True)

    if not coords.empty:
        df_meta["key"]  = df_meta["station_id"].apply(_normalize_key)
        merge_cols      = [c for c in ["key","lat","lon","station_name","area_type"] if c in coords.columns]
        df_meta         = df_meta.merge(coords[merge_cols], on="key", how="left").drop(columns=["key"])
    else:
        df_meta["lat"] = df_meta["lon"] = None
        df_meta["station_name"] = df_meta["station_id"]

    df_meta["station_name"] = df_meta["station_name"].fillna(df_meta["station_id"])
    if "area_type" not in df_meta.columns:
        df_meta["area_type"] = "unknown"
    df_meta["area_type"] = df_meta["area_type"].fillna("unknown")

    name_map = df_meta.set_index("station_id")["station_name"].to_dict()
    df_all["station_name"] = df_all["station_id"].map(name_map).fillna(df_all["station_id"])
    return df_meta, df_all


@lru_cache(maxsize=32)
def _get_all_station_data_cached(country_code, pollutant, cache_bust):
    """Cached wrapper. `cache_bust` ties the cache key to the Refresh button
    counter — pressing Refresh creates a new cache key and thus refetches."""
    return _get_all_station_data(country_code, pollutant)


# ── Display builder ────────────────────────────────────────────────────────────

def build_hour_display(hour, df_meta, df_all, pollutant):
    """Per-station mean for the given floored hour, with EAQI styling applied."""
    df = df_meta.copy()
    if not df_all.empty and hour is not None:
        mask      = df_all["Start"].dt.floor("h") == hour
        hour_vals = (df_all[mask].groupby("station_id")["Value"].mean()
                     .reset_index().rename(columns={"Value": "_v"}))
        df = df.merge(hour_vals, on="station_id", how="left")
        df["Value"] = df["_v"].fillna(df["Value"])
        df = df.drop(columns=["_v"])
    return apply_aqi_styling(df.dropna(subset=["lat","lon"]).copy(), pollutant)


# ── Client-side map payload builder ────────────────────────────────────────────

_JS_EXPORT_COLS = ["lon", "lat", "color_r", "color_g", "color_b", "color_a",
                   "station_name", "aqi_label", "value_str", "area_type"]


def _df_to_records(df):
    """Serialize a DataFrame to a list of plain Python dicts, with NaN→None."""
    if df.empty:
        return []
    cols = [c for c in _JS_EXPORT_COLS if c in df.columns]
    return df[cols].where(df[cols].notna(), None).to_dict("records")


def build_map_payload(df):
    """Split the styled display DataFrame into the three area-type buckets the
    client-side JS expects: urban, suburban, rural."""
    payload = {"urban": [], "suburban": [], "rural": []}
    if df.empty:
        return payload
    if "area_type" in df.columns:
        norm = df["area_type"].str.lower().fillna("unknown")
        urban    = df[norm.isin(["urban", "unknown"])]
        suburban = df[norm == "suburban"]
        rural    = df[norm.isin(["rural", "rural-nearcity", "rural_nearcity"])]
    else:
        urban, suburban, rural = df, df.iloc[0:0], df.iloc[0:0]
    payload["urban"]    = _df_to_records(urban)
    payload["suburban"] = _df_to_records(suburban)
    payload["rural"]    = _df_to_records(rural)
    return payload


# ── Client-side map bootstrap (deck.gl + maplibre-gl) ──────────────────────────

_MAP_INIT_JS = r"""
(function () {
  var deckInstance = null;         // deck.gl MapboxOverlay
  var mapInstance  = null;         // maplibre-gl Map
  var pendingData  = null;         // data queued before init finished
  var selectedStations = [];       // mirror of Python's selected_stations
  var initTries = 0;

  function fillColor(d) {
    return [d.color_r || 128, d.color_g || 128, d.color_b || 128, d.color_a || 220];
  }

  function onStationClick(info) {
    if (!info || !info.object || !info.object.station_name) return;
    // Bump a side-channel event input so Python sees every click even when
    // the same station is clicked twice in a row.
    var name = info.object.station_name;
    if (typeof Shiny !== 'undefined' && Shiny.setInputValue) {
      Shiny.setInputValue('map_click', {name: name, nonce: Date.now()},
                          {priority: 'event'});
    }
  }

  function buildLayers(data) {
    if (!data) data = {urban: [], suburban: [], rural: []};
    var layers = [];

    // Urban — largest circles, no ring
    layers.push(new deck.ScatterplotLayer({
      id: 'urban-layer',
      data: data.urban || [],
      getPosition: function (d) { return [d.lon, d.lat]; },
      getRadius: 7000,
      radiusMinPixels: 5,
      radiusMaxPixels: 16,
      getFillColor: fillColor,
      pickable: true,
      onClick: onStationClick,
      updateTriggers: {getFillColor: [data.stamp || 0]}
    }));

    // Suburban — medium circles + thin ring
    layers.push(new deck.ScatterplotLayer({
      id: 'suburban-layer',
      data: data.suburban || [],
      getPosition: function (d) { return [d.lon, d.lat]; },
      getRadius: 6000,
      radiusMinPixels: 5,
      radiusMaxPixels: 16,
      getFillColor: fillColor,
      pickable: true,
      onClick: onStationClick,
      updateTriggers: {getFillColor: [data.stamp || 0]}
    }));
    layers.push(new deck.ScatterplotLayer({
      id: 'suburban-ring',
      data: data.suburban || [],
      getPosition: function (d) { return [d.lon, d.lat]; },
      getRadius: 6000,
      radiusMinPixels: 5,
      radiusMaxPixels: 16,
      stroked: true,
      filled: false,
      getLineColor: [255, 255, 255, 200],
      lineWidthMinPixels: 2,
      pickable: false
    }));

    // Rural — smallest circles + thicker ring
    layers.push(new deck.ScatterplotLayer({
      id: 'rural-layer',
      data: data.rural || [],
      getPosition: function (d) { return [d.lon, d.lat]; },
      getRadius: 5000,
      radiusMinPixels: 5,
      radiusMaxPixels: 16,
      getFillColor: fillColor,
      pickable: true,
      onClick: onStationClick,
      updateTriggers: {getFillColor: [data.stamp || 0]}
    }));
    layers.push(new deck.ScatterplotLayer({
      id: 'rural-ring',
      data: data.rural || [],
      getPosition: function (d) { return [d.lon, d.lat]; },
      getRadius: 5000,
      radiusMinPixels: 5,
      radiusMaxPixels: 16,
      stroked: true,
      filled: false,
      getLineColor: [255, 255, 255, 200],
      lineWidthMinPixels: 3,
      pickable: false
    }));

    // Selection highlight — outer yellow ring around any station currently
    // picked for comparison. Data is derived from all three buckets.
    if (selectedStations.length) {
      var pool = [].concat(data.urban || [], data.suburban || [], data.rural || []);
      var hits = pool.filter(function (d) {
        return selectedStations.indexOf(d.station_name) !== -1;
      });
      layers.push(new deck.ScatterplotLayer({
        id: 'selection-highlight',
        data: hits,
        getPosition: function (d) { return [d.lon, d.lat]; },
        getRadius: 9000,
        radiusMinPixels: 10,
        radiusMaxPixels: 22,
        stroked: true,
        filled: false,
        getLineColor: [255, 215, 0, 255],
        lineWidthMinPixels: 3,
        pickable: false
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
      style: {
        backgroundColor: '#1a1a1a',
        color: '#fafafa',
        border: '1px solid #444',
        padding: '6px 10px',
        borderRadius: '4px'
      }
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
      center: [10, 50],
      zoom: 3.5,
      attributionControl: true
    });
    mapInstance.addControl(new maplibregl.NavigationControl({visualizePitch: false}),
                           'top-right');

    deckInstance = new deck.MapboxOverlay({
      interleaved: false,
      layers: buildLayers(pendingData),
      getTooltip: buildTooltip
    });

    mapInstance.on('load', function () {
      mapInstance.addControl(deckInstance);
      // Once deck is wired into the map, apply any data that arrived before init.
      if (pendingData) applyData(pendingData);
    });

    // Give the container a hard-coded height (already set in CSS) and force
    // maplibre to measure it once rendered.
    setTimeout(function () { if (mapInstance) mapInstance.resize(); }, 200);
  }

  function setupShinyHandlers() {
    if (typeof Shiny === 'undefined' || !Shiny.addCustomMessageHandler) {
      setTimeout(setupShinyHandlers, 60);
      return;
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
  } else {
    boot();
  }
})();
"""


# ── UI ─────────────────────────────────────────────────────────────────────────

app_css = """
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
.selectize-dropdown, .selectize-dropdown-content { background-color: #262730 !important; color: #fafafa !important; }
.selectize-dropdown .active { background-color: #ff4b4b !important; color: #fff !important; }
.btn-primary { background-color: #ff4b4b; border-color: #ff4b4b; }
.btn-primary:hover { background-color: #d43d3d; border-color: #d43d3d; }
.irs--shiny .irs-bar { background: #ff4b4b; border-top-color: #ff4b4b; border-bottom-color: #ff4b4b; }
.irs--shiny .irs-from, .irs--shiny .irs-to, .irs--shiny .irs-single { background-color: #ff4b4b; }
.irs--shiny .irs-line { background: #333; border-color: #333; }
.irs--shiny .irs-grid-text { color: #aaa; }
.shiny-notification { background-color: #1a1a1a; color: #fafafa; border-color: #333; }
#deck-map {
    width: 100%;
    height: 450px;
    border-radius: 4px;
    overflow: hidden;
    position: relative;
    background: #0e1117;
}
#deck-map canvas { outline: none; }
"""

app_ui = ui.page_fluid(
    ui.head_content(
        ui.tags.link(rel="stylesheet",
                     href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css"),
        ui.tags.script(src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"),
        ui.tags.script(src="https://unpkg.com/deck.gl@9.0.33/dist.min.js"),
    ),
    ui.tags.style(app_css),
    ui.h1("Live Air Quality — Last 7 Days"),
    ui.layout_columns(
        # ── Left column: map + legend + comparison ─────────────────────────
        ui.div(
            ui.output_ui("map_label"),
            ui.output_ui("map_status"),
            # Static map container — mounted exactly once, never re-rendered.
            ui.tags.div(id="deck-map"),
            ui.tags.script(ui.HTML(_MAP_INIT_JS)),
            ui.output_ui("map_legend"),
            ui.output_ui("comparison_section"),
        ),
        # ── Right column: controls ─────────────────────────────────────────
        ui.div(
            ui.layout_columns(
                ui.h4("Live Data"),
                ui.input_select("pollutant", None, choices=POLLUTANTS, selected="PM10"),
                col_widths=(6, 6),
            ),
            ui.input_select("country", "Country", choices=COUNTRIES, selected="AT"),
            ui.input_action_button("load", "Load / Refresh", class_="btn-primary"),
            ui.output_ui("hour_slider_ui"),
            ui.layout_columns(
                ui.input_action_button("play", "▶️ Play"),
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

def server(input, output, session):
    # ── Reactive state ──────────────────────────────────────────────────────
    selected_stations = reactive.Value([])   # list[str]
    playing           = reactive.Value(False)
    anim_idx          = reactive.Value(0)

    # ── Data fetch (memoized per country+pollutant+refresh count) ──────────
    @reactive.calc
    def fetched():
        country   = input.country()
        pollutant = input.pollutant()
        load_n    = input.load()  # click-count acts as cache buster
        with ui.Progress(min=1, max=1) as p:
            p.set(message=f"Fetching live data for {country} / {pollutant}…")
            return _get_all_station_data_cached(country, pollutant, load_n)

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

    # Reset per-view state when country / pollutant / refresh-count change
    @reactive.effect
    @reactive.event(input.country, input.pollutant, input.load)
    def _reset_on_change():
        selected_stations.set([])
        playing.set(False)
        anim_idx.set(0)

    # ── Play / Stop handlers ────────────────────────────────────────────────
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

    # ── Animation driver ────────────────────────────────────────────────────
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

    # ── Hour slider UI (rebuilt when data range changes) ───────────────────
    @output
    @render.ui
    def hour_slider_ui():
        hours = all_hours()
        if len(hours) > 1:
            return ui.input_slider(
                "hour_slider", "Scrub through time",
                min=hours[0], max=hours[-1], value=hours[-1],
                step=timedelta(hours=1),
                time_format="%d %b, %H:%M",
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

    # ── Current frame resolution (animation vs. slider) ────────────────────
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

    # ── Push map data to the client whenever it changes ────────────────────
    @reactive.effect
    async def _push_map_data():
        df = df_display()
        payload = build_map_payload(df)
        # A stamp lets deck.gl know fill colours need re-evaluation even when
        # the row objects are structurally identical across frames.
        payload["stamp"] = datetime.utcnow().timestamp()
        await session.send_custom_message("update_map_data", payload)

    # ── Push current selection to the client for the yellow highlight ──────
    @reactive.effect
    async def _push_selection():
        sel = list(selected_stations())
        await session.send_custom_message("update_selected_stations", {"stations": sel})

    # ── Header above the map ────────────────────────────────────────────────
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

    # ── Inline info banner (empty data / missing coords) ───────────────────
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

    # ── Right-panel informational outputs ──────────────────────────────────
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

    # ── Manual multi-select (rebuilt when station list changes) ────────────
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

    # ── Map-click handler ──────────────────────────────────────────────────
    @reactive.effect
    @reactive.event(input.map_click)
    def _on_map_click():
        if playing():           # ignore stray clicks during animation
            return
        click = input.map_click()
        if not click:
            return
        # Client sends {name, nonce}; older form (plain string) handled too.
        name = click.get("name") if isinstance(click, dict) else click
        if not name:
            return
        with reactive.isolate():
            current = list(selected_stations())
        if name in current:
            return  # mirror Streamlit original: no-op if already selected
        if len(current) < 2:
            current.append(name)
        else:
            current = [current[1], name]
        selected_stations.set(current)

    # ── Keep the selectize widget in sync when the map drives selection ────
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

    # ── Comparison section (line chart + breakdown + cards) ────────────────
    @output
    @render.ui
    def comparison_section():
        if not selected_stations() or df_all().empty:
            return ui.tags.div()
        return ui.tags.div(
            ui.hr(),
            ui.h4("Station Comparison"),
            ui.layout_columns(
                ui.div(output_widget("line_chart")),
                ui.div(
                    output_widget("breakdown_chart"),
                    ui.output_ui("summary_cards"),
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
            alt.Chart(bands_df)
            .mark_rect(opacity=0.15)
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
            alt.Chart(breakdown)
            .mark_bar(height=28)
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
            .properties(height=90, title="Air Quality Breakdown (share of hours)",
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


app = App(app_ui, server)