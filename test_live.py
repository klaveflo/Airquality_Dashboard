import io
import gzip
import zipfile
import importlib
import streamlit as st
import requests
import pandas as pd
import pydeck as pdk
import altair as alt
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

st.set_page_config(page_title="Live Air Quality", layout="wide")
st.markdown("""
    <style>
        .block-container { padding-top: 1rem; padding-bottom: 0rem; }
        h1 { margin-top: -2rem; }
    </style>
""", unsafe_allow_html=True)
st.title("Live Air Quality — Last 7 Days")

# ── Constants ──────────────────────────────────────────────────────────────────
EEA_API_URL      = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/ParquetFile/urls"
EEA_METADATA_URL = ("https://discomap.eea.europa.eu/App/AQViewer/download"
                    "?fqn=Airquality_Dissem.b2g.measurements&f=csv")
COUNTRIES  = ["AT", "DE", "FR", "IT", "ES", "CH"]
POLLUTANTS = ["PM10", "PM2.5", "NO2", "O3"]

# European Air Quality Index (EAQI) — 6-tier official EEA scale (upper µg/m³, label, hex colour)
EAQI_THRESHOLDS = {
    "PM2.5": [(15, "Good", "#79BC6A"), (35, "Fair", "#BBCF4C"), (75, "Moderate", "#EEC20B"),
              (115, "Poor", "#F29305"), (150, "Very Poor", "#E8416F"), (float("inf"), "Extremely Poor", "#A50034")],
    "PM10":  [(25, "Good", "#79BC6A"), (50, "Fair", "#BBCF4C"), (90, "Moderate", "#EEC20B"),
              (180, "Poor", "#F29305"), (280, "Very Poor", "#E8416F"), (float("inf"), "Extremely Poor", "#A50034")],
    "NO2":   [(40, "Good", "#79BC6A"), (100, "Fair", "#BBCF4C"), (200, "Moderate", "#EEC20B"),
              (400, "Poor", "#F29305"), (1000, "Very Poor", "#E8416F"), (float("inf"), "Extremely Poor", "#A50034")],
    "O3":    [(60, "Good", "#79BC6A"), (120, "Fair", "#BBCF4C"), (180, "Moderate", "#EEC20B"),
              (240, "Poor", "#F29305"), (320, "Very Poor", "#E8416F"), (float("inf"), "Extremely Poor", "#A50034")],
}
EAQI_LABELS  = ["Good", "Fair", "Moderate", "Poor", "Very Poor", "Extremely Poor"]
EAQI_COLOURS = {"Good": "#79BC6A", "Fair": "#BBCF4C", "Moderate": "#EEC20B",
                "Poor": "#F29305", "Very Poor": "#E8416F", "Extremely Poor": "#A50034"}
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
    df["color_r"]   = tiers.apply(lambda t: t[1][0])
    df["color_g"]   = tiers.apply(lambda t: t[1][1])
    df["color_b"]   = tiers.apply(lambda t: t[1][2])
    df["color_a"]   = df["Value"].apply(lambda v: 0 if (pd.isna(v) or v <= 0) else 220)
    df["symbol"]    = (df["area_type"].str.lower().map(_AREA_SYMBOLS).fillna("●")
                       if "area_type" in df.columns else "●")
    df["value_str"] = df["Value"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
    return df


# ── Map rendering ──────────────────────────────────────────────────────────────

def render_map(df):
    view       = pdk.ViewState(latitude=50.0, longitude=10.0, zoom=3.5, pitch=0)
    fill_color = "[color_r, color_g, color_b, color_a]" if "color_r" in df.columns else [200, 200, 200, 180]

    layers = []

    # Layer 1: Urban stations — filled circles in EAQI colours
    if "area_type" in df.columns:
        urban_df = df[df["area_type"].isin(["urban", "Urban"])].copy() if not df.empty else pd.DataFrame()
    else:
        urban_df = pd.DataFrame()

    if not urban_df.empty:
        urban_layer = pdk.Layer(
            "ScatterplotLayer",
            data=urban_df,
            id="urban-layer",
            get_position=["lon", "lat"],
            get_radius=6000,
            radius_min_pixels=5,
            radius_max_pixels=16,
            get_fill_color=fill_color,
            filled=True,
            stroked=False,
            pickable=True,
            transitions={"getFillColor": 600},
        )
        layers.append(urban_layer)

    # Layer 2: Suburban stations — filled circles with thick white outline
    if "area_type" in df.columns:
        suburban_df = df[df["area_type"].isin(["suburban", "Suburban"])].copy() if not df.empty else pd.DataFrame()
    else:
        suburban_df = pd.DataFrame()

    if not suburban_df.empty:
        suburban_layer = pdk.Layer(
            "ScatterplotLayer",
            data=suburban_df,
            id="suburban-layer",
            get_position=["lon", "lat"],
            get_radius=6000,
            radius_min_pixels=5,
            radius_max_pixels=16,
            get_fill_color=fill_color,
            filled=True,
            stroked=True,
            get_line_color=[255, 255, 255, 220],
            line_width_min_pixels=2,
            line_width_max_pixels=4,
            pickable=True,
            transitions={"getFillColor": 600},
        )
        layers.append(suburban_layer)

    # Layer 3: Rural stations — filled circles with lighter outline
    if "area_type" in df.columns:
        rural_df = df[df["area_type"].isin(["rural", "Rural", "rural-nearcity", "rural_nearcity"])].copy() if not df.empty else pd.DataFrame()
    else:
        rural_df = pd.DataFrame()

    if not rural_df.empty:
        rural_layer = pdk.Layer(
            "ScatterplotLayer",
            data=rural_df,
            id="rural-layer",
            get_position=["lon", "lat"],
            get_radius=6000,
            radius_min_pixels=5,
            radius_max_pixels=16,
            get_fill_color=fill_color,
            filled=True,
            stroked=True,
            get_line_color=[180, 180, 180, 180],
            line_width_min_pixels=1,
            line_width_max_pixels=2,
            pickable=True,
            transitions={"getFillColor": 600},
        )
        layers.append(rural_layer)

    # Fallback layer for stations with missing/unknown area type
    if "area_type" in df.columns:
        unknown_df = df[~df["area_type"].isin(["urban", "Urban", "suburban", "Suburban",
                                                "rural", "Rural", "rural-nearcity", "rural_nearcity"])].copy() if not df.empty else pd.DataFrame()
    else:
        unknown_df = df.copy()

    if not unknown_df.empty:
        unknown_layer = pdk.Layer(
            "ScatterplotLayer",
            data=unknown_df,
            id="unknown-layer",
            get_position=["lon", "lat"],
            get_radius=6000,
            radius_min_pixels=5,
            radius_max_pixels=16,
            get_fill_color=fill_color,
            filled=True,
            stroked=False,
            pickable=True,
            transitions={"getFillColor": 600},
        )
        layers.append(unknown_layer)

    return pdk.Deck(
        layers=layers,
        map_style=None,
        initial_view_state=view,
        tooltip={"text": "{station_name}\n{aqi_label}\nValue: {value_str} µg/m³"},
    )


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
        + '<br><span style="color:#aaa;font-size:11px">● Urban &nbsp;◆ Suburban &nbsp;▲ Rural</span>'
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


# ── Station metadata (coordinates + area type) ────────────────────────────────

@st.cache_data(ttl=21600, show_spinner=False)
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

@st.cache_data(ttl=21600, show_spinner=False)
def get_station_urls(country_code, pollutant):
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


@st.cache_data(ttl=3600, show_spinner=False)
def get_all_station_data(country_code, pollutant):
    urls   = get_station_urls(country_code, pollutant)
    coords = get_station_coords()
    _EMPTY_META = pd.DataFrame(columns=["station_id", "station_name", "lat", "lon", "area_type", "url", "Value"])
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


# ── Session state ──────────────────────────────────────────────────────────────

for _k, _v in [("selected_stations", []), ("playing", False),
               ("loaded_key", None), ("anim_idx", 0), ("hour_slider", None)]:
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ── Layout ─────────────────────────────────────────────────────────────────────

col_map, col_ctrl = st.columns([2, 1])

with col_ctrl:
    hdr_col, poll_col = st.columns([1, 1])
    with hdr_col:
        st.subheader("Live Data")
    with poll_col:
        selected_pollutant = st.selectbox("Pollutant", POLLUTANTS, label_visibility="collapsed")

    selected_country = st.selectbox("Country", COUNTRIES)
    load_btn = st.button("Load / Refresh", type="primary")

current_key = f"{selected_country}_{selected_pollutant}"
if load_btn or st.session_state.loaded_key != current_key:
    st.session_state.update({"loaded_key": current_key, "selected_stations": [],
                              "playing": False, "anim_idx": 0, "hour_slider": None})

with st.spinner(f"Fetching live data for {selected_country} / {selected_pollutant}…"):
    df_meta, df_all = get_all_station_data(selected_country, selected_pollutant)

if df_meta.empty:
    st.warning("No data found for this country/pollutant combination. Try another selection.")
    st.stop()

all_hours = ([h.to_pydatetime() for h in sorted(df_all["Start"].dt.floor("h").unique())]
             if not df_all.empty else [])

# Initialise slider to latest available hour on first load
if st.session_state.hour_slider is None and all_hours:
    st.session_state.hour_slider = all_hours[-1]

# Sync slider position with animation frame BEFORE rendering the slider widget
if st.session_state.playing and all_hours:
    st.session_state.hour_slider = all_hours[st.session_state.anim_idx % len(all_hours)]

with col_ctrl:
    if len(all_hours) > 1:
        selected_hour = st.slider(
            "Scrub through time",
            min_value=all_hours[0],
            max_value=all_hours[-1],
            step=timedelta(hours=1),
            format="DD MMM, HH:mm",
            key="hour_slider",
        )
    elif all_hours:
        selected_hour = all_hours[0]
        st.write(f"**Time:** {all_hours[0].strftime('%d %b %Y, %H:%M')}")
    else:
        selected_hour = None

    btn_col1, btn_col2 = st.columns([1, 2])
    with btn_col1:
        play_pressed = st.button("▶️ Play")
    with btn_col2:
        stop_pressed = st.button("⏹️ Stop")

    if play_pressed and all_hours:
        # Start from the hour currently shown on the slider
        try:
            start_idx = all_hours.index(selected_hour)
        except ValueError:
            start_idx = 0
        st.session_state.update({"playing": True, "anim_idx": start_idx})
    if stop_pressed:
        st.session_state.playing = False

    st.markdown("---")
    st.write(f"**Stations found:** {len(df_meta)}")

    if st.session_state.selected_stations:
        st.write("**Selected for comparison:**")
        for s in st.session_state.selected_stations:
            st.write(f"  • {s}")
        if st.button("Clear selection"):
            st.session_state.selected_stations = []
            st.rerun()
    else:
        st.info("Click up to 2 stations on the map to compare them.")

    st.markdown("---")
    st.write("**Or pick stations manually:**")
    name_options = sorted(df_meta["station_name"].dropna().tolist())
    manual_pick  = st.multiselect(
        "Select stations", options=name_options,
        default=[s for s in st.session_state.selected_stations if s in name_options],
        max_selections=2, label_visibility="collapsed",
    )
    if manual_pick != st.session_state.selected_stations:
        st.session_state.selected_stations = manual_pick
        st.rerun()


# ── Map ────────────────────────────────────────────────────────────────────────

has_coords = not df_meta["lat"].isna().all()

# Resolve the frame to display BEFORE entering col_map so the chart call is
# always at the same widget-tree position regardless of animation state.
if st.session_state.playing and all_hours:
    anim_idx     = st.session_state.anim_idx % len(all_hours)
    display_hour = all_hours[anim_idx]
    map_label    = (f"**Animating:** {display_hour.strftime('%d %b %Y, %H:%M')} "
                    f"*(frame {anim_idx + 1} / {len(all_hours)})*")
else:
    anim_idx     = None
    display_hour = selected_hour
    map_label    = f"**Viewing:** {display_hour.strftime('%d %b %Y, %H:%M') if display_hour else '—'}"

df_display = build_hour_display(display_hour, df_meta, df_all, selected_pollutant)

with col_map:
    if not has_coords:
        st.info("Station coordinates could not be resolved from EEA metadata. "
                "Use the manual picker on the right to compare stations.")
    else:
        st.write(map_label)
        # Single unconditional call — same widget-tree position on every run
        # → pydeck component never remounts → user's zoom/pan is preserved
        map_state = st.pydeck_chart(
            render_map(df_display),
            height=450,
            selection_mode="single-object",
            on_select="rerun",
            key="live_map",
        )
        st.markdown(render_legend(selected_pollutant), unsafe_allow_html=True)

        # Click-to-select only when not animating
        if anim_idx is None:
            sel          = getattr(getattr(map_state, "selection", None), "objects", {}) or {}
            # Check all layer IDs: urban, suburban, rural, unknown
            clicked_objs = (sel.get("urban-layer", []) + sel.get("suburban-layer", []) +
                           sel.get("rural-layer", []) + sel.get("unknown-layer", []))
            if clicked_objs:
                clicked_name = clicked_objs[0].get("station_name")
                if clicked_name and clicked_name not in st.session_state.selected_stations:
                    if len(st.session_state.selected_stations) < 2:
                        st.session_state.selected_stations.append(clicked_name)
                    else:
                        st.session_state.selected_stations = [st.session_state.selected_stations[1], clicked_name]
                    st.rerun()

# Advance animation after all rendering is complete
if anim_idx is not None:
    next_idx = anim_idx + 1
    if next_idx >= len(all_hours):
        st.session_state.playing = False
    else:
        st.session_state.anim_idx = next_idx
        time.sleep(0.25)
        st.rerun()


# ── Comparison section ─────────────────────────────────────────────────────────

if st.session_state.selected_stations and not df_all.empty:
    df_sel = df_all[df_all["station_name"].isin(st.session_state.selected_stations)].copy()

    if not df_sel.empty:
        df_sel["aqi_label"] = df_sel["Value"].apply(
            lambda val: get_aqi_label(val, selected_pollutant) or "Unknown"
        )

        st.markdown("---")
        st.subheader("Station Comparison")
        c_line, c_right = st.columns([3, 2])

        with c_line:
            thresholds = EAQI_THRESHOLDS.get(selected_pollutant, EAQI_THRESHOLDS["PM10"])
            y_max = max(float(df_sel["Value"].max()) * 1.15, thresholds[1][0] + 1)

            # Coloured AQI background bands
            band_data, prev = [], 0
            for upper, label, colour in thresholds:
                y2 = min(upper, y_max) if upper != float("inf") else y_max
                if prev >= y_max:
                    break
                band_data.append({"y1": float(prev), "y2": float(y2), "label": label, "colour": colour})
                prev = upper if upper != float("inf") else y_max

            bands_df    = pd.DataFrame(band_data)
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
                alt.Chart(df_sel)
                .mark_line(point=alt.OverlayMarkDef(size=30))
                .encode(
                    x=alt.X("Start:T", title="Date & Time"),
                    y=alt.Y("Value:Q", title=f"{selected_pollutant} (µg/m³)",
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

            st.altair_chart(
                alt.layer(bands, line).resolve_scale(color="independent"),
                use_container_width=True,
            )

        with c_right:
            # AQI category breakdown — stacked bar showing % of hours in each tier
            order_map   = {lbl: i for i, lbl in enumerate(EAQI_LABELS + ["Unknown"])}
            breakdown   = (df_sel.groupby(["station_name", "aqi_label"])
                           .size().reset_index(name="hours"))
            breakdown["order_idx"] = breakdown["aqi_label"].map(order_map).fillna(99).astype(int)

            full_scale = alt.Scale(
                domain=EAQI_LABELS,
                range=[EAQI_COLOURS[lbl] for lbl in EAQI_LABELS],
            )
            breakdown_chart = (
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
                .properties(height=90, title="Air Quality Breakdown (share of hours)")
            )
            st.altair_chart(breakdown_chart, use_container_width=True)

            # Headline summary cards — one per selected station
            st.write("")
            for station in st.session_state.selected_stations:
                sdf = df_sel[df_sel["station_name"] == station]
                if sdf.empty:
                    continue
                dominant = sdf["aqi_label"].value_counts().index[0]
                colour   = EAQI_COLOURS.get(dominant, "#888")
                worst    = sdf.loc[sdf["Value"].idxmax()]
                best     = sdf.loc[sdf["Value"].idxmin()]
                worst_lbl = get_aqi_label(worst["Value"], selected_pollutant) or "—"
                best_lbl  = get_aqi_label(best["Value"], selected_pollutant) or "—"
                st.markdown(f"""
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
""", unsafe_allow_html=True)
