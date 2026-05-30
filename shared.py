"""
shared.py — constants, EAQI helpers, data helpers, station metadata,
live-data fetching, and DuckDB / historic query functions.

Imported by live_tab.py, hist_tab.py, and app.py.
"""

import io
import gzip
import zipfile
import importlib
from functools import lru_cache
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd
import altair as alt
import duckdb


# ── Constants ──────────────────────────────────────────────────────────────────
EEA_API_URL      = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/ParquetFile/urls"
EEA_METADATA_URL = ("https://discomap.eea.europa.eu/App/AQViewer/download"
                    "?fqn=Airquality_Dissem.b2g.measurements&f=csv")
# All EEA-reporting countries present in station_metadata_clean.csv.
# Dict maps ISO 3166-1 alpha-2 code (used in API calls) → display label.
COUNTRIES = {
    "AD": "Andorra (AD)",
    "AL": "Albania (AL)",
    "AT": "Austria (AT)",
    "BA": "Bosnia and Herzegovina (BA)",
    "BE": "Belgium (BE)",
    "BG": "Bulgaria (BG)",
    "CH": "Switzerland (CH)",
    "CY": "Cyprus (CY)",
    "CZ": "Czechia (CZ)",
    "DE": "Germany (DE)",
    "DK": "Denmark (DK)",
    "EE": "Estonia (EE)",
    "ES": "Spain (ES)",
    "FI": "Finland (FI)",
    "FR": "France (FR)",
    "GB": "United Kingdom (GB)",
    "GE": "Georgia (GE)",
    "GR": "Greece (GR)",
    "HR": "Croatia (HR)",
    "HU": "Hungary (HU)",
    "IE": "Ireland (IE)",
    "IS": "Iceland (IS)",
    "IT": "Italy (IT)",
    "LT": "Lithuania (LT)",
    "LU": "Luxembourg (LU)",
    "LV": "Latvia (LV)",
    "ME": "Montenegro (ME)",
    "MK": "North Macedonia (MK)",
    "MT": "Malta (MT)",
    "NL": "Netherlands (NL)",
    "NO": "Norway (NO)",
    "PL": "Poland (PL)",
    "PT": "Portugal (PT)",
    "RO": "Romania (RO)",
    "RS": "Serbia (RS)",
    "SE": "Sweden (SE)",
    "SI": "Slovenia (SI)",
    "SK": "Slovakia (SK)",
    "TR": "Türkiye (TR)",
    "UA": "Ukraine (UA)",
    "XK": "Kosovo (XK)",
}
POLLUTANTS = ["PM10", "PM2.5", "NO2", "O3"]

# Approximate map center + zoom for each country code; used by both tabs.
# Falls back to "ALL" (all-Europe) if a code is missing.
COUNTRY_VIEWS = {
    "ALL": {"center": [10.0, 50.0], "zoom": 3.5},
    "AD": {"center": [1.6,   42.5], "zoom": 8.5},
    "AL": {"center": [20.2,  41.2], "zoom": 6.0},
    "AT": {"center": [14.6,  47.7], "zoom": 5.5},
    "BA": {"center": [17.8,  44.2], "zoom": 6.0},
    "BE": {"center": [4.5,   50.5], "zoom": 6.5},
    "BG": {"center": [25.5,  42.7], "zoom": 5.5},
    "CH": {"center": [8.2,   46.8], "zoom": 6.5},
    "CY": {"center": [33.2,  35.1], "zoom": 7.0},
    "CZ": {"center": [15.5,  49.8], "zoom": 5.5},
    "DE": {"center": [10.4,  51.2], "zoom": 4.5},
    "DK": {"center": [10.0,  56.0], "zoom": 5.5},
    "EE": {"center": [25.0,  58.7], "zoom": 6.0},
    "ES": {"center": [-3.7,  40.4], "zoom": 4.5},
    "FI": {"center": [26.0,  64.0], "zoom": 4.0},
    "FR": {"center": [2.2,   46.6], "zoom": 4.5},
    "GB": {"center": [-2.0,  54.0], "zoom": 4.5},
    "GE": {"center": [43.8,  42.3], "zoom": 6.0},
    "GR": {"center": [22.0,  39.5], "zoom": 5.0},
    "HR": {"center": [16.4,  45.1], "zoom": 6.0},
    "HU": {"center": [19.0,  47.2], "zoom": 5.5},
    "IE": {"center": [-8.0,  53.4], "zoom": 5.5},
    "IS": {"center": [-19.0, 65.0], "zoom": 4.5},
    "IT": {"center": [12.5,  42.5], "zoom": 4.5},
    "LT": {"center": [24.0,  55.9], "zoom": 6.0},
    "LU": {"center": [6.1,   49.8], "zoom": 8.5},
    "LV": {"center": [25.0,  56.9], "zoom": 6.0},
    "ME": {"center": [19.4,  42.8], "zoom": 7.0},
    "MK": {"center": [21.7,  41.6], "zoom": 7.0},
    "MT": {"center": [14.4,  35.9], "zoom": 9.0},
    "NL": {"center": [5.3,   52.3], "zoom": 6.0},
    "NO": {"center": [10.7,  65.0], "zoom": 3.5},
    "PL": {"center": [19.5,  52.0], "zoom": 5.0},
    "PT": {"center": [-8.0,  39.5], "zoom": 5.0},
    "RO": {"center": [25.0,  45.8], "zoom": 5.0},
    "RS": {"center": [21.0,  44.0], "zoom": 6.0},
    "SE": {"center": [17.0,  63.0], "zoom": 3.5},
    "SI": {"center": [14.8,  46.1], "zoom": 7.0},
    "SK": {"center": [19.2,  48.7], "zoom": 6.0},
    "TR": {"center": [35.0,  39.0], "zoom": 4.0},
    "UA": {"center": [32.0,  49.0], "zoom": 4.5},
    "XK": {"center": [21.1,  42.6], "zoom": 7.5},
}

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
    """Assign EAQI colour and tooltip string to each row."""
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
    df["value_str"] = df["Value"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
    return df


_EAQI_LIGHT_SWATCHES = {"Fair", "Moderate"}

def build_eaqi_legend(pollutant):
    """Unified EAQI legend used by both Live and Historic tabs."""
    thresholds = EAQI_THRESHOLDS.get(pollutant, EAQI_THRESHOLDS["PM10"])
    prev, swatches = 0, []
    for upper, label, colour in thresholds:
        rng = f"{prev}–{upper}" if upper != float("inf") else f"{prev}+"
        if label in _EAQI_LIGHT_SWATCHES:
            txt, shadow = "#222", ""
        else:
            txt, shadow = "#fff", "text-shadow:0 1px 2px rgba(0,0,0,0.55);"
        swatches.append(
            f'<span style="background:{colour};color:{txt};{shadow}'
            f'padding:2px 7px;border-radius:3px;font-size:11px;font-weight:600;'
            f'white-space:nowrap;display:inline-block;margin:2px 3px 2px 0">'
            f'{label}&thinsp;<span style="font-size:10px;font-weight:400">{rng}</span></span>'
        )
        prev = upper if upper != float("inf") else prev
    swatches.append(
        '<span style="background:#808080;color:#fff;text-shadow:0 1px 2px rgba(0,0,0,0.55);'
        'padding:2px 7px;border-radius:3px;font-size:11px;font-weight:600;'
        'white-space:nowrap;display:inline-block;margin:2px 3px 2px 0">No data</span>'
    )

    _dot = "display:inline-block;width:12px;height:12px;border-radius:50%;vertical-align:middle;margin-right:4px;box-sizing:border-box;"
    urban_dot    = f'<span style="{_dot}background:#6699cc;"></span>'
    suburban_dot = f'<span style="{_dot}background:#6699cc;border:2px solid #fff;"></span>'
    rural_dot    = f'<span style="{_dot}background:transparent;border:2.5px solid #6699cc;"></span>'

    return (
        '<div style="margin-top:8px;line-height:2.0">'
        f'<div style="font-size:14px;font-weight:700;color:inherit;margin-bottom:4px">'
        f'European Air Quality Index (EAQI) &mdash; {pollutant} (&micro;g/m&sup3;)</div>'
        '<div style="display:flex;flex-wrap:wrap;align-items:center;gap:0;margin-bottom:4px">'
        + "".join(swatches)
        + '</div>'
        f'<div style="font-size:13px;color:inherit;opacity:0.75">'
        f'Station types:&ensp;{urban_dot}Urban&ensp;{suburban_dot}Suburban&ensp;{rural_dot}Rural'
        '</div></div>'
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
                         .str.replace("﻿", "", regex=False).str.strip().str.lower()
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

    # Try local CSV first (fast, no network required)
    try:
        df_local = pd.read_csv("station_metadata_clean.csv", low_memory=False)
        result = _extract_coords(df_local, "local_csv")
        if not result.empty:
            print(f"[get_station_coords] Loaded {len(result)} stations from local CSV")
            return result
    except Exception as exc:
        print(f"[get_station_coords] Local CSV loading failed: {exc}")

    # Fall back to EEA API
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


# ── Live data fetching ─────────────────────────────────────────────────────────

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
def get_all_station_data_cached(country_code, pollutant, cache_bust):
    """Cached wrapper — cache_bust is the Refresh button click count."""
    return _get_all_station_data(country_code, pollutant)


# ── Live display builder ───────────────────────────────────────────────────────

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


_JS_EXPORT_COLS = ["lon", "lat", "color_r", "color_g", "color_b", "color_a",
                   "station_name", "aqi_label", "value_str", "area_type"]


def _df_to_records(df):
    if df.empty:
        return []
    cols = [c for c in _JS_EXPORT_COLS if c in df.columns]
    return df[cols].where(df[cols].notna(), None).to_dict("records")


def build_map_payload(df):
    """Split styled DataFrame into urban/suburban/rural buckets for the client JS."""
    payload = {"urban": [], "suburban": [], "rural": []}
    if df.empty:
        return payload
    if "area_type" in df.columns:
        norm     = df["area_type"].str.lower().fillna("unknown")
        urban    = df[norm.isin(["urban", "unknown"])]
        suburban = df[norm == "suburban"]
        rural    = df[norm.isin(["rural", "rural-nearcity", "rural_nearcity"])]
    else:
        urban, suburban, rural = df, df.iloc[0:0], df.iloc[0:0]
    payload["urban"]    = _df_to_records(urban)
    payload["suburban"] = _df_to_records(suburban)
    payload["rural"]    = _df_to_records(rural)
    return payload


# ── DuckDB connection ──────────────────────────────────────────────────────────

_db_conn = None

def get_db():
    global _db_conn
    if _db_conn is None:
        _db_conn = duckdb.connect("eeaopt.db", read_only=True)
    return _db_conn


# ── Historic query functions ───────────────────────────────────────────────────

@lru_cache(maxsize=1)
def hist_available_countries():
    """Return the set of country codes that have at least one row in either historic table."""
    conn = get_db()
    df = conn.execute("""
        SELECT DISTINCT "Country" FROM airquality_5
        UNION
        SELECT DISTINCT "Country" FROM airquality_6001
    """).fetch_df()
    return set(df["Country"].dropna().str.strip())

@lru_cache(maxsize=4)
def hist_get_all_stations(table_name):
    conn = get_db()
    df = conn.execute(f"""
        SELECT DISTINCT "Latitude" as lat, "Longitude" as lon, "Country"
        FROM {table_name}
        WHERE "Latitude" IS NOT NULL AND "Longitude" IS NOT NULL
    """).fetch_df()
    return df


def hist_get_master_stations(table_name, start, end, country="ALL"):
    df = hist_get_all_stations(table_name)
    if country != "ALL":
        df = df[df["Country"] == country]
    df = df[["lat","lon"]].reset_index(drop=True)

    # Join station metadata (name, area_type) from the shared coords cache.
    # Round to 4 dp (~11 m) before joining to absorb any float-repr differences
    # between the CSV source and the DuckDB export.
    coords = get_station_coords()
    if not coords.empty:
        df["_lat_r"] = df["lat"].round(4)
        df["_lon_r"] = df["lon"].round(4)
        c = coords.copy()
        c["_lat_r"] = c["lat"].round(4)
        c["_lon_r"] = c["lon"].round(4)
        df = df.merge(
            c[["_lat_r","_lon_r","station_name","area_type"]],
            on=["_lat_r","_lon_r"], how="left"
        ).drop(columns=["_lat_r","_lon_r"])
    if "station_name" not in df.columns:
        df["station_name"] = None
    if "area_type" not in df.columns:
        df["area_type"] = "unknown"
    df["station_name"] = df["station_name"].fillna(
        df["lat"].astype(str) + "," + df["lon"].astype(str))
    df["area_type"] = df["area_type"].fillna("unknown")
    return df


@lru_cache(maxsize=64)
def hist_get_map_data_cached(date_str, table_name):
    conn = get_db()
    query = f"""
        SELECT "Latitude" as lat, "Longitude" as lon, AVG("Value") as "Value"
        FROM {table_name}
        WHERE DATE("Date") = '{date_str}' AND "Value" IS NOT NULL
          AND "Latitude" IS NOT NULL AND "Longitude" IS NOT NULL
        GROUP BY "Latitude", "Longitude"
    """
    return conn.execute(query).fetch_df()


def hist_get_map_data(date_str, table_name, master_df=None):
    df = hist_get_map_data_cached(date_str, table_name)
    if master_df is not None and not master_df.empty:
        df = df.drop_duplicates(subset=["lat","lon"])
        df = pd.merge(master_df, df, on=["lat","lon"], how="left")
    df = df[(df["Value"].notna()) & (df["Value"] > 0)]
    pollutant = "PM2.5" if "6001" in table_name else "PM10"
    if not df.empty and "Value" in df.columns:
        thresholds = EAQI_THRESHOLDS.get(pollutant, EAQI_THRESHOLDS["PM10"])
        def _color(v):
            if pd.isna(v) or v <= 0:
                return (128, 128, 128, 0)
            for upper, _, hx in thresholds:
                if v <= upper:
                    return (int(hx[1:3],16), int(hx[3:5],16), int(hx[5:7],16), 210)
            return (136, 34, 85, 210)
        colors = df["Value"].apply(_color)
        df["color_r"]   = colors.apply(lambda c: c[0]).astype(int)
        df["color_g"]   = colors.apply(lambda c: c[1]).astype(int)
        df["color_b"]   = colors.apply(lambda c: c[2]).astype(int)
        df["color_a"]   = colors.apply(lambda c: c[3]).astype(int)
        df["value_str"] = df["Value"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
        df["aqi_label"] = df["Value"].apply(
            lambda v: get_aqi_label(v, pollutant) or "No data")
    return df


@lru_cache(maxsize=64)
def hist_get_daily_averages(table_name, start, end, country="ALL"):
    conn = get_db()
    country_clause = f'AND "Country" = \'{country}\'' if country != "ALL" else ""
    query = f"""
        SELECT DATE("Date") as "Date", AVG("Value") as AvgValue
        FROM {table_name}
        WHERE "Value" IS NOT NULL AND DATE("Date") BETWEEN '{start}' AND '{end}'
          {country_clause}
        GROUP BY DATE("Date") ORDER BY "Date"
    """
    df = conn.execute(query).fetch_df()
    df["Date"] = pd.to_datetime(df["Date"])
    return df


@lru_cache(maxsize=32)
def hist_get_yoy_data(table_name, start, end, country="ALL"):
    conn = get_db()
    country_clause = f'AND "Country" = \'{country}\'' if country != "ALL" else ""
    query = f"""
        SELECT
            YEAR("Date")  AS Year,
            MONTH("Date") AS Month,
            AVG("Value")  AS AvgValue
        FROM {table_name}
        WHERE "Value" IS NOT NULL AND DATE("Date") BETWEEN '{start}' AND '{end}'
          {country_clause}
        GROUP BY YEAR("Date"), MONTH("Date")
        ORDER BY Year, Month
    """
    df = conn.execute(query).fetch_df()
    df["YearStr"] = df["Year"].astype(str)
    df["MonthDate"] = pd.to_datetime(
        "2000-" + df["Month"].astype(str).str.zfill(2) + "-15", format="%Y-%m-%d")
    return df


# ── Historic chart / payload builders ─────────────────────────────────────────

_HIST_MAP_JS_COLS = ["lon", "lat", "color_r", "color_g", "color_b", "color_a",
                     "value_str", "station_name", "aqi_label", "area_type"]


def _hist_df_to_records(df):
    if df.empty:
        return []
    cols = [c for c in _HIST_MAP_JS_COLS if c in df.columns]
    return df[cols].where(df[cols].notna(), None).to_dict("records")


def build_hist_map_payload(df):
    """Split styled historic DataFrame into urban/suburban/rural buckets."""
    payload = {"urban": [], "suburban": [], "rural": [], "stamp": 0}
    if df.empty:
        return payload
    df = df.copy()
    if "value_str" not in df.columns and "Value" in df.columns:
        df["value_str"] = df["Value"].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
    if "area_type" in df.columns:
        norm     = df["area_type"].str.lower().fillna("unknown")
        urban    = df[norm.isin(["urban", "unknown"])]
        suburban = df[norm == "suburban"]
        rural    = df[norm.isin(["rural", "rural-nearcity", "rural_nearcity"])]
    else:
        urban, suburban, rural = df, df.iloc[0:0], df.iloc[0:0]
    payload["urban"]    = _hist_df_to_records(urban)
    payload["suburban"] = _hist_df_to_records(suburban)
    payload["rural"]    = _hist_df_to_records(rural)
    payload["stamp"]    = datetime.utcnow().timestamp()
    return payload


def build_hist_avg_chart(df_avg, current_date, pollutant="PM10"):
    if df_avg.empty:
        return (alt.Chart(pd.DataFrame({"Date": [], "AvgValue": []}))
                .mark_line().properties(height=300, width="container").to_dict())
    df = df_avg.copy()
    df["DateStr"] = df["Date"].dt.strftime("%Y-%m-%d")
    date_str = str(current_date)

    # EAQI background bands — clipped to data range so axis isn't distorted.
    # Alternating opacity (0.30 / 0.20) makes adjacent bands distinguishable;
    # a subtle stroke adds a visible boundary between them.
    _BAND_OPACITIES = [0.30, 0.20, 0.30, 0.20, 0.30, 0.20]
    thresholds = EAQI_THRESHOLDS.get(pollutant, EAQI_THRESHOLDS["PM10"])
    y_max = float(df["AvgValue"].max()) * 1.1
    band_data, prev = [], 0.0
    for i, (upper, label, colour) in enumerate(thresholds):
        y2 = min(float(upper), y_max) if upper != float("inf") else y_max
        if prev >= y_max:
            break
        band_data.append({"y1": prev, "y2": y2, "label": label, "colour": colour,
                          "op": _BAND_OPACITIES[i % len(_BAND_OPACITIES)]})
        prev = float(upper) if upper != float("inf") else y_max
    bands_df = pd.DataFrame(band_data)
    aqi_col_scale = alt.Scale(
        domain=[r["label"] for r in band_data],
        range=[r["colour"] for r in band_data],
    )
    op_scale = alt.Scale(
        domain=[r["label"] for r in band_data],
        range=[r["op"]    for r in band_data],
    )
    bands = (
        alt.Chart(bands_df).mark_rect(stroke="#555", strokeWidth=0.5)
        .encode(
            y=alt.Y("y1:Q", scale=alt.Scale(domain=[0, y_max])),
            y2=alt.Y2("y2:Q"),
            color=alt.Color("label:N", scale=aqi_col_scale, legend=None),
            opacity=alt.Opacity("label:N", scale=op_scale, legend=None),
        )
    )

    date_param = alt.param(name="histCurDate", value=date_str)

    # Click-to-jump selection.  Kept on its own invisible layer so it does not
    # interfere with .interactive() pan/zoom on the base line layer.
    # nearest=True uses Vega's Voronoi snapping so any click in the chart area
    # finds the closest data point.  opacity=0.001 keeps the marks renderable
    # (opacity=0 can cause the canvas renderer to skip marks entirely, missing clicks).
    # fields=["DateStr"] captures the pre-formatted date string directly, avoiding
    # any JS timestamp-to-date conversion and the timezone off-by-one it causes.
    click_sel = alt.selection_point(
        name="date_click", on="click", nearest=True, fields=["DateStr"])
    click_target = (alt.Chart(df)
                    .mark_point(opacity=0.001, size=300)
                    .encode(x="Date:T", y="AvgValue:Q")
                    .add_params(click_sel))

    base = (alt.Chart(df).mark_line(color="gray", strokeWidth=2)
            .encode(x=alt.X("Date:T", title="Date"),
                    y=alt.Y("AvgValue:Q", title=f"Daily Average {pollutant} (µg/m³)",
                            scale=alt.Scale(domain=[0, y_max]),
                            axis=alt.Axis(titlePadding=15)))
            .properties(height=300, title=f"Daily Average {pollutant} Over Time")
            .interactive())
    dot = (alt.Chart(df)
           .mark_circle(color="red", size=100, opacity=1)
           .encode(
               x="Date:T",
               y="AvgValue:Q",
               tooltip=[
                   alt.Tooltip("Date:T", title="Date", format="%d %b %Y"),
                   alt.Tooltip("AvgValue:Q", title="Value", format=".1f"),
               ],
           )
           .transform_filter("datum.DateStr === histCurDate"))
    spec = (alt.layer(bands, base, dot, click_target)
            .add_params(date_param)
            .resolve_scale(y="shared", color="independent")
            .properties(width="container")
            .to_dict())
    # Ensure the chart fills its container div at any window size.
    spec["autosize"] = {"type": "fit-x", "contains": "padding"}
    return spec


def build_yoy_chart(df_yoy, pollutant):
    if df_yoy.empty:
        return alt.Chart(pd.DataFrame()).mark_line().properties(height=300)

    # EAQI background bands — clipped to data range so axis isn't distorted.
    # Alternating opacity (0.30 / 0.20) makes adjacent bands distinguishable;
    # a subtle stroke adds a visible boundary between them.
    _BAND_OPACITIES = [0.30, 0.20, 0.30, 0.20, 0.30, 0.20]
    thresholds = EAQI_THRESHOLDS.get(pollutant, EAQI_THRESHOLDS["PM10"])
    y_max = float(df_yoy["AvgValue"].max()) * 1.1
    band_data, prev = [], 0.0
    for i, (upper, label, colour) in enumerate(thresholds):
        y2 = min(float(upper), y_max) if upper != float("inf") else y_max
        if prev >= y_max:
            break
        band_data.append({"y1": prev, "y2": y2, "label": label, "colour": colour,
                          "op": _BAND_OPACITIES[i % len(_BAND_OPACITIES)]})
        prev = float(upper) if upper != float("inf") else y_max
    bands_df = pd.DataFrame(band_data)
    aqi_col_scale = alt.Scale(
        domain=[r["label"] for r in band_data],
        range=[r["colour"] for r in band_data],
    )
    op_scale = alt.Scale(
        domain=[r["label"] for r in band_data],
        range=[r["op"]    for r in band_data],
    )
    bands = (
        alt.Chart(bands_df).mark_rect(stroke="#555", strokeWidth=0.5)
        .encode(
            y=alt.Y("y1:Q", scale=alt.Scale(domain=[0, y_max])),
            y2=alt.Y2("y2:Q"),
            color=alt.Color("label:N", scale=aqi_col_scale, legend=None),
            opacity=alt.Opacity("label:N", scale=op_scale, legend=None),
        )
    )

    year_sel = alt.selection_point(fields=["YearStr"], bind="legend")
    _year_domain = sorted(df_yoy["YearStr"].unique().tolist())
    _year_palette = [
        "#4477AA", "#EE6677", "#228833", "#CCBB44", "#66CCEE",
        "#AA3377", "#BBBBBB", "#FF8C00", "#6B8E23", "#8B008B",
        "#DC143C", "#00CED1", "#FF6347",
    ]
    year_color_scale = alt.Scale(
        domain=_year_domain,
        range=_year_palette[:len(_year_domain)],
    )
    line = (
        alt.Chart(df_yoy)
        .mark_line(point=True, strokeWidth=2)
        .encode(
            x=alt.X("MonthDate:T", title="Month",
                    axis=alt.Axis(format="%b", tickCount="month", labelAngle=0)),
            y=alt.Y("AvgValue:Q", title=f"{pollutant} Monthly Avg (µg/m³)",
                    scale=alt.Scale(domain=[0, y_max]),
                    axis=alt.Axis(titlePadding=20)),
            color=alt.Color("YearStr:N", scale=year_color_scale, title="Year"),
            opacity=alt.condition(year_sel, alt.value(1.0), alt.value(0.15)),
            tooltip=[
                alt.Tooltip("YearStr:N", title="Year"),
                alt.Tooltip("MonthDate:T", format="%B", title="Month"),
                alt.Tooltip("AvgValue:Q", format=".1f", title="µg/m³"),
            ],
        )
        .add_params(year_sel)
        .properties(height=300, title="Year-over-Year Comparison (monthly averages)")
        .interactive()
    )
    return (
        alt.layer(bands, line)
        .resolve_scale(color="independent")
        .properties(width="container")
    )
