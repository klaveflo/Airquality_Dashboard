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
import datetime as _dt
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
        + '<b>Station types:</b> ● Filled: urban &nbsp;|&nbsp; ◉ Filled + white border: suburban &nbsp;|&nbsp; ○ Hollow EAQI-colour ring: rural</span>'
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
        # Left-join preserves master station_name / area_type columns
        df = pd.merge(master_df, df, on=["lat","lon"], how="left")
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


@lru_cache(maxsize=32)
def hist_get_daily_averages(table_name, start, end):
    conn = get_db()
    query = f"""
        SELECT DATE("Date") as "Date", AVG("Value") as AvgValue
        FROM {table_name}
        WHERE "Value" IS NOT NULL AND DATE("Date") BETWEEN '{start}' AND '{end}'
        GROUP BY DATE("Date") ORDER BY "Date"
    """
    df = conn.execute(query).fetch_df()
    df["Date"] = pd.to_datetime(df["Date"])
    return df


@lru_cache(maxsize=16)
def hist_get_yoy_data(table_name, start, end):
    conn = get_db()
    query = f"""
        SELECT
            YEAR("Date")  AS Year,
            MONTH("Date") AS Month,
            AVG("Value")  AS AvgValue
        FROM {table_name}
        WHERE "Value" IS NOT NULL AND DATE("Date") BETWEEN '{start}' AND '{end}'
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


def build_hist_avg_chart(df_avg, current_date):
    if df_avg.empty:
        return alt.Chart(pd.DataFrame({"Date":[],"AvgValue":[]})).mark_line().properties(height=190)
    df = df_avg.copy()
    df["DateStr"] = df["Date"].dt.strftime("%Y-%m-%d")
    date_str = str(current_date)

    # Scalar Vega-Lite param that holds the selected date string.
    # During animation this is updated client-side via the Vega signal API
    # (see update_hist_dot_date message handler) so only the dot moves, not
    # the whole chart.
    date_param = alt.param(name="histCurDate", value=date_str)

    base = (alt.Chart(df).mark_line(color="gray", strokeWidth=2)
            .encode(x=alt.X("Date:T", title="Date"),
                    y=alt.Y("AvgValue:Q", title="Daily Average Value"))
            .properties(height=190)
            .interactive())
    dot = (alt.Chart(df)
           .mark_circle(color="red", size=100, opacity=1)
           .encode(x="Date:T", y="AvgValue:Q")
           .transform_filter("datum.DateStr === histCurDate"))
    return (alt.layer(base, dot)
            .add_params(date_param)
            .resolve_scale(y="shared")
            .properties(width="container"))


def build_yoy_chart(df_yoy, pollutant):
    if df_yoy.empty:
        return alt.Chart(pd.DataFrame()).mark_line().properties(height=200)
    years = sorted(df_yoy["Year"].unique())
    if len(years) > 5:
        step  = max(1, len(years) // 4)
        picked = years[::step]
        if years[-1] not in picked:
            picked = list(picked) + [years[-1]]
        df_yoy = df_yoy[df_yoy["Year"].isin(picked)]
    return (
        alt.Chart(df_yoy)
        .mark_line(point=True, strokeWidth=2)
        .encode(
            x=alt.X("MonthDate:T", title="Month",
                    axis=alt.Axis(format="%b", tickCount="month", labelAngle=0)),
            y=alt.Y("AvgValue:Q", title=f"{pollutant} Monthly Avg (µg/m³)"),
            color=alt.Color("YearStr:N", title="Year"),
            tooltip=[
                alt.Tooltip("YearStr:N", title="Year"),
                alt.Tooltip("MonthDate:T", format="%B", title="Month"),
                alt.Tooltip("AvgValue:Q", format=".1f", title="µg/m³"),
            ],
        )
        .properties(height=200, title="Year-over-Year Comparison (monthly averages)")
        .interactive()
        .properties(width="container")
    )


def build_hist_eaqi_legend(pollutant):
    thresholds = EAQI_THRESHOLDS.get(pollutant, EAQI_THRESHOLDS["PM10"])
    prev, swatches = 0, []
    for upper, label, colour in thresholds:
        rng = f"{prev}–{upper}" if upper != float("inf") else f"{prev}+"
        swatches.append(
            f'<span style="background:{colour};color:#111;padding:2px 8px;'
            f'border-radius:3px;font-size:11px;white-space:nowrap;margin-right:3px">'
            f'{label}&nbsp;<span style="font-size:10px">{rng}</span></span>'
        )
        prev = upper if upper != float("inf") else prev
    return (
        '<div style="margin-top:6px;font-size:12px;color:#ccc;line-height:2.2">'
        f'<b>{pollutant} EAQI (µg/m³):</b> '
        + " ".join(swatches)
        + '<br><span style="color:#aaa;font-size:11px;margin-top:6px;display:block">'
        + '<b>Station types:</b> ● Filled: urban &nbsp;|&nbsp; ◉ Filled + white border: suburban &nbsp;|&nbsp; ○ Hollow EAQI-colour ring: rural</span>'
        + "</div>"
    )
