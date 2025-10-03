# ingestion.py
# Purpose: Fetch raw datasets from ECCC (CSV) and Open-Meteo (API).
# Implements: IHistoricalData and IForecastData
# Provides: IRawDatasets (via returned DataFrames)

from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import requests
import numpy as np  # using np.nan for missing values

# ---- Configuration ----
TORONTO_TZ = ZoneInfo("America/Toronto")

def now_toronto_iso() -> str:
    """Return current ETL timestamp in America/Toronto."""
    return datetime.now(tz=TORONTO_TZ).strftime("%Y-%m-%d %H:%M:%S")

# --- helpers ---------------------------------------------------------------

def _first_match(df_cols, candidates):
    """Pick the first existing column name from candidates; raise if none found."""
    for name in candidates:
        if name in df_cols:
            return name
    raise KeyError(f"Missing expected column among {candidates}. Please update mapping for your CSV.")

def _to_series_date(values) -> pd.Series:
    """
    Robustly convert various datetime-like inputs (Series/array/DatetimeIndex)
    to a pandas Series of Python date objects (YYYY-MM-DD).
    """
    ts = pd.to_datetime(values, errors="coerce")
    # Ensure we have a Series so .dt is always available
    if isinstance(ts, pd.DatetimeIndex):
        ts = pd.Series(ts)
    return ts.dt.date

# --- ingestion: ECCC (historical) -----------------------------------------

def ingest_eccc(csv_path: str) -> pd.DataFrame:
    """Read ECCC daily CSV and return a raw historical DataFrame."""
    # NOTE: ECCC headers vary by station/locale; keep candidates for robustness.
    colmap_candidates = {
        "date":   ["Date", "Date/Time", "LOCAL_DATE", "date"],
        "tmax":   ["Max Temp (°C)", "Maximum Temperature (°C)", "Max Temp (C)", "MAX_TEMPERATURE", "Max Temp"],
        "tmin":   ["Min Temp (°C)", "Minimum Temperature (°C)", "Min Temp (C)", "MIN_TEMPERATURE", "Min Temp"],
        "precip": ["Total Precip (mm)", "Total Rain (mm)", "Total Precipitation (mm)", "TOTAL_PRECIP", "Precipitation"],
    }

    df = pd.read_csv(csv_path)

    c_date   = _first_match(df.columns, colmap_candidates["date"])
    c_tmax   = _first_match(df.columns, colmap_candidates["tmax"])
    c_tmin   = _first_match(df.columns, colmap_candidates["tmin"])
    c_precip = _first_match(df.columns, colmap_candidates["precip"])

    out = pd.DataFrame({
        # Daily data: no need to attach timezone; store plain date
        "date": _to_series_date(df[c_date]),
        "obs_tmax_c":    pd.to_numeric(df[c_tmax],   errors="coerce"),
        "obs_tmin_c":    pd.to_numeric(df[c_tmin],   errors="coerce"),
        "obs_precip_mm": pd.to_numeric(df[c_precip], errors="coerce"),
    })

    out["data_type"]   = "historical"
    out["source"]      = "ECCC"
    out["ingested_at"] = now_toronto_iso()

    # For merge strategy, initialize forecast fields as missing
    out["fc_tmax_c"] = np.nan
    out["fc_tmin_c"] = np.nan
    out["fc_precip_mm"] = np.nan

    # Optional debug:
    # print(f"[DEBUG] ECCC rows: {len(out)} from {csv_path}")

    return out

# --- ingestion: Open-Meteo (forecast) -------------------------------------

def ingest_openmeteo(
    lat: float,
    lon: float,
    days: int = 16,
    past_days: int = 5,
    daily_params=("temperature_2m_max", "temperature_2m_min", "precipitation_sum"),
) -> pd.DataFrame:
    """Call Open-Meteo daily API and return a raw forecast DataFrame."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": ",".join(daily_params),
        "forecast_days": days,
        "past_days": past_days,
        "timezone": "America/Toronto",
    }
    print(f"[DEBUG] Open-Meteo GET: {url} params={params}")
    r = requests.get(url, params=params, timeout=30)
    # DEBUG: show final URL and status
    print(f"[DEBUG] Open-Meteo GET: {r.url}")
    print(f"[DEBUG] Status code  : {r.status_code}")
    r.raise_for_status()

    payload = r.json()
    daily = payload.get("daily", {})
    times = daily.get("time", [])

    print(f"[DEBUG] daily keys   : {list(daily.keys())}")
    print(f"[DEBUG] days returned: {len(times)}")

    # If API returned nothing, fail fast so we know it
    if not times:
        raise RuntimeError("Open-Meteo returned no daily data (check coords/network/params).")

    dates = _to_series_date(times)

    # Map API arrays to our fc_* fields
    fc_tmax = pd.to_numeric(daily.get("temperature_2m_max", []), errors="coerce")
    fc_tmin = pd.to_numeric(daily.get("temperature_2m_min", []), errors="coerce")
    fc_prcp = pd.to_numeric(daily.get("precipitation_sum", []),   errors="coerce")

    out = pd.DataFrame({
        "date": dates,
        "fc_tmax_c": fc_tmax,
        "fc_tmin_c": fc_tmin,
        "fc_precip_mm": fc_prcp,
    })

    out["data_type"]   = "forecast"
    out["source"]      = "OpenMeteo"
    out["ingested_at"] = now_toronto_iso()

    # For merge strategy, initialize observed fields as missing
    out["obs_tmax_c"] = np.nan
    out["obs_tmin_c"] = np.nan
    out["obs_precip_mm"] = np.nan

    # Optional debug:
    # print(f"[DEBUG] Open-Meteo rows: {len(out)} (lat={lat}, lon={lon}, days={days})")

    return out
