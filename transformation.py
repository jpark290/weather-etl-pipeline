# transformation.py
# Purpose: Unify raw data into a 10-field flat schema using the MERGE (Outer Join) strategy.
# Implements: IFlatRows (Provided interface)

import pandas as pd
import numpy as np

# 10 Final Fields definition
FLAT_COLUMNS = [
    "date", "data_type", "source", "ingested_at",
    "obs_tmax_c", "obs_tmin_c", "obs_precip_mm",
    "fc_tmax_c", "fc_tmin_c", "fc_precip_mm",
]

def to_flat_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures the exact 10-field schema and normalizes data types.
    (Final stage of Transformation)
    """
    out = df.copy()

    # Add missing columns (created as NA if absent)
    for c in FLAT_COLUMNS:
        if c not in out.columns:
            out[c] = np.nan
    
    # Reorder columns to match the flat schema definition
    out = out[FLAT_COLUMNS]

    # Final date formatting and sorting
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype("string")
    out = out.sort_values(by="date").reset_index(drop=True)
    return out

def merge_flat(eccc_df: pd.DataFrame, fc_df: pd.DataFrame) -> pd.DataFrame:
    """
    MERGE strategy: Full Outer Join on 'date'.
    - Historical values (obs_*) come from ECCC
    - Forecast values (fc_*) come from Open-Meteo
    - Metadata (data_type, source, ingested_at) resolved by defined rules
    """
    # 1. Full Outer Join on date key
    merged = pd.merge(
        eccc_df, fc_df,
        on="date", how="outer",
        suffixes=("_hist", "_fc")
    )

    # 2. ingested_at = the latest timestamp from either source
    ia_hist = pd.to_datetime(merged.get("ingested_at_hist"), errors="coerce")
    ia_fc   = pd.to_datetime(merged.get("ingested_at_fc"),   errors="coerce")
    merged["ingested_at"] = ia_hist.combine(
        ia_fc, func=lambda a, b: max(a, b) if (pd.notna(a) or pd.notna(b)) else pd.NaT
    )
    merged["ingested_at"] = merged["ingested_at"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # 3. obs_* fields come from historical data only
    merged["obs_tmax_c"]    = merged.get("obs_tmax_c_hist")
    merged["obs_tmin_c"]    = merged.get("obs_tmin_c_hist")
    merged["obs_precip_mm"] = merged.get("obs_precip_mm_hist")

    # 4. fc_* fields come from forecast data only
    merged["fc_tmax_c"]     = merged.get("fc_tmax_c_fc")
    merged["fc_tmin_c"]     = merged.get("fc_tmin_c_fc")
    merged["fc_precip_mm"]  = merged.get("fc_precip_mm_fc")

    # 5. Resolve row type (data_type) and source
    has_hist = merged["obs_tmax_c"].notna() | merged["obs_tmin_c"].notna() | merged["obs_precip_mm"].notna()
    has_fc   = merged["fc_tmax_c"].notna()  | merged["fc_tmin_c"].notna()  | merged["fc_precip_mm"].notna()

    # If historical exists, mark as historical/ECCC, otherwise forecast/OpenMeteo
    merged["data_type"] = np.where(has_hist, "historical",
                           np.where(has_fc, "forecast", "unknown"))
    merged["source"] = np.where(has_hist, merged.get("source_hist"),
                         np.where(has_fc, merged.get("source_fc"), np.nan))

    # 6. Remove temporary merged columns (_hist, _fc)
    drop_cols = [c for c in merged.columns if c.endswith("_hist") or c.endswith("_fc")]
    merged = merged.drop(columns=drop_cols, errors="ignore")

    # 7. Finalize schema order and formatting
    return to_flat_schema(merged)
