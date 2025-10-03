# run_etl.py
# Purpose: CLI entrypoint to run the full ETL pipeline (Joe System Orchestration).

import argparse
from ingestion import ingest_eccc, ingest_openmeteo
from transformation import merge_flat 
from loading import to_csv, export
import pandas as pd
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


# --- Configuration ---
# NOTE: Use actual path and coordinates for assignment execution
DEFAULT_LAT = 43.79
DEFAULT_LON = -79.35
PREDICTION_API = 'http://prediction_system/api/v1/data' # Mock external API URL

def main():
    parser = argparse.ArgumentParser(description="Joe ETL: ECCC + Open-Meteo → flat 10-field CSV")
    parser.add_argument("--eccc_csv", default="data/eccc_station.csv",
                    help="Path to ECCC daily CSV file (default: data/eccc_station.csv)")
    parser.add_argument("--lat", type=float, default=DEFAULT_LAT, help="Station latitude")
    parser.add_argument("--lon", type=float, default=DEFAULT_LON, help="Station longitude")
    parser.add_argument("--days", type=int, default=16, help="Forecast horizon in days (default: 16)")
    parser.add_argument("--past_days", type=int, default=5, 
                        help="Historical days included in the Open-Meteo API call (default: 5)")
    
    parser.add_argument("--out", default="data/flat_weather.csv", help="Output CSV path")
    args = parser.parse_args()

    # 1. Ingestion Phase (E)
    print("--- 1. Ingestion Phase (E) ---")
    try:
        eccc_df = ingest_eccc(args.eccc_csv)
        # ingest_openmeteo 함수 호출 시 args.past_days를 전달합니다.
        fc_df = ingest_openmeteo(args.lat, args.lon, days=args.days, past_days=args.past_days)
        # IRawDatasets is implicitly passed here as the tuple (eccc_df, fc_df)
        print("Ingestion complete: Raw historical and forecast data acquired.")
    except Exception as e:
        print(f"Ingestion failed: {e}")
        return

    # 2. Transformation Phase (T)
    print("\n--- 2. Transformation Phase (T) ---")
    # Call the merge_flat function (IFlatRows is the return value)
    flat_df = merge_flat(eccc_df, fc_df)
    print(f"Rows (total): {len(flat_df)}")
    print(f"Forecast rows present (fc_tmax_c notna): {flat_df['fc_tmax_c'].notna().sum()}")
    print(f"Unique dates: {flat_df['date'].nunique()}")
    print("\nForecast-filled sample:")
    print(flat_df[flat_df["fc_tmax_c"].notna()].head(5).to_string(index=False))
    print(f"Transformation complete. {len(flat_df)} unified rows prepared.")
    
    # 3. Loading Phase (L)
    print("\n--- 3. Loading Phase (L) ---")
    
    # A. Persistence to file (for testing/debugging)
    out_path = to_csv(flat_df, args.out)

    # B. Export to external system (IPredictionPort implementation)
    export(flat_df, api_url=PREDICTION_API)
    
    print("\n--- ETL Pipeline Execution Finished ---")
    print(f"Final output file: {out_path}")
    print(flat_df.to_string(index=False, header=True))


if __name__ == "__main__":
    main()