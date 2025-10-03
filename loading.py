# loading.py
# Purpose: Persist the final flat rows and export to the prediction system.
# Implements: IPredictionPort (Required interface)

import pandas as pd
from pathlib import Path
import requests

# Default URL for the external system (can be overridden via main script)
PREDICTION_SYSTEM_URL = 'http://prediction_system/api/v1/data' 

def to_csv(df: pd.DataFrame, out_path: str) -> str:
    """Persists the final DataFrame to a CSV file."""
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    return str(out.resolve())

def export(df: pd.DataFrame, api_url: str = PREDICTION_SYSTEM_URL):
    """IPredictionPort implementation: Transmits the unified data to the external system."""
    print(f"\n-> Loading: Attempting to transmit {len(df)} rows to external system...")
    
    # Convert data to JSON payload
    payload = df.to_json(orient='records', date_format='iso')
    
    try:
        # Simulate API call (actual request commented out)
        print(f"Simulating API call: POST {api_url}")
        # response = requests.post(api_url, data=payload, headers={'Content-Type': 'application/json'})
        # response.raise_for_status()
        print("Successfully transmitted data! (Simulation)")
        
    except requests.exceptions.RequestException as e:
        print(f"Transmission failed: {e}")