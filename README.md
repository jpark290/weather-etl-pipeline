# Weather ETL Pipeline (Joe)

**Goal:** Ingest **historical** daily weather from **ECCC** (CSV) and **forecast** from **Open-Meteo** (API), **merge** them by date into a single **flat table** that a prediction system can consume.

* **Language:** Python (pandas, requests)
* **Pattern:** Pipeline (Ingestion → Transformation → Loading), Adapter (source normalization), Facade (single export entry)
* **Merge strategy:** **Full outer join by `date`** — one row per day with both observed (`obs_*`) and forecast (`fc_*`) features when available.

---

## 1) Repository Structure

```
WEATHER-ETL-PIPELINE/
├─ data/
│  ├─ eccc_station.csv         # Historical daily CSV (1 month from ECCC portal)
│  ├─ flat_weather.csv         # Output produced by run_etl.py (can be re-generated)
│  └─ sample_output.csv        # Example output committed for quick preview
├─ ingestion.py                # ECCC CSV + Open-Meteo API adapters (raw frames)
├─ transformation.py           # Merge (outer join) + flat 10-field schema
├─ loading.py                  # Persist to CSV and (simulated) export hook
├─ run_etl.py                  # CLI entrypoint/orchestrator
└─ requirements.txt
```

---

## 2) Data Model (Flat Table)

| Field           | Description                                                         |
| --------------- | ------------------------------------------------------------------- |
| `date`          | Observation or forecast date (`America/Toronto`)                    |
| `data_type`     | `historical` if observed values exist for the date, else `forecast` |
| `source`        | `ECCC` for observed rows, `OpenMeteo` for forecast-only rows        |
| `ingested_at`   | Latest ingestion timestamp for that date                            |
| `obs_tmax_c`    | Observed daily max temperature (°C)                                 |
| `obs_tmin_c`    | Observed daily min temperature (°C)                                 |
| `obs_precip_mm` | Observed daily total precipitation (mm)                             |
| `fc_tmax_c`     | Forecast daily max temperature (°C)                                 |
| `fc_tmin_c`     | Forecast daily min temperature (°C)                                 |
| `fc_precip_mm`  | Forecast daily total precipitation (mm)                             |

> **Note:** For dates where only one side exists (e.g., future dates), the other side’s fields are `NaN`.

---

## 3) How It Works

### Ingestion (`ingestion.py`)

* **ECCC CSV** → robust column mapping → normalized **historical** frame
* **Open-Meteo API** → `daily` endpoint → normalized **forecast** frame
* Both frames add `data_type`, `source`, `ingested_at` and initialize the “other side” fields as `NaN` for clean merging.

### Transformation (`transformation.py`)

* **Full outer join** on `date` (`merge_flat`), then:

  * Resolve metadata: `ingested_at = max(ingested_at_hist, ingested_at_fc)`
  * Prefer `historical/ECCC` for `data_type`/`source` when obs exists
  * Enforce the **10-field** flat schema and sort by date

### Loading (`loading.py`)

* Save to CSV (e.g., `data/flat_weather.csv`)
* Provide a simple `export()` hook (simulated POST) that acts as a Facade to an external prediction service

---

## 4) Run It

### 4.1 Install

```bash
# inside the repo root
pip install -r requirements.txt
```

### 4.2 Execute (Windows CMD / PowerShell / macOS / Linux)

```bash
# Minimal run (uses default path data/eccc_station.csv)
python run_etl.py --lat 45.53 --lon -78.27 --days 16 --past_days 10 --out data/flat_weather.csv
```

**Parameters**

* `--eccc_csv` : path to the downloaded ECCC daily CSV (default: `data/eccc_station.csv`)
* `--lat`, `--lon` : station coordinates (copy from the ECCC station you chose)
* `--days` : forecast horizon (e.g., 16)
* `--past_days` : how many **recent past days** to also request from Open-Meteo so the two sources **overlap** for the merge (e.g., 10)
* `--out` : output CSV path (default: `data/flat_weather.csv`)

The script prints quick diagnostics:

* Rows total, unique dates
* Count of rows where `fc_tmax_c` exists (so you can confirm forecasts arrived)
* A small **forecast-filled sample** for visual validation

---

## 5) Example Output (snippet)

`data/sample_output.csv` contains a full example. Below is a short excerpt:

```
date       data_type  source     ingested_at          obs_tmax_c  obs_tmin_c  obs_precip_mm  fc_tmax_c  fc_tmin_c  fc_precip_mm
2025-09-27 historical ECCC       2025-10-03 ...            21.6         7.9           0.0       21.2        9.1           0.0
2025-09-28 historical ECCC       2025-10-03 ...            22.4         5.4           0.0       21.0        9.5           0.0
2025-09-29 historical ECCC       2025-10-03 ...            24.7         2.4           0.0       23.3        5.2           0.0
2025-09-30 historical ECCC       2025-10-03 ...            19.1         2.3           0.0       18.8        6.7           0.0
2025-10-01 forecast   OpenMeteo  2025-10-03 ...             NaN         NaN           NaN       16.5        1.5           0.0
...
```

* **Past end-of-month dates** show **both** `obs_*` and `fc_*` (overlap via `--past_days`)
* **Future dates** show **forecast only** (`obs_*` are `NaN`)

This demonstrates the **merge** working as intended.

---

## 6) Why This Design (short)

* **Simple consumer view:** one **flat row per date**, both observed and forecast features if available
* **Loose coupling:** components communicate via clear interfaces (dataframes with known schema)
* **Flexible overlap:** `--past_days` lets you bring API **past days** to overlap with ECCC and **prove the merge**

---

## 7) Reproducing the Assignment Steps

1. Download one month of daily ECCC data for a station in Ontario → save as `data/eccc_station.csv`
2. Copy that station’s **lat/lon**
3. Run the pipeline with the same **lat/lon** and a suitable `--past_days` (e.g., 10) so the end of month overlaps
4. Check `data/flat_weather.csv` (or `data/sample_output.csv`) and the console diagnostics

---

## 8) Notes & References

* ECCC Historical Weather Portal: *(student picked station & month, CSV exported)*
* Open-Meteo API (free): [https://open-meteo.com/en/docs](https://open-meteo.com/en/docs)

> No credentials required. All calls are anonymous and free for the scope of this exercise.

---

## 9) Future Work (nice-to-have)

* Add unit tests for column mapping and merge rules
* Add `to_parquet()` and S3/DB loaders
* Wire the `export()` to a real prediction service (currently simulated)
* Add a small chart to visualize obs vs. fc trends around the overlap

---

**Author:** Haley
**Project Type:** ETL / Data Engineering Prototype (portfolio-ready)
