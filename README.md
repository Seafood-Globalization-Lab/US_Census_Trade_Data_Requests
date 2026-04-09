# US Census Trade Data Pull

A set of Python scripts for pulling U.S. international trade data (imports and exports) from the U.S. Census Bureau API, organized by Harmonized System (HS) commodity codes.

## Purpose

This repository provides a lightweight pipeline for extracting monthly U.S. import and export trade data at the 10-digit HS code level from the [U.S. Census Bureau International Trade API](https://www.census.gov/data/developers/data-sets/international-trade.html). It is designed for research and analysis of specific commodity categories (e.g., seafood) across countries and years.

The pipeline:
- Fetches raw trade records from the Census API by HS code and year
- Logs each API request with status codes, response sizes, and record counts
- Aggregates raw CSV files into consolidated import and export datasets

### Data Pull Documentation

Use GitHub release tags to document discrete data pull events. Each release tag should capture:

- **Tag format:** `v<YYYYMMDD>-<descriptor>` (e.g., `v20250401-seafood-hs10`)
- **Tag notes should include:**
 - Date of pull
 - HS codes pulled
 - Year range covered
 - Country filters applied (if any)
 - Census API endpoint versions used
 - Any known data gaps or API errors logged in `request_log.csv`

> [!NOTE]
> A full pull of all 10-digit seafood HS codes from 1996–2024 for all countries would be tagged `v20250401-seafood-hs10-all-countries`.


### US Census Trade Background

This pipeline draws from two Census Bureau time series datasets:

| Dataset | Description | API Endpoint |
|---|---|---|
| **Exports by HS Code** | Monthly U.S. exports by 10-digit Harmonized System code | `data/timeseries/intltrade/exports/hs` |
| **Imports by HS Code** | Monthly U.S. imports by 10-digit Harmonized System code | `data/timeseries/intltrade/imports/hs` |

**Key concepts:**

- **Harmonized System (HS) Codes** — A standardized international nomenclature for classifying traded goods. This pipeline operates at the `HS10` level (10-digit codes), the most granular level available.
- **COMM_LVL** — Controls aggregation level in the API query. Set to `HS10` here for full commodity detail.
- **CTY_CODE** — 4-digit Census country codes. A full reference list is provided in `us_census_country_codes.txt`.
- **Export variables** (e.g., `ALL_VAL_MO`, `QTY_1_MO`, `VES_VAL_MO`) — Documented in `us_census_api_variables_data_timeseries_intltrade_exports_hs_variables.json`
- **Import variables** (e.g., `GEN_VAL_MO`, `GEN_QY1_MO`, `AIR_VAL_MO`) — Documented in `us_census_api_variables_data_timeseries_intltrade_imports_hs_variables.json`
- Data is available from **1996 to present**, released monthly with a ~2 month lag.

## Instructions to Pull Data

### Setup and Installation

**Prerequisites:**
- Python 3.8+
- A U.S. Census Bureau API key — register for free at [api.census.gov/data/key_signup.html](https://api.census.gov/data/key_signup.html)

**1. Clone the repository:**
```bash
git clone <repo-url>
cd <repo-directory>
```

**2. Install dependencies:**
```bash
pip install -r requirements.txt
```

**3. Add your Census API key:**

Create a plain text file named `census_api_key.txt` in the root directory and paste your API key as the only contents:
```
your_api_key_here
```

> [!TIP]
> `census_api_key.txt` is listed in `.gitignore` and will not be committed.

**4. Prepare your HS code input file:**

Create the file `data/seafoodLvl10Codes.csv` containing one 10-digit HS code per line:
```
0301110020
0301110040
0302110010
...
```

**5. (Optional) Filter by country:**

To restrict pulls to specific countries, uncomment and populate the `ctyCodes` list in `01-request_data.py` using the 4-digit Census country codes found in `us_census_country_codes.txt`. For example:
```python
ctyCodes = [
   "5700",  # China
   "5830",  # Taiwan
   "4621",  # Russia
]
```
Leave the list empty to pull all countries.

### Scripts to Run

Run the scripts in order:

**Step 1 — Fetch raw trade data:**
```bash
python 01-request_data.py
```
- Loops over all years (1996–2024) and all HS codes in the input file
- Makes separate API calls for imports and exports
- Saves each response as a CSV file organized by trade type and year
- Logs every request to `request_log.csv` including HTTP status, response size, and record count
- Includes a 1-second delay between requests to respect API rate limits

**Step 2 — Aggregate into combined datasets:**
```bash
python 02-aggregate_data.py
```
- Recursively finds all raw CSV files produced in Step 1
- Combines them into two consolidated files:
 - `output/imports_combined.csv`
 - `output/exports_combined.csv`

### Where to Store Data

The pipeline writes all output to the `output/` directory, which is excluded from version control via `.gitignore`.

```
output/
└── us-census-data-<YYYYMMDD>/
   ├── request_log.csv               # API request log for the pull
   ├── exports/
   │   ├── 1996/
   │   │   └── <hs_code>_1996.csv
   │   ├── 1997/
   │   └── ...
   └── imports/
       ├── 1996/
       │   └── <hs_code>_1996.csv
       ├── 1997/
       └── ...

output/
├── imports_combined.csv              # Aggregated imports (all years, all HS codes)
└── exports_combined.csv              # Aggregated exports (all years, all HS codes)
```

> **Note:** The `data/` directory (containing your HS code input files) is also excluded from version control. Store input files there locally but do not commit them unless they are intended to be part of the repository's public record.
