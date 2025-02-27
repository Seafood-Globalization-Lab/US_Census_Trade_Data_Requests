"""
01-request_data.py

Purpose:
- Fetches trade data from the U.S. Census Bureau's API.
- Saves retrieved data into structured directories.

Key Functions:
- Fetching Data: Sends API requests to retrieve import and export data.
- Saving Data: Stores responses as raw CSV files for later processing.

Execution:
- Run this script **first** to collect raw trade data.

Notes:
- User can include up to 50 variables in a single API query
"""


import os
import requests
import json
import pprint
import helpers
import time
import csv

# Dataset Title -  "Time Series International Trade: Monthly U.S. Exports by Harmonized System (HS) Code"
EXPORT_URL = 'https://api.census.gov/data/timeseries/intltrade/exports/hs'
# Dataset Title - "Time Series International Trade: Monthly U.S. Imports by Harmonized System (HS) Code"
IMPORT_URL = 'https://api.census.gov/data/timeseries/intltrade/imports/hs'

# read in API key
f = open('census_api_key.txt', 'r')
API_KEY = f.read()
f.close()

# make sure there are no extra characters in key
API_KEY = API_KEY.rstrip('\n')

# Initialize the log file with headers if it doesn't exist
if not os.path.exists(log_file):
    with open(log_file, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["date", "year", "hs_code", "dataset", "http_status", "request_url"])

# define which variables we want to pull

# Census Data API: Variables in /data/timeseries/intltrade/exports/hs/variables
# description table https://api.census.gov/data/timeseries/intltrade/exports/hs/variables.html
tableHeadersExport = [
    'CTY_CODE', # 4-character Country Code
    'CTY_NAME', # 50-character Country Name
    'ALL_VAL_MO', # 15-digit Total Value
    'QTY_1_MO', # 15-digit Quantity 1
    'QTY_1_MO_FLAG', # 1-character True Zero Flag for Quantity 1
    'UNIT_QY1', # 3-character Export Unit of Quantity 1
    'QTY_2_MO', # 	15-digit Quantity 2
    'QTY_2_MO_FLAG', # 1-character True Zero Flag for Quantity 2
    'UNIT_QY2', # 3-character Export Unit of Quantity 2
    'UNIT_QY2', # 3-character Export Unit of Quantity 2
    'AIR_VAL_MO', # 15-digit Air Value
    'AIR_WGT_MO', # 15-digit Air Shipping Weight
    'VES_VAL_MO', # vessel value # 15-digit Vessel Value
    'VES_WGT_MO', # 15-digit Vessel Shipping Weight
    'CNT_VAL_MO', # 15-digit Containerized Vessel Value
    'CNT_WGT_MO', # 15-digit Containerized Vessel Shipping Weight
    'MONTH', # 2-character Month
    'SUMMARY_LVL', # Detail ('DET') or Country Grouping ('CGP') indicator
    'DF', # 1-character Domestic or Foreign Code
    'COMM_LVL', # 4-character aggregation levels for commodity code. HS2=2-digit HS totals. HS4=4-digit HS totals. HS6=6-digit HS totals. HS10=10-digit HS totals.
    'E_COMMODITY_LDESC'# 150-character Export Harmonized Code Description ,
    # 'PORT',
    # 'PORT_NAME'
]

# Census Data API: Variables in /data/timeseries/intltrade/imports/hs/variables
# from table variable descriptions: https://api.census.gov/data/timeseries/intltrade/imports/hs/variables.html
tableHeadersImport = [
    'CTY_CODE', # 4-character Country Code
    'CTY_NAME', # 50-character Country Name
    'GEN_CHA_MO', # 15-digit General Imports, Charges
    'GEN_VAL_MO', # 15-digit General Imports, Total Value
    'GEN_QY1_MO', # 15-digit General Imports, Quantity 1
    'GEN_QY1_MO_FLAG', # 1-character True Zero Flag for General Imports, Quantity 2
    'UNIT_QY1', # 3-character Import Unit of Quantity 1
    'GEN_QY2_MO', # 15-digit General Imports, Quantity 2
    'GEN_QY2_MO_FLAG', # 1-character True Zero Flag for Year-to-Date General Imports, Quantity 3
    'UNIT_QY2', # 3-character Import Unit of Quantity 2
    'AIR_VAL_MO', # 15-digit Air Value
    'AIR_WGT_MO', # 15-digit Air Shipping Weight
    'VES_VAL_MO', # 15-digit Vessel Value
    'VES_WGT_MO', # 15-digit Vessel Shipping Weight
    'CNT_VAL_MO', # 15-digit Containerized Vessel Value
    'CNT_WGT_MO', # 15-digit Containerized Vessel Shipping Weight
    'MONTH', # 2-character Month
    'SUMMARY_LVL', # Detail ('DET') or Country Grouping ('CGP') indicator
    'I_COMMODITY_LDESC'# 150-character Import Harmonized Code Description ,
    # 'PORT',
    # 'PORT_NAME'
    #'DF'
]

# Define years to pull (descending order from 2024 to 1996)
years = list(range(2024, 1995, -1))

# define country codes to pull
ctyCodes = []
#ctyCodes = [
#    "5700", # China
#    "5830", # Taiwan
#    "5820", # Hong Kong
#    "5660", # Macau
#    "4621" # Russia
#]
# define HS level to pull
hsLvl = 'HS10'

# Get all relevant HS Codes from files
inFilePath = 'data/seafoodLvl10Codes.csv'
inF = open(inFilePath, 'r')
lines = inF.readlines()
inF.close()


seafoodHScodes = []

# remove newline characters
for line in lines:
    line = line.rstrip('\n')
    seafoodHScodes.append(line)

#seafoodHScodes = ['0301110020']

# create directories to store data
outdir = "rus_chn_20250225"
exports_dir = os.path.join(outdir, "exports")
imports_dir = os.path.join(outdir, "imports")

# create directories if they don't exist
if not os.path.exists(exports_dir):
    os.makedirs(exports_dir)

if not os.path.exists(imports_dir):
    os.makedirs(imports_dir)

# loop through years and HS codes to pull data
for year in years:
    print(f'------------------------------------------- \nhs codes are being searched for {year}')

    for hsCode in seafoodHScodes:

        print(f'****\nYEAR: {year} HS CODE: {hsCode}')

        # Construct Export Request URL (without API key)
        export_url = f"{EXPORT_URL}?get={','.join(tableHeadersExport)}&COMM_LVL={hsLvl}&E_COMMODITY={hsCode}&YEAR={year}"
        if ctyCodes:
            export_url += "&" + "&".join(f"CTY_CODE={code}" for code in ctyCodes)

        # Fetch export data & HTTP status
        exports, export_status = helpers.getTradeRecords('export', EXPORT_URL, tableHeadersExport, [hsCode], hsLvl, [year], ctyCodes, API_KEY)
        helpers.log_request(year, hsCode, "export", export_status, export_url)  # ✅ Log export request

        # Handle API response
        if exports:
            print(f'Exports - Successfully retrieved for {hsCode} in {year}')
            exportFile = helpers.makeCSV(exports)
            exports_year_dir = os.path.join(exports_dir, str(year))
            os.makedirs(exports_year_dir, exist_ok=True)

            # Write to file
            exportFilePath = os.path.join(exports_year_dir, f'{hsCode}_{year}.csv')
            with open(exportFilePath, 'w') as fOut:
                fOut.write(exportFile)
        else:
            print(f'Exports - No data available for {hsCode} in {year}. Skipping file creation.')

        time.sleep(1)  # Prevent hitting API rate limits

        # Construct Import Request URL (without API key)
        import_url = f"{IMPORT_URL}?get={','.join(tableHeadersImport)}&COMM_LVL={hsLvl}&I_COMMODITY={hsCode}&YEAR={year}"
        if ctyCodes:
            import_url += "&" + "&".join(f"CTY_CODE={code}" for code in ctyCodes)

        # Fetch import data & HTTP status
        imports, import_status = helpers.getTradeRecords('import', IMPORT_URL, tableHeadersImport, [hsCode], hsLvl, [year], ctyCodes, API_KEY)
        helpers.log_request(year, hsCode, "import", import_status, import_url)  # ✅ Log import request

        # Handle API response
        if imports:
            print(f'Imports - Successfully retrieved for {hsCode} in {year}')
            importFile = helpers.makeCSV(imports)
            imports_year_dir = os.path.join(imports_dir, str(year))
            os.makedirs(imports_year_dir, exist_ok=True)

            # Write to file
            importFilePath = os.path.join(imports_year_dir, f'{hsCode}_{year}.csv')
            with open(importFilePath, 'w') as importF:
                importF.write(importFile)
        else:
            print(f'Imports - No data available for {hsCode} in {year}. Skipping file creation.')

        time.sleep(1)  # Prevent hitting API rate limits

print("DONE")



