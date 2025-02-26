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
"""


import os
import requests
import json
import pprint
import helpers
import time

# international import and export trade URLs
EXPORT_URL = 'https://api.census.gov/data/timeseries/intltrade/exports/hs'
IMPORT_URL = 'https://api.census.gov/data/timeseries/intltrade/imports/hs'

# read in API key
f = open('census_api_key.txt', 'r')
API_KEY = f.read()
f.close()

# make sure there are no extra characters in key
API_KEY = API_KEY.rstrip('\n')

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

# define years to pull
years = list(range(2013, 2024))

# define country codes to pull
ctyCodes = [
    "5700", # China
    "5830", # Taiwan
    "5820", # Hong Kong
    "5660", # Macau
    "4621" # Russia
]
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
    print(f'hs codes are being searched for {year}')

    for hsCode in seafoodHScodes:

        print(f'YEAR: {year} HS CODE: {hsCode}')

# Fetch export data
        try:
            exports = helpers.getTradeRecords('export', EXPORT_URL, tableHeadersExport, [hsCode], hsLvl, [year], ctyCodes, API_KEY)
            if exports:
                print(f'Successfully retrieved exports for {hsCode} in {year}')
            else:
                print(f'No export data available for {hsCode} in {year}')
        # write to error log
        except Exception as e:
            exports = None
            print(f'Error retrieving exports for {hsCode} in {year}: {e}')
            with open(os.path.join(outdir, "error_log.txt"), "a+") as error_f:
                error_f.write(f'export, {year}, {hsCode}\n')

        # Save export data only if it's valid
        if exports:
            exportFile = helpers.makeCSV(exports)
            exports_year_dir = os.path.join(exports_dir, str(year))
            os.makedirs(exports_year_dir, exist_ok=True)

            # write to file
            exportFilePath = os.path.join(exports_year_dir, f'{hsCode}_{year}.csv')
            with open(exportFilePath, 'w') as fOut:
                fOut.write(exportFile)
        else:
            print(f'Skipping file creation for exports: {hsCode} in {year}')
        # wait 1 second before making next request
        time.sleep(1)

        # Fetch import data
        try:
            imports = helpers.getTradeRecords('import', IMPORT_URL, tableHeadersImport, [hsCode], hsLvl, [year], ctyCodes, API_KEY)
            if imports:
                print(f'Successfully retrieved imports for {hsCode} in {year}')
            else:
                print(f'No import data available for {hsCode} in {year}')
        # write to error log
        except Exception as e:
            imports = None
            print(f'Error retrieving imports for {hsCode} in {year}: {e}')
            with open(os.path.join(outdir, "error_log.txt"), "a+") as error_f:
                error_f.write(f'import, {year}, {hsCode}\n')

        # Save import data only if it's valid
        if imports:
            importFile = helpers.makeCSV(imports)
            imports_year_dir = os.path.join(imports_dir, str(year))
            os.makedirs(imports_year_dir, exist_ok=True)
            # write to file
            importFilePath = os.path.join(imports_year_dir, f'{hsCode}_{year}.csv')
            with open(importFilePath, 'w') as importF:
                importF.write(importFile)
        else:
            print(f'Skipping file creation for imports: {hsCode} in {year}')

        time.sleep(1)  # Prevent hitting API rate limits

print("DONE")


