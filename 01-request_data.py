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
import 01-helpers
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

tableHeadersExport = [
    'CTY_CODE',
    'CTY_NAME',
    'ALL_VAL_MO',
    'QTY_1_MO',
    'QTY_1_MO_FLAG',
    'UNIT_QY1',
    'QTY_2_MO',
    'QTY_2_MO_FLAG',
    'UNIT_QY2',
    'UNIT_QY2',
    'AIR_VAL_MO', # air value
    'AIR_WGT_MO', # air weight value
    'VES_VAL_MO', # vessel value
    'VES_WGT_MO',
    'CNT_VAL_MO', # containerized vessel value
    'CNT_WGT_MO',
    'MONTH',
    'SUMMARY_LVL',
    'DF',
    'COMM_LVL',
    'E_COMMODITY_LDESC'# ,
    # 'PORT',
    # 'PORT_NAME'
]

tableHeadersImport = [
    'CTY_CODE',
    'CTY_NAME',
    'GEN_CHA_MO',
    'GEN_VAL_MO',
    'GEN_QY1_MO',
    'GEN_QY1_MO_FLAG',
    'UNIT_QY1',
    'GEN_QY2_MO',
    'GEN_QY2_MO_FLAG',
    'UNIT_QY2',
    'AIR_VAL_MO', # air value
    'AIR_WGT_MO', # air weight value
    'VES_VAL_MO', # vessel value
    'VES_WGT_MO', # vessel value weight
    'CNT_VAL_MO', # containerized vessel value
    'CNT_WGT_MO', # containerized vessel weight value
    'MONTH',
    'SUMMARY_LVL',
    'I_COMMODITY_LDESC'# ,
    # 'PORT',
    # 'PORT_NAME'
    #'DF'
]

years = list(range(2013, 2023))


ctyCodes = [
    "5700", # China
    "5830", # Taiwan
    "5820", # Hong Kong
    "5660", # Macau
    "4621" # Russia
]

hsLvl = 'HS10'

# Get all relevant HS Codes from files

inFilePath = 'seafoodLvl10Codes.csv'
inF = open(inFilePath, 'r')
lines = inF.readlines()
inF.close()

seafoodHScodes = []

for line in lines:
    line = line.rstrip('\n')
    seafoodHScodes.append(line)

#seafoodHScodes = ['0301110020']

outdir = "rus_chn_20230928"
exports_dir = os.path.join(outdir, "exports")
imports_dir = os.path.join(outdir, "imports")

if not os.path.exists(exports_dir):
    os.makedirs(exports_dir)

if not os.path.exists(imports_dir):
    os.makedirs(imports_dir)

for year in years:
    print(f'hs codes are being searched for {year}')

    for hsCode in seafoodHScodes:

        print(f'YEAR: {year} HS CODE: {hsCode}')

        try:
            # INTL and Domestic export trades
            exports = helpers.getTradeRecords('export', EXPORT_URL, tableHeadersExport, [hsCode], hsLvl, [year], ctyCodes, API_KEY)
        except:
            exports = None
            error_log_file = os.path.join(outdir, "error_log.txt")
            error_f = open(error_log_file, "a+")
            error_f.write(f'export, {year}, {hsCode}\n')
            error_f.close()

        exportFile = ''
        if exports != None:
            exportFile = helpers.makeCSV(exports)
        print('retrieved exports')

        exports_year_dir = os.path.join(exports_dir, str(year))
        imports_year_dir = os.path.join(imports_dir, str(year))

        if not os.path.exists(exports_year_dir):
            os.makedirs(exports_year_dir)
        
        if not os.path.exists(imports_year_dir):
            os.makedirs(imports_year_dir)

        fOutName = os.path.join(exports_year_dir, f'{hsCode}_{str(year)}.csv')
        fOut = open(fOutName, 'w')
        fOut.write(exportFile)
        fOut.close()

        time.sleep(1)

        try:
            imports = helpers.getTradeRecords('import', IMPORT_URL, tableHeadersImport, [hsCode], hsLvl, [year], ctyCodes, API_KEY)
        except:
            imports = None
            error_log_file = os.path.join(outdir, "error_log.txt")
            error_f = open(error_log_file, "a+")
            error_f.write(f'import, {year}, {hsCode}\n')
            error_f.close()
        
        importFile = ''
        if imports != None:
            importFile = helpers.makeCSV(imports)
        print('retrieved imports')

        importFilePath = os.path.join(imports_year_dir, f'{hsCode}_{str(year)}.csv')
        importF = open(importFilePath, 'w')
        importF.write(importFile)
        importF.close()

        time.sleep(1)
        

print("DONE")

