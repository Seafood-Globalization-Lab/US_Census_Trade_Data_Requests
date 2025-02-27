"""
helpers.py

Purpose:
- Contains utility functions for data processing.

Key Functions:
- Data Filtering: Filters datasets based on Harmonized System (HS) codes.
- CSV Creation: Generates CSV-formatted strings.
- Header Construction: Builds column headers for structured datasets.

Execution:
- This script is **not standalone**; it is used by `01-request_data.py` and `02-aggregate_data.py`.
"""

import requests
import json
import csv
import datetime
import os

# Ensure the output directory exists
outdir = "output"
os.makedirs(outdir, exist_ok=True)  #  Creates "output" directory if it doesn't exist

# Filter data based on HS code
def filterData(data, hsLvl, hsCode):
    filteredData = []

    for row in data:
        if str(row[0])[:hsLvl] == hsCode:
            filteredData.append(row)
    
    return filteredData

# Create CSV-formatted string
def makeCSV(data):
    csvStr = ''

    for row in data:
        for cell in row:
            csvStr += (str(cell)).replace(',', '')
            csvStr += ','
        csvStr = csvStr[:-1]
        csvStr += '\n'
    
    return csvStr

# Build column headers
def buildColHeaders(colHeaders):

    colNames = ''

    for colName in colHeaders:
        colNames += colName
        colNames += ','
    
    colNames = colNames[:-1]

    return colNames

# Build URL for HS codes
def buildHS_Codes(tradeType, hsCodes, commLvl):

    commodityType = ''
    if tradeType == 'export':
        commodityType = 'E_COMMODITY'
    elif tradeType == 'import':
        commodityType = 'I_COMMODITY'
    
    hsURL = 'COMM_LVL=' + commLvl + '&'
    #hsURL = hsURL + commodityType + '='
    print(type(hsCodes))
    for hsCode in hsCodes:
        hsURL = hsURL + commodityType + '=' + hsCode + '&'
    hsURL = hsURL[:-1]
    return hsURL

# Build URL for years
def buildYears(years):
    yearURL = ''

    for year in years:
        yearURL = yearURL + 'YEAR=' + str(year) + '&'
    yearURL = yearURL[:-1]
    return yearURL

# Build URL for country codes
def buildCtyCodes(ctyCodes):
    if not ctyCodes:  # If ctyCodes is empty, return an empty string (no filter)
        return ""
    
    ctyCodeURL = "&".join(f"CTY_CODE={str(ctyCode)}" for ctyCode in ctyCodes)
    return ctyCodeURL


def getTradeRecords(tradeType, tradeURL, colHeaders, hsCodes, hsLvl, years, ctyCodes, apiKey):
    """Fetch trade records from the Census API and return (data, HTTP status)."""
    
    # Construct full URL
    fullURL = tradeURL + '?get='
    fullURL += buildColHeaders(colHeaders)
    fullURL += '&' + buildHS_Codes(tradeType, hsCodes, hsLvl)
    fullURL += '&' + buildYears(years)
    fullURL += '&' + buildCtyCodes(ctyCodes)
    fullURL += '&key=' + apiKey

    print(f"Requesting: {fullURL}")  # Debugging

    # Request data from API
    try:
        resp = requests.get(fullURL)
        print(f"HTTP Response Status: {resp.status_code}")  # Debugging

        # If the request was successful (200), attempt to parse response
        if resp.status_code == 200:
            if not resp.text.strip():  # Check if response is empty
                print(f"Warning: Empty response from API for {hsCodes}, {years}")
                return None, resp.status_code  # ✅ Return (None, HTTP status)

            try:
                tradeRecords = resp.json()
                return tradeRecords, resp.status_code  # ✅ Return (data, HTTP status)
            except json.JSONDecodeError as e:
                print(f"JSONDecodeError for {hsCodes}, {years}: {e}")
                print(f"Raw API Response (truncated):\n{resp.text[:300]}...\n")
                return None, resp.status_code  # ✅ Return (None, HTTP status)

        # If status is not 200, return None + status
        return None, resp.status_code  # ✅ Return (None, HTTP status)

    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None, -1  # ✅ Return (None, -1) for network-related failures


# Format country codes
def formatCountryCodes(inFP, outFP):
    f = open(inFP, 'r')
    lines = f.readlines()
    f.close()

    codes = []

    # remove newline characters
    for line in lines:
        codes.append(line[:4])
    
    f = open(outFP, 'w')
    f.write('code,\n')
    for code in codes:
        if code != codes[-1]:
            f.write(code + ',\n')
        else:
            f.write(code)
    f.close()

# Log request details
def log_request(year, hsCode, dataset, http_status, request_url):
    """Append request details to request_log.csv"""
    with open(log_file, "a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([datetime.datetime.utcnow(), year, hsCode, dataset, http_status, request_url])

