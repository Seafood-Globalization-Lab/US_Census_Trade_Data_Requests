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


# Request trade records
def getTradeRecords(tradeType, tradeURL, colHeaders, hsCodes, hsLvl, years, ctyCodes, apiKey):
    # Construct full URL
    fullURL = tradeURL + '?get='
    fullURL = fullURL + buildColHeaders(colHeaders)
    fullURL = fullURL + '&' + buildHS_Codes(tradeType, hsCodes, hsLvl)
    fullURL = fullURL + '&' + buildYears(years)
    fullURL = fullURL + '&' + buildCtyCodes(ctyCodes)
    fullURL = fullURL + '&key=' + apiKey

    print(f"Requesting: {fullURL}")  # Debugging

    # Request data from API
    try:
        # Send GET request
        resp = requests.get(fullURL)
        # Check response status
        print(f"HTTP Response Status: {resp.status_code}")
        # Check if response is successful
        if resp.status_code == 200:
            # Check if response is empty
            if not resp.text.strip():  # Check if response is empty
                print(f"Warning: Empty response from API for {hsCodes}, {years}")
                return None
            # Parse JSON response
            try:
                tradeRecords = resp.json()
                return tradeRecords
            # Handle JSON parsing error
            except json.JSONDecodeError as e:
                print(f"JSONDecodeError for {hsCodes}, {years}: {e}")
                print(f"Raw API Response (truncated):\n{resp.text[:300]}...\n")  # Print first 300 characters only
                return None
        # Handle non-200 response
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

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
