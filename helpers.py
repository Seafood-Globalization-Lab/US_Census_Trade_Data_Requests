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

def filterData(data, hsLvl, hsCode):
    filteredData = []

    for row in data:
        if str(row[0])[:hsLvl] == hsCode:
            filteredData.append(row)
    
    return filteredData

def makeCSV(data):
    csvStr = ''

    for row in data:
        for cell in row:
            csvStr += (str(cell)).replace(',', '')
            csvStr += ','
        csvStr = csvStr[:-1]
        csvStr += '\n'
    
    return csvStr

def buildColHeaders(colHeaders):

    colNames = ''

    for colName in colHeaders:
        colNames += colName
        colNames += ','
    
    colNames = colNames[:-1]

    return colNames

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

def buildYears(years):
    yearURL = ''

    for year in years:
        yearURL = yearURL + 'YEAR=' + str(year) + '&'
    yearURL = yearURL[:-1]
    return yearURL

def buildCtyCodes(ctyCodes):
    ctyCodeURL = ''

    if len(ctyCodes) > 0:
        for ctyCode in ctyCodes:
            ctyCodeURL = ctyCodeURL + 'CTY_CODE=' + str(ctyCode) + '&'
        
        ctyCodeURL = ctyCodeURL[:-1]

    return ctyCodeURL

def getTradeRecords(tradeType, tradeURL, colHeaders, hsCodes, hsLvl, years, ctyCodes, apiKey):

    fullURL = tradeURL + '?get='
    fullURL = fullURL + buildColHeaders(colHeaders)
    fullURL = fullURL + '&' + buildHS_Codes(tradeType, hsCodes, hsLvl)
    fullURL = fullURL + '&' + buildYears(years)
    fullURL = fullURL + '&' + buildCtyCodes(ctyCodes)
    fullURL = fullURL + '&key=' + apiKey

    print(f"Requesting: {fullURL}")  # Debugging

    try:
        resp = requests.get(fullURL)
        print(f"Response Status: {resp.status_code}")

        if resp.status_code == 200:
            if not resp.text.strip():  # Check if response is empty
                print(f"Warning: Empty response from API for {hsCodes}, {years}")
                return None

            try:
                tradeRecords = resp.json()
                return tradeRecords
            except json.JSONDecodeError as e:
                print(f"JSONDecodeError for {hsCodes}, {years}: {e}")
                print(f"Raw API Response (truncated):\n{resp.text[:300]}...\n")  # Print first 300 characters only
                return None
        else:
            print(f"API Error {resp.status_code}: {resp.text}")  # Print error message from API
            return None
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

def formatCountryCodes(inFP, outFP):
    f = open(inFP, 'r')
    lines = f.readlines()
    f.close()

    codes = []

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
