"""Module for downloading excel sheets from the AEMO generator information page and loading
a selection of the sheets as pandas dataframes. The sheets below can be currently be loaded:
- New Developments
- Existing S & SS Generation
- Existing NS Generation
"""
from io import BytesIO
from collections import namedtuple
import datetime
import requests
import pandas as pd

BASE_URL = "https://www.aemo.com.au/-/media/Files/Electricity/NEM/Planning_and_Forecasting/"\
           "Generation_Information/{0}-{1}/Generation_Information_{2}_{3}_{1}.xlsx"

#Class factory function for sheet meta data for generator info spreadsheets
AemoSheets = namedtuple("sheet",
                        ["sheet_name",
                         "skiprows",
                         "skipfooter"])

SHEETS = {
    "new_developments": AemoSheets(
        sheet_name="New Developments",
        skiprows=1,
        skipfooter=2),
    "existing_s&ss": AemoSheets(
        sheet_name="Existing S & SS Generation",
        skiprows=1,
        skipfooter=4),
    "existing_ns": AemoSheets(
        sheet_name="Existing NS Generation",
        skiprows=1,
        skipfooter=2)
    }

def download_geninfo(regionid="NSW", date=datetime.datetime(2019, 1, 21)):
    """Downloads an excel sheet from AEMO generator information page, for particular region from a
    a particular date. Returns a BytesIO file-like object.
    """
    url = BASE_URL.format(date.strftime("%b"), date.year, regionid, date.strftime("%B"))
    response = requests.get(url)
    return BytesIO(response.content)

def load_sheet(file_obj, sheetid="new_developments"):
    """Returns a dataframe from a particular sheet ("sheetid" from a AEMO generator information
    file, or file-like object. Paramaters for reading the specifc sheetid are taken from the SHEETS
    dictionary
    """
    sheet = SHEETS[sheetid]
    file_obj.seek(0)
    dataframe = pd.read_excel(file_obj,
                              sheet_name=sheet.sheet_name,
                              skiprows=sheet.skiprows,
                              skip_footer=sheet.skipfooter)
    return dataframe

def load_date(date=datetime.datetime(2019, 1, 21)):
    """Loads all the sheets for from the excel files in the generator information page from a
    particular date for all the regions. Returns a dictionary (with form dict[regionid][sheetid])
    """
    data_dict = {}
    for regionid in ["NSW", "QLD", "SA", "TAS", "VIC"]:
        file_obj = download_geninfo(regionid=regionid, date=date)
        data_dict[regionid] = {sheetid: load_sheet(file_obj, sheetid) for sheetid in SHEETS}
    return data_dict
