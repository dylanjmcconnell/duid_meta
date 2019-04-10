import os
import pandas as pd
import requests
from sqlalchemy import create_engine
from duid_meta import CONFIG, MODULE_DIR, ENGINE

PATH = os.path.join(CONFIG['local_settings']['test_folder'],"testdb.db")
SQLITE = create_engine("sqlite:///{0}".format(PATH))
PYTHON = create_engine("mysql://{username}:{password}@{hostname}/meta?unix_socket={socket}".format(**CONFIG['python_sql']))

def load_facility(facility_id="Q019SIE001", filename="test_npi.csv"):
    #doesn't work
    url = "http://www.npi.gov.au/npidata/action/load/emission-by-individual-facility-result/criteria/state/QLD/year/2017/jurisdiction-facility/Q019SIE001?exportCsv=true"
    url = "https://data.gov.au/dataset/ds-dga-043f58e0-a188-4458-b61c-04e5b540aea4/distribution/dist-dga-258f16fc-5e91-40a9-8ffb-00d62027a036/details?q="
    r = requests.get(url)
    return r
    with open(filename, "w") as f:
        f.write(r.contents)

def load_npi_data(select=SQLITE):
    path = os.path.join(MODULE_DIR, "data","npi_data.csv")
    df = pd.read_csv(path)
    df.rename(columns={col_name:col_name.upper() for col_name in df}, inplace=True)

    #map subtances
    substance_keys = pd.read_sql("SELECT SUBSTANCE_NAME, ID FROM SUBSTANCE", con=select, index_col="SUBSTANCE_NAME")
    key_map = substance_keys.to_dict(orient='dict')['ID']
    df['SUBSTANCE_NAME'] = df['SUBSTANCE_NAME'].apply(lambda x: key_map[x])

    #map facilities    

    #duid_keys = pd.read_sql("SELECT DUID, ID FROM DU", con=select, index_col="DUID")
    #station_keys = pd.read_sql("SELECT DUID, STATIONID FROM DUDETAILSUMMARY", con=select, index_col="DUID")

    #key_map = station_keys.to_dict(orient='dict')['STATIONID']

    #dx['SID'] = dx.ID.apply(lambda x: station_keymap(x, key_map))

    #key_map = substance_keys.to_dict(orient='dict')['ID']
    #df['SUBSTANCE_NAME'] = df['SUBSTANCE_NAME'].apply(lambda x: key_map[x])

    return df[['REPORT_ID', 'REPORT_YEAR', 'FACILITY_ID', 'SUBSTANCE_NAME', 'AIR_POINT_EMISSION_KG',
               'AIR_FUGITIVE_EMISSION_KG', 'AIR_TOTAL_EMISSION_KG', 'WATER_EMISSION_KG', 'LAND_EMISSION_KG']]

def station_keymap(x, keymap):
    try:
        return keymap[x]
    except:
        pass
