import pandas as pd
import os
from duid_meta import CONFIG, mmsds_reader
from sqlalchemy import create_engine

PATH = os.path.join(CONFIG['local_settings']['test_folder'],"testdb.db")
SQLITE = create_engine("sqlite:///{0}".format(PATH))
PYTHON = create_engine("mysql://{username}:{password}@{hostname}/meta?unix_socket={socket}".format(**CONFIG['python_sql']))

def populate_regionid(engine=SQLITE):
    mapping = {1: 'NSW1', 2 : 'QLD1', 3:'SA1',  4 : 'TAS1', 5:'VIC1'}
    df = pd.DataFrame.from_dict(mapping, orient="index")
    df.rename(columns={0: "REGIONID"},inplace=True)
    df.to_sql("REGION", con=engine, index=False, if_exists='append')

def populate_states(engine=SQLITE):
    region_keys = pd.read_sql("SELECT REGIONID as _KEY, ID FROM REGION", con=engine, index_col="_KEY")
    key_map = region_keys.to_dict(orient='dict')['ID']

    mapping = {'NSW': ["NSW1", "New South Wales"], 'QLD': ["QLD1", 'Queensland'], 'SA': ["SA1",'South Australia'], 'TAS': ["TAS1", "Tasmania"], 'VIC': ["VIC1", 'Victoria'], 'ACT': ["NSW1", "Australian Capital Territory"]}
    df = pd.DataFrame.from_dict(mapping, orient="index")
    df.reset_index(inplace=True)
    df.rename(columns={"index":"STATE", 0: "REGIONID", 1: "STATENAME"},inplace=True)

    df.REGIONID = df.REGIONID.apply(lambda x: key_map[x])
    df.to_sql("STATE", con=engine, index=False, if_exists='append')

def populate_stations(engine=SQLITE):
    state_keys = pd.read_sql("SELECT STATE as _KEY, ID FROM STATE UNION SELECT STATENAME as _KEY, ID FROM STATE", con=engine, index_col="_KEY")
    key_map = state_keys.to_dict(orient='dict')['ID']

    df = mmsds_reader.download(dataset="station", y=2019, m=2)
    df.STATE = df.STATE.apply(lambda x: key_map[x])
    for string in ['COOMA', 'BRISBANE', 'ADELAIDE', 'PORTLAND']:
        df.loc[df.POSTCODE==string, "POSTCODE"] = pd.np.nan

    dx = df[['STATIONID', 'STATIONNAME', 'STATE', 'POSTCODE']]
    df_latlon =  load_latlon()

    df_comb = dx.merge(df_latlon, on="STATIONID", how="left")
    df_comb.to_sql("STATION", con=engine, index=False, if_exists='append')

def load_latlon():
    df = pd.read_csv(CONFIG['local_settings']['latlon_data'])
    dx = df[['STATIONID', 'Latitude', 'Longitude']].drop_duplicates()
    return dx.rename(columns={'Latitude': "LATITUDE", 'Longitude': "LONGITUDE"})

def simple_tables(engine=SQLITE):    
    tables = {"STARTTYPE": ['FAST', 'NOT DISPATCHED', 'SLOW', pd.np.nan],
              "DISPATCHTYPE": ['GENERATOR', 'LOAD'],
              "SCHEDULE_TYPE": ['NON-SCHEDULED', 'SCHEDULED', 'SEMI-SCHEDULED'],
              "STATUS" : ['COMMISSIONED', 'DECOMMISSIONED'],
              "CO2E_ENERGY_SOURCE" : ['Black coal', 'Brown coal', 'Hydro', pd.np.nan, 'Natural Gas (Pipeline)',
                                     'Landfill biogas methane', 'Bagasse', 'Coal seam methane', 'Wind',
                                     'Diesel oil', 'Solar', 'Primary solid biomass fuels',
                                     'Coal mine waste gas', 'Battery Storage', 'Other Biofuels',
                                     'Kerosene - non aviation', 'Other solid fossil fuels',
                                     'Biomass and industrial materials'],
              "CO2E_DATA_SOURCE" :   ['NTNDP 2011', 'NTNDP 2014', 'NTNDP 2016', pd.np.nan,
                                     'Estimate - NGA 2016', 'Estimate - NGA 2015', 'Estimated',
                                     'Estimate - Other', 'Estimate - NGA 2012', 'Estimate - NGA 2014',
                                     'Estimate - NGA 2011', 'NTNDP 2015', 'Estimate - NGA 2013']}
    
    for table in tables:
        df = pd.DataFrame(tables[table], columns = [table])
        df.to_sql(table, con=engine, index=False, if_exists='append')
