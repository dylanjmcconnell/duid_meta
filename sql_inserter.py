import pandas as pd
import datetime
import os
from duid_meta import CONFIG, MODULE_DIR, mmsds_reader, display_names
from sqlalchemy import create_engine

PATH = os.path.join(CONFIG['local_settings']['test_folder'],"testdb.db")
SQLITE = create_engine("sqlite:///{0}".format(PATH))
PYTHON = create_engine("mysql://{username}:{password}@{hostname}/meta?unix_socket={socket}".format(**CONFIG['python_sql']))

#for legacy duid mapping
legacy = create_engine("mysql://select:marblemysql@marble.earthsci.unimelb.edu.au/nemweb_meta")

def key_mapper(table="PARTICIPANTCLASS", col="PARTICIPANTCLASSID", engine=SQLITE):
    sql = "SELECT {1}, ID FROM {0}".format(table, col)
    df = pd.read_sql(sql, con=engine, index_col="{0}".format(col))
    return df.to_dict(orient='dict')['ID']

def populate_regions(engine=SQLITE):
    mapping = {1: 'NSW1', 2 : 'QLD1', 3:'SA1',  4 : 'TAS1', 5:'VIC1', 6: 'SNOWY1'}
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

def populate_stations(engine=SQLITE, y=2019, m=9):
    state_keys = pd.read_sql("SELECT STATE as _KEY, ID FROM STATE UNION SELECT STATENAME as _KEY, ID FROM STATE", con=engine, index_col="_KEY")
    key_map = state_keys.to_dict(orient='dict')['ID']

    df = mmsds_reader.download(dataset="station", y=y, m=m)
    df.STATE = df.STATE.apply(lambda x: key_map[x])
    for string in ['COOMA', 'BRISBANE', 'ADELAIDE', 'PORTLAND']:
        df.loc[df.POSTCODE==string, "POSTCODE"] = pd.np.nan

    dx = df[['STATIONID', 'STATIONNAME', 'STATE', 'POSTCODE']].copy()
    dx['STATIONNAME'] = dx['STATIONNAME'].apply(str.strip)
    dx['DISPLAYNAME'] = df['STATIONNAME'].apply(display_names.display_names)
    dx.sort_values("STATIONID", inplace=True)
    df_latlon =  load_latlon()

    df_comb = dx.merge(df_latlon, on="STATIONID", how="left")
    df_comb.to_sql("STATION", con=engine, index=False, if_exists='append')

def populate_participants(engine=SQLITE, y=2019, m=9):
    key_map = key_mapper("PARTICIPANTCLASS", engine=engine)

    df = mmsds_reader.download(dataset="participant", y=y, m=m)
    df.PARTICIPANTCLASSID = df.PARTICIPANTCLASSID.apply(lambda x: key_map[x])
    dx = df[['PARTICIPANTID', 'PARTICIPANTCLASSID', 'NAME']]
    dx.to_sql("PARTICIPANT", con=engine, index=False, if_exists='append')

def populate_connection_points(engine=SQLITE, y=2019, m=9):
    df = mmsds_reader.download(dataset="dudetail", y=y, m=m)
    df_conn = df.CONNECTIONPOINTID.copy().drop_duplicates()
    df_conn.sort_values(inplace=True)
    df_conn.to_sql("CONNECTIONPOINT", con=engine, index=False, if_exists='append')

def load_latlon():
    df = pd.read_csv(CONFIG['local_settings']['latlon_data'])
    dx = df[['STATIONID', 'Latitude', 'Longitude']].drop_duplicates()
    return dx.rename(columns={'Latitude': "LATITUDE", 'Longitude': "LONGITUDE"})

def populate_simple_tables(engine=SQLITE):
    tables = {"STARTTYPE": ['FAST', 'NOT DISPATCHED', 'SLOW', pd.np.nan],
              "DISPATCHTYPE": ['GENERATOR', 'LOAD'],
              "SCHEDULE_TYPE": ['NON-SCHEDULED', 'SCHEDULED', 'SEMI-SCHEDULED'],
              "STATUS" : ['COMMISSIONED', 'DECOMMISSIONED', 'COMMITTED'],
              "CO2E_ENERGY_SOURCE" : ['Black coal', 'Brown coal', 'Hydro', pd.np.nan, 'Natural Gas (Pipeline)',
                                     'Landfill biogas methane', 'Bagasse', 'Coal seam methane', 'Wind',
                                     'Diesel oil', 'Solar', 'Primary solid biomass fuels',
                                     'Coal mine waste gas', 'Battery Storage', 'Other Biofuels',
                                     'Kerosene - non aviation', 'Other solid fossil fuels',
                                     'Biomass and industrial materials'],
              "CO2E_DATA_SOURCE" :   ['NTNDP 2011', 'NTNDP 2014', 'NTNDP 2016', pd.np.nan,
                                     'Estimate - NGA 2016', 'Estimate - NGA 2015', 'Estimated',
                                     'Estimate - Other', 'Estimate - NGA 2012', 'Estimate - NGA 2014',
                                     'Estimate - NGA 2011', 'NTNDP 2015', 'Estimate - NGA 2013', 'NGA 2018',
                                     'ISP 2018', 'On Exclusion List', 'Excluded NMNS']}

    for table in tables:
        df = pd.DataFrame(tables[table], columns = [table])
        df.to_sql(table, con=engine, index=False, if_exists='append')

    df = pd.DataFrame(['MARKET PARTICIPANT', 'SPECIAL PARTICIPANT', 'POOL PARTICIPANT','NONMARKET'], columns = ["PARTICIPANTCLASSID"])
    df.to_sql("PARTICIPANTCLASS", con=engine, index=False, if_exists='append')

def populate_station_alias(engine=SQLITE):
    id_key_map = key_mapper("STATION", "STATIONID")    
    path = os.path.join(MODULE_DIR, "data","station_alias.csv")
    df = pd.read_csv(path)
    df.STATIONID = df.STATIONID.apply(lambda x: id_key_map[x])
    df[['STATION_ALIAS', 'STATIONID']].to_sql("STATION_ALIAS", con=engine, index=False, if_exists='append')


def populate_substance_ids(engine=SQLITE):
    path = os.path.join(MODULE_DIR, "data","substance_id_name.csv")
    df = pd.read_csv(path)
    df.rename(columns = {"substance_id" : "ID", "substance_name" : "SUBSTANCE_NAME"}, inplace=True)
    df.to_sql("SUBSTANCE", con=engine, index=False, if_exists='append')

def populate_dudetailsummary(engine=SQLITE, y=2019, m=9):
    df = mmsds_reader.download(dataset="dudetailsummary", y=y, m=m)
    cols = ['DUID',  'REGIONID', 'STATIONID', 'PARTICIPANTID', 'CONNECTIONPOINTID', 
            'DISPATCHTYPE', 'SCHEDULE_TYPE', 'STARTTYPE',  
            'TRANSMISSIONLOSSFACTOR', 'DISTRIBUTIONLOSSFACTOR', 
            'MIN_RAMP_RATE_UP', 'MIN_RAMP_RATE_DOWN', 'MAX_RAMP_RATE_UP', 'MAX_RAMP_RATE_DOWN',
            'IS_AGGREGATED', 'START_DATE', 'END_DATE', 'LASTCHANGED']

    for col in ['START_DATE', 'END_DATE', 'LASTCHANGED']:
        df[col] = df[col].apply(lambda x: date_parse(x))

    for id_table, column in {"STATION": "STATIONID",    
                            "REGION":"REGIONID",
                            "PARTICIPANT":"PARTICIPANTID",
                            "CONNECTIONPOINT": "CONNECTIONPOINTID",
                            "DISPATCHTYPE": "DISPATCHTYPE",
                            "SCHEDULE_TYPE": "SCHEDULE_TYPE",
                            "DU": "DUID"}.items():
        id_key_map = key_mapper(id_table, column)    
        df[column] = df[column].apply(lambda x: id_key_map[x])    

    id_key_map = key_mapper("STARTTYPE", "STARTTYPE")
    df['STARTTYPE'] = df['STARTTYPE'].apply(lambda x: nan_parse(id_key_map, x))
    df[cols].to_sql("DUDETAILSUMMARY", con=engine, index=False, if_exists='append')

def populate_genunits(engine=SQLITE, y=2019, m=9):
    df = mmsds_reader.download(dataset="genunits", y=y, m=m)
    cols = ['GENSETID', 'STATIONID', 'CDINDICATOR', 'AGCFLAG', 'SPINNINGFLAG',
            'VOLTLEVEL', 'REGISTEREDCAPACITY', 'STARTTYPE',
            'MKTGENERATORIND', 'NORMALSTATUS', 'MAXCAPACITY', 'GENSETTYPE',
            'LASTCHANGED', 'CO2E_EMISSIONS_FACTOR',
            'CO2E_ENERGY_SOURCE', 'CO2E_DATA_SOURCE']

    df = df[~df.GENSETID.isin(["GE01","GE03","GK03","GE04","GE02","GK04","GH01","GH02","GK01","GK02"])]
    gen_unit_map(df)

    for col in ['LASTCHANGED']:
        df[col] = df[col].apply(lambda x: date_parse(x))

    for col in ['CDINDICATOR', 'AGCFLAG', 'SPINNINGFLAG', 'MKTGENERATORIND', 'NORMALSTATUS']:
        df[col] = df[col].apply(lambda x: True if x == "Y" else False)

    id_key_map = key_mapper("DISPATCHTYPE", "DISPATCHTYPE")    
    df["GENSETTYPE"] = df["GENSETTYPE"].apply(lambda x: id_key_map[x])    

    id_key_map = key_mapper("GENSET", "GENSETID")    
    df["GENSETID"] = df["GENSETID"].apply(lambda x: id_key_map[x])


    id_key_map = key_mapper("STATION", "STATIONID")
    df["STATIONID"] = df["STATIONID"].apply(lambda x: id_key_map[x])

    for id_table, column in {"CO2E_ENERGY_SOURCE": "CO2E_ENERGY_SOURCE",
                             "CO2E_DATA_SOURCE": "CO2E_DATA_SOURCE",
                             "STARTTYPE": "STARTTYPE"}.items():
        id_key_map = key_mapper(id_table, column)    
        df[column] = df[column].apply(lambda x: nan_parse(id_key_map,x))

    df[cols].to_sql("GENUNITS", con=engine, index=False, if_exists='append')

def gen_unit_map(df, engine=SQLITE):
    sql = "SELECT DU.DUID, S.STATIONID FROM DUDETAILSUMMARY DS "\
          "INNER JOIN STATION S ON S.ID = DS.STATIONID "\
          "INNER JOIN DU ON DU.ID = DS.DUID"
    df_map = pd.read_sql(sql, con=engine, index_col="DUID")
    duid_station_map = df_map.to_dict(orient='dict')['STATIONID']

    path = os.path.join(MODULE_DIR, "data","genunit-station-map.csv")
    manual_map = pd.read_csv(path).set_index("GENSETID").to_dict()['STATIONID']

    df["STATIONID"] = df["GENSETID"].apply(lambda x: map_lambda(x, manual_map, duid_station_map))

def map_lambda(x, manual_map, duid_map):
    if x in duid_map:
        return duid_map[x]
    elif x in manual_map:
        return manual_map[x]
    else:
        print (x)
        raise Exception ("Unmapped Genset")

def date_parse(x):
    dt = datetime.datetime.strptime(x, "%Y/%m/%d %H:%M:%S")
    if dt.year == 2999:
        dt=dt.replace(year=2100)
    return dt

def nan_parse(id_key_map, x):
    try: 
        return id_key_map[x]
    except:
        return x

def populate_duid_table(engine=SQLITE, y=2019, m=9):
    df_ds = mmsds_reader.download(dataset="dudetailsummary", y=y, m=m)

    #all duids
    duid_key_map = key_mapper("FULL_REGISTER", "DUID", engine=legacy) 
    df_ds['ID'] = df_ds.DUID.apply(lambda x: duid_parse(duid_key_map,x))
    
    unique_duids = df_ds[['ID','DUID']].drop_duplicates()

    #duids with legacy ids    
    da = unique_duids[unique_duids.ID.notna()]
    da.to_sql("DU", con=engine, if_exists='append', index=None) 

    #new duids
    db = unique_duids[unique_duids.ID.isna()]
    db.to_sql("DU", con=engine, if_exists='append', index=None) 

def populate_genset_table(engine=SQLITE, y=2019, m=9):
    # could be added to genunit create
    df_g = mmsds_reader.download(dataset="genunits", y=y, m=m)

    #all duids 
    duid_key_map = key_mapper("FULL_REGISTER", "DUID", engine=legacy) 
    df_g['ID'] = df_g.GENSETID.apply(lambda x: duid_parse(duid_key_map,x))
    
    genunitid = df_g[['ID','GENSETID']]

    da = genunitid[genunitid.ID.notna()]
    da.to_sql("GENSET", con=engine, if_exists='append', index=None) 

    #new duids
    db = genunitid[genunitid.ID.isna()]
    db.to_sql("GENSET", con=engine, if_exists='append', index=None) 

def duid_parse(id_key_map, x):
    try: 
        return id_key_map[x]
    except:
        return pd.np.nan

def populate_operating_status(engine=SQLITE, y=2019, m=9):
    df_os = mmsds_reader.download(dataset='operatingstatus', y=y, m=m)
    station_key_map = key_mapper("STATION", "STATIONID") 
    df_os['STATIONID'] = df_os.STATIONID.apply(lambda x: station_key_map[x])

    status_key_map = key_mapper("STATUS", "STATUS") 
    df_os['STATUS'] = df_os['STATUS'].apply(lambda x: status_key_map[x])

    df_os[['STATIONID', 'STATUS', 'EFFECTIVEDATE']].to_sql("STATIONOPERATINGSTATUS", con=engine, if_exists='append', index=None) 

def make_all(engine=SQLITE, y=2019, m=11):
    populate_simple_tables(engine=engine)
    populate_regions(engine=engine)
    populate_states(engine=engine)
    populate_connection_points(engine=engine, y=y,m=m)
    populate_participants(engine=engine, y=y,m=m)
    populate_stations(engine=engine, y=y,m=m)
    populate_duid_table(engine=engine, y=y,m=m)
    populate_dudetailsummary(engine=engine, y=y,m=m)
    populate_genset_table(engine=engine, y=y,m=m)
    populate_genunits(engine=engine, y=y,m=m)
    populate_operating_status(engine=engine, y=y,m=m)
    populate_station_alias(engine=engine)
    populate_substance_ids(engine=engine)
