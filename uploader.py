import simplejson
import boto3
from io import BytesIO
import gzip
import shutil
import pandas as pd
import datetime
import os
from duid_meta import CONFIG, MODULE_DIR, mmsds_reader, display_names, npi
from sqlalchemy import create_engine

aws_settings = CONFIG['aws_settings']

PATH = os.path.join(CONFIG['local_settings']['test_folder'],"testdb.db")
SQLITE = create_engine("sqlite:///{0}".format(PATH))
PYTHON = create_engine("mysql://{username}:{password}@{hostname}/meta?unix_socket={socket}".format(**CONFIG['python_sql']))

legacy = create_engine("mysql://{username}:{password}@{hostname}/nemweb_meta?unix_socket={socket}".format(**CONFIG['basic_sql']))

npi_map_path = os.path.join(MODULE_DIR, "data","duid_to_npi_fid.csv")
npi_map = pd.read_csv(npi_map_path)

path = os.path.join(MODULE_DIR, "data","npi_data.csv")
df_npi = pd.read_csv(path)

def load_full_reg_stations():
    sql = "SELECT * FROM FULL_REGISTER"
    return pd.read_sql(sql, con=legacy)

def load_alias_map(engine=SQLITE):
    sql = "SELECT SA.STATION_ALIAS, S.STATIONNAME "\
          "FROM STATION_ALIAS SA "\
          "INNER JOIN STATION S "\
              "ON S.ID = SA.STATIONID"
    return pd.read_sql(sql, con=engine)

COLUMN_NAMES = {"STATIONAMES"           : "station_name",
                "STARTTYPE"             : "start_type",
                "REGISTEREDCAPACITY"    : "registered_capacity",
                "MAXCAPACITY"           : "max_capacity",
                "DISTRIBUTIONLOSSFACTOR": "distribution_loss_factor",
                "TRANSMISSIONLOSSFACTOR": "transmission_loss_factor",
                "DISPATCHTYPE"          : "dispatch_type",
                "VOLTLEVEL"             : "volt_level",
                "STATIONID"             : "station_id",
                "REGIONID"              : "region_id",
                "CONNECTIONPOINTID"     : "connection_point_id",
                "DISPLAYNAME"           : "display_name",
                "STATIONNAME"           : "station_name",
                "CO2E_ENERGY_SOURCE"    : 'co2e_energy_source',
                "CO2E_DATA_SOURCE"      : 'co2e_data_source',
                "CO2E_EMISSIONS_FACTOR" : 'co2e_emissions_factor',
                'MAX_RAMP_RATE_DOWN'    : 'max_ramp_rate_down',
                'MAX_RAMP_RATE_UP'      : 'max_ramp_rate_up',
                'MIN_RAMP_RATE_DOWN'    : 'min_ramp_rate_down',
                'MIN_RAMP_RATE_UP'      : 'min_ramp_rate_up',
                'SCHEDULE_TYPE'         : 'schedule_type'}

def load_all_stations(engine=SQLITE):
    sql = "SELECT * FROM STATION"
    df = pd.read_sql(sql, con=engine)
    #need to add exclude list
    return display_names.exclude(df)

def load_station(df_fuel_tech, stationid="BAYSW", engine=SQLITE):
    station_sql = "SELECT S.STATIONID, S.STATIONNAME, S.DISPLAYNAME, STATE.STATE, S.POSTCODE, S.LATITUDE, S.LONGITUDE "\
                  "FROM STATION S "\
                  "INNER JOIN STATE ON STATE.ID = S.STATE "\
                  "WHERE S.STATIONID = '{0}'".format(stationid)
    station_df = pd.read_sql(station_sql, con=engine)
    station_data = station_df.to_dict(orient="rows")[0]

    station_dict = {COLUMN_NAMES[key]: station_data[key] for key in ['STATIONID', 'STATIONNAME', 'DISPLAYNAME']}
    station_dict['location'] = {key.lower(): station_data[key] for key in ['STATE', 'POSTCODE', 'LATITUDE', 'LONGITUDE']}

    ds = load_dudetailsummary(stationid=stationid, engine=engine)
    latest = ds[ds.END_DATE==ds.END_DATE.max()]

    uniq_cols = [ 'DISPATCHTYPE', 'SCHEDULE_TYPE', 'STARTTYPE', 'CONNECTIONPOINTID', 'TRANSMISSIONLOSSFACTOR',
       'DISTRIBUTIONLOSSFACTOR', 'MIN_RAMP_RATE_UP', 'MIN_RAMP_RATE_DOWN',
       'MAX_RAMP_RATE_UP', 'MAX_RAMP_RATE_DOWN']

    station_cols = ['REGIONID', 'PARTICIPANT']

    duid_data = latest[uniq_cols].copy()
    for col in ['DISPATCHTYPE', 'SCHEDULE_TYPE', 'STARTTYPE']:
        duid_data[col] = duid_data[col].apply(str.lower)
    duid_data.rename(columns = COLUMN_NAMES, inplace=True)
    station_dict['duid_data'] = duid_data.to_dict(orient='index')

    du_station_data = latest[station_cols].drop_duplicates()
    try:
        du_station_data.rename(columns = COLUMN_NAMES, inplace=True)
        du_station_dict = du_station_data.to_dict(orient="rows")[0]
        station_dict.update({key.lower(): meta_lower(key, du_station_dict[key]) for key in du_station_dict})
    except:
        pass
        #print (stationid)

    dg= load_genunits(stationid=stationid, engine=engine)
    genset_data = dg[['MAXCAPACITY', 'REGISTEREDCAPACITY', 'VOLTLEVEL', 'CO2E_DATA_SOURCE', 'CO2E_ENERGY_SOURCE', 'CO2E_EMISSIONS_FACTOR']].copy()
    genset_data.rename(columns=COLUMN_NAMES, inplace=True)

    for duid in station_dict['duid_data']:
        duid_ft = df_fuel_tech[df_fuel_tech.duid==duid]
        if len(duid_ft) == 1:
            station_dict['duid_data'][duid].update({"fuel_tech": duid_ft.fuel_tech.values[0]})
            if duid_ft.first_run.notna().values[0]:
                station_dict['duid_data'][duid].update({"first_run": int(duid_ft.first_run.values[0])})
        elif len(duid_ft) == 0:
            if duid in genset_data.index:
                energy_source = genset_data.loc[duid].co2e_energy_source
                ft_map = {"Diesel oil": 'distillate',
                          "Black coal" : 'black_coal',
                          "Brown coal" : 'brown_coal',
                          "Hydro"       : 'hydro',
                          "Landfill biogas methane" : 'gas_recip',
                          "Natural Gas (Pipeline)" : 'gas_ocgt', 
                          "Solar" : 'solar', 
                          "Other Biofuels"  :   'biomass'}
                station_dict['duid_data'][duid].update({"fuel_tech": ft_map[energy_source]})
            else:
                station_map = {"TUMUT1" : 'hydro',
                               "TUMUT2" : 'hydro',
                               "TUMUT3" : 'hydro',
                               "MURRAY1" : 'hydro',
                               "MURRAY2" : 'hydro',
                               "RUBICON" : 'hydro',
                               "SNWYGJP2" : 'pumps',
                               "SNOWY4" : 'hydro',
                               "SNOWY5" : 'hydro',
                               "SNOWY4" : 'hydro',
                               "GUTHEGA" : 'hydro',
                               "BLOWER" : 'hydro',
                               "LYGS": 'gas_ocgt',
                               "BELLYBAY" : 'gas_ocgt'}
                if stationid in station_map:
                    station_dict['duid_data'][duid].update({"fuel_tech": station_map[stationid]})
                elif stationid in ['ERG_AS', 'TOMAGO','PORTLAND', 'PTH']:
                    pass
                else:
                    print (stationid, duid)
        else:
            ft_unique = duid_ft.fuel_tech.unique()
            station_dict['duid_data'][duid].update({"fuel_tech": ft_unique[-1]})

    genset_data = genset_data[['max_capacity', 'registered_capacity', 'volt_level', 'co2e_data_source', 'co2e_emissions_factor']]
    for genset, data in genset_data.iterrows():
        if genset in station_dict['duid_data']:
            station_dict['duid_data'][genset].update(data.to_dict())
        else:
            station_dict['duid_data'][genset] = data.to_dict()

    station_dict['status'] = load_operating_status(stationid=stationid)

    try:
        if "NL1" not in duid_data.index[0]:
            npi_d = duid_data.index[0]
        else:
            npi_d = duid_data.index[1]

        npi_year, n_data = npi_data(duid=npi_d)
        n_data.set_index('substance_name', inplace=True)
        n_data.rename(columns={col: col.replace("_emission_kg","") for col in n_data}, inplace=True)
        station_dict['npi'] = {"data" : {substance: group.dropna().to_dict() for substance, group in n_data.iterrows()},
                               "report_year": int(npi_year),
                               "unit": "kg"}
    except:
        pass

    return station_dict

def meta_lower(k,v):
    if k in ['DISPATCHTYPE', 'SCHEDULE_TYPE', 'STARTTYPE']:
        return v.lower()
    else: 
        return v

def load_dudetailsummary(stationid="BAYSW", engine=SQLITE):
    sql = "SELECT DU.DUID, R.REGIONID, P.NAME as PARTICIPANT, C.CONNECTIONPOINTID, D.DISPATCHTYPE, S.SCHEDULE_TYPE, ST.STARTTYPE, "\
          "DS.TRANSMISSIONLOSSFACTOR, DS.DISTRIBUTIONLOSSFACTOR, DS.MIN_RAMP_RATE_UP, DS.MIN_RAMP_RATE_DOWN, "\
          "DS.MAX_RAMP_RATE_UP, DS.MAX_RAMP_RATE_DOWN, DS.IS_AGGREGATED, DS.START_DATE, DS.END_DATE, DS.LASTCHANGED "\
          "FROM DUDETAILSUMMARY DS "\
              "INNER JOIN DU ON DU.ID = DS.DUID "\
              "INNER JOIN REGION R ON R.ID = DS.REGIONID "\
              "INNER JOIN PARTICIPANT P ON P.ID = DS.PARTICIPANTID "\
              "INNER JOIN CONNECTIONPOINT C ON C.ID = DS.CONNECTIONPOINTID "\
              "INNER JOIN DISPATCHTYPE D ON D.ID = DS.DISPATCHTYPE "\
              "INNER JOIN SCHEDULE_TYPE S ON S.ID = DS.SCHEDULE_TYPE "\
              "INNER JOIN STARTTYPE ST ON ST.ID = DS.STARTTYPE "\
              "INNER JOIN STATION ON STATION.ID = DS.STATIONID "\
          "WHERE STATION.STATIONID = '{0}'"
    return pd.read_sql(sql.format(stationid), con=engine, index_col = "DUID")

def load_operating_status(stationid="HAZEL", engine=SQLITE):
    sql = "SELECT STATUS.STATUS, OS.EFFECTIVEDATE FROM STATIONOPERATINGSTATUS OS "\
          "INNER JOIN STATION ON STATION.ID = OS.STATIONID "\
          "INNER JOIN STATUS ON STATUS.ID = OS.STATUS "\
          "WHERE STATION.STATIONID = '{0}'"
    df = pd.read_sql(sql.format(stationid), con=engine)

    latest = df.EFFECTIVEDATE.max()
    latest_dt = datetime.datetime.strptime(latest[:-9], "%Y/%m/%d")
    status = df[df.EFFECTIVEDATE == latest]
    if status.STATUS.values[0]=="DECOMMISSIONED":
        return {"state":"Decommissioned",
                "date" : str(latest_dt.date())}
    else:
        return {"state": "Commissioned"}

def load_genunits(stationid="BAYSW", engine=SQLITE):
    sql = "SELECT GS.GENSETID, CE.CO2E_ENERGY_SOURCE, CD.CO2E_DATA_SOURCE, "\
          "G.REGISTEREDCAPACITY, G.MAXCAPACITY, G.CO2E_EMISSIONS_FACTOR, G.VOLTLEVEL "\
          "FROM GENUNITS G "\
              "INNER JOIN CO2E_ENERGY_SOURCE CE ON CE.ID = G.CO2E_ENERGY_SOURCE "\
              "INNER JOIN CO2E_DATA_SOURCE CD ON CD.ID = G.CO2E_DATA_SOURCE "\
              "INNER JOIN GENSET GS ON GS.ID = G.GENSETID "\
              "INNER JOIN STATION ON STATION.ID = G.STATIONID "\
          "WHERE STATION.STATIONID = '{0}'"
    return pd.read_sql(sql.format(stationid), con=engine, index_col = "GENSETID")

def npi_data(duid="HWPS1"):
    f_id = npi_map[npi_map.DUID == duid].npi_facility_id.values[0]
    f_data = df_npi[df_npi.facility_id == f_id]
    year = f_data.report_year.max()
    d=f_data[f_data.report_year==year]
    return year, d[['substance_name', 'air_point_emission_kg',
       'air_fugitive_emission_kg', 'air_total_emission_kg',
       'water_emission_kg', 'land_emission_kg']]

def select_meta(engine=legacy):
    sql = "SELECT F.DUID as duid, F.STATION_NAME as station_name, F.REGIONID as regionid, FT.openNEM_keys as fuel_tech, "\
          "F.REG_CAP as reg_cap, NT.FIRST_RUN as first_run FROM nemweb_meta.FULL_REGISTER F "\
          "LEFT JOIN nemweb_meta.FUEL_TECHS FT "\
              "ON FT.FUEL_TECH = F.FUEL_TECH "\
          "LEFT JOIN nemweb_meta.NTNDP_TECHNICAL_DATA NT "\
              "ON NT.DUID = F.ID"
    return pd.read_sql(sql, con=legacy)
    
def file_upload(d, client, keyname="generator_registry.json"):
    json_str = simplejson.dumps(d, ignore_nan=True).encode()
    string_buffer = BytesIO(json_str)
    gz_buffer = stream_to_gzip(string_buffer)
            
    #client = aws_client()
    
    metadata = {'ContentType'        : 'application/json', 
                'ContentEncoding'    : 'gzip',
                'CacheControl'        : 'max-age={0}'.format(10)}
    
    file_key = aws_settings['key_root'] + keyname

    client.upload_fileobj(  gz_buffer,
                            aws_settings['data_bucket'], 
                            file_key,
                            ExtraArgs = metadata)

def upload_all(client):
    s = load_all_stations()
    df = select_meta()
    for i in s.STATIONID:
        d = load_station(df, stationid=i)
        keyname="{0}.json".format(d['stationid'])
        file_upload(d, client, keyname)
        print (i)

def upload_master(client):
    s = load_all_stations()
    df = select_meta()
    d = {}
    for i in s.STATIONID:
        d[i] = load_station(df, stationid=i)
        print (i)
    file_upload(d, client, keyname="generator_registry.json")

def stream_to_gzip(json_buffer):
    json_buffer.seek(0)
    gzip_buffer = BytesIO()
    with gzip.GzipFile(filename="test", fileobj=gzip_buffer, mode="wb") as gz:
        shutil.copyfileobj(json_buffer, gz)
    gzip_buffer.seek(0)
    return gzip_buffer

def aws_client():
    client = boto3.client('s3', 
                          aws_access_key_id = aws_settings['aws_access_key_id'],
                          aws_secret_access_key=aws_settings['aws_secret_access_key'])
