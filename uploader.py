import pandas as pd
import datetime
import os
from duid_meta import CONFIG, MODULE_DIR, mmsds_reader, display_names, npi
from sqlalchemy import create_engine

PATH = os.path.join(CONFIG['local_settings']['test_folder'],"testdb.db")
SQLITE = create_engine("sqlite:///{0}".format(PATH))
PYTHON = create_engine("mysql://{username}:{password}@{hostname}/meta?unix_socket={socket}".format(**CONFIG['python_sql']))

legacy = create_engine("mysql://select:marblemysql@marble.earthsci.unimelb.edu.au/nemweb_meta")

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

def load_all_stations(engine=SQLITE):
    sql = "SELECT * FROM STATION"
    df = pd.read_sql(sql, con=engine)
    #need to add exclude list
    return display_names.exclude(df)

def load_station(stationid="BAYSW", engine=SQLITE):
    station_sql = "SELECT S.STATIONID, S.STATIONNAME, S.DISPLAYNAME, STATE.STATE, S.POSTCODE, S.LATITUDE, S.LONGITUDE "\
                  "FROM STATION S "\
                  "INNER JOIN STATE ON STATE.ID = S.STATE "\
                  "WHERE S.STATIONID = '{0}'".format(stationid)
    station_df = pd.read_sql(station_sql, con=engine)
    station_data = station_df.to_dict(orient="rows")[0]

    station_dict = {key.lower(): station_data[key] for key in ['STATIONID', 'STATIONNAME', 'DISPLAYNAME']}
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
    duid_data.rename(columns = {col:col.lower() for col in uniq_cols}, inplace=True)
    station_dict['duid_data'] = duid_data.to_dict(orient='index')

    du_station_data = latest[station_cols].drop_duplicates()

    du_station_dict = du_station_data.to_dict(orient="rows")[0]
    station_dict.update({key.lower(): meta_lower(key, du_station_dict[key]) for key in du_station_dict})

    station_dict['status'] = load_operating_status(stationid=stationid)

    if "NL1" not in duid_data.index[0]:
        npi_d = duid_data.index[0]
    else:
        npi_d = duid_data.index[1]

    npi_year, n_data = npi_data(duid=npi_d)
    station_dict['npi'] = {"data" : n_data.set_index('substance_name').to_dict(orient='index'),
                           "report_year": npi_year}

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
    status = df[df.EFFECTIVEDATE == latest]
    if status.STATUS.values[0]=="DECOMMISSIONED":
        return "Decommissioned ({0})".format(latest[:-9])
    else:
        return "Commissioned"

def npi_data(duid="HWPS1"):
    f_id = npi_map[npi_map.DUID == duid].npi_facility_id.values[0]
    f_data = df_npi[df_npi.facility_id == f_id]
    year = f_data.report_year.max()
    d=f_data[f_data.report_year==year]
    return year, d[['substance_name', 'air_point_emission_kg',
       'air_fugitive_emission_kg', 'air_total_emission_kg',
       'water_emission_kg', 'land_emission_kg']]
