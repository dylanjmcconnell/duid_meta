import simplejson
import yaml
import boto3
from io import BytesIO
import gzip
import shutil
import pandas as pd
import datetime
import os
from duid_meta import CONFIG, MODULE_DIR, mmsds_reader, display_names, wa
from sqlalchemy import create_engine

aws_settings = CONFIG['aws_settings']

PATH = os.path.join(CONFIG['local_settings']['test_folder'],"testdb.db")
SQLITE = create_engine("sqlite:///{0}".format(PATH))
PYTHON = create_engine("mysql://{username}:{password}@{hostname}/meta?unix_socket={socket}".format(**CONFIG['python_sql']))

legacy = create_engine("mysql://{username}:{password}@{hostname}/nemweb_meta?unix_socket={socket}".format(**CONFIG['basic_sql']))

latlon_path = os.path.join(MODULE_DIR, "data","latlon.csv")
df_latlon = pd.read_csv(latlon_path)

with open(os.path.join(MODULE_DIR, "data", "fueltech_override.yml"), 'r') as ymlfile:
    FUELTECH_OVERRIDE = yaml.safe_load(ymlfile)

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

def max_by_duid(group):
    return group[group.END_DATE==group.END_DATE.max()]

def load_duid(stationid="BAYSW", engine=SQLITE):
    ds = load_dudetailsummary(stationid=stationid, engine=engine)

    ds_d = ds[ds.STARTTYPE != "NOT DISPATCHED"]
    ds_nd = ds[ds.STARTTYPE == "NOT DISPATCHED"]
    uniq_nd = set(ds_nd.index)
    uniq_d = set(ds_d.index)

    #drop station load
    if (len(uniq_nd) == 1) & (len(uniq_d) > 1):
        #need to check clover
        ds = ds[ds.STARTTYPE != "NOT DISPATCHED"]

    return ds

def latest_record(ds):
    if ds.empty:
        latest = ds[ds.END_DATE==ds.END_DATE.max()]
    else:
        latest = ds.groupby(ds.index).apply(max_by_duid)
        latest.set_index(latest.index.droplevel(1), inplace=True)
    return latest

def load_station(df_fuel_tech, stationid="BAYSW", engine=SQLITE):
    station_sql = "SELECT S.STATIONID, S.DISPLAYNAME, STATE.STATE, S.POSTCODE, S.LATITUDE, S.LONGITUDE "\
                  "FROM STATION S "\
                  "INNER JOIN STATE ON STATE.ID = S.STATE "\
                  "WHERE S.STATIONID = '{0}'".format(stationid)
    station_df = pd.read_sql(station_sql, con=engine)
    station_data = station_df.to_dict(orient="rows")[0]

    station_dict = {COLUMN_NAMES[key]: station_data[key] for key in ['STATIONID', 'DISPLAYNAME']}
    station_dict['location'] = {key.lower(): station_data[key] for key in ['STATE', 'POSTCODE', 'LATITUDE', 'LONGITUDE']}

    ds = load_duid(stationid=stationid, engine=engine)
    latest = latest_record(ds)

    duid_data = latest[['STARTTYPE']].copy()[[]]

    station_dict['duid_data'] = duid_data.to_dict(orient='index')

    station_cols = ['REGIONID']
    du_station_data = latest[station_cols].drop_duplicates()
    try:
        du_station_data.rename(columns = COLUMN_NAMES, inplace=True)
        du_station_dict = du_station_data.to_dict(orient="rows")[0]
        station_dict.update({key.lower(): meta_lower(key, du_station_dict[key]) for key in du_station_dict})
    except:
        pass
        #print (stationid)
    dg= load_genunits(stationid=stationid, engine=engine)
    genset_data = dg[['REGISTEREDCAPACITY','CO2E_ENERGY_SOURCE']].copy()
    genset_data.rename(columns=COLUMN_NAMES, inplace=True)

    for duid in station_dict['duid_data']:
        duid_ft = df_fuel_tech[df_fuel_tech.duid==duid]
        
        ft_count = len(duid_ft)

        if ft_count == 1:
            station_dict['duid_data'][duid].update({"fuel_tech": duid_ft.fuel_tech.values[0],
                                                    "registered_capacity": duid_ft.reg_cap.values[0]})

        elif ft_count == 0:
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

        elif ft_count==2:
            dx = duid_ft[duid_ft['reg_cap'].notna()]
            if len(dx) == 1:
                station_dict['duid_data'][duid].update({"fuel_tech": duid_ft.fuel_tech.values[0],
                                                        "registered_capacity": duid_ft.reg_cap.values[0]})
            else:
                station_dict['duid_data'][duid].update({"fuel_tech": duid_ft.fuel_tech.values[0],
                                                        "registered_capacity": duid_ft.reg_cap.values[0]})

        else:
            dx = duid_ft[duid_ft['reg_cap'].notna()]
            #check if all reg_cap data the same
            if len(dx.reg_cap.unique()) == 1:
                station_dict['duid_data'][duid].update({"fuel_tech": duid_ft.fuel_tech.values[-1],
                                                        "registered_capacity": dx.reg_cap.values[-1]})
            #if not, sum values
            else:
                station_dict['duid_data'][duid].update({"fuel_tech": duid_ft.fuel_tech.values[-1],
                                                        "registered_capacity": dx.reg_cap.sum()})


    for genset, data in genset_data.iterrows():
        if genset in station_dict['duid_data']:
            if genset != "MTGELWF1":
                station_dict['duid_data'][genset].update(data[['registered_capacity']].to_dict())
        #else:
            #station_dict['duid_data'][genset] = data.to_dict()

    station_dict['status'] = load_operating_status(stationid=stationid)

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
    sql = "SELECT GS.GENSETID, G.REGISTEREDCAPACITY, CE.CO2E_ENERGY_SOURCE FROM GENUNITS G "\
          "INNER JOIN STATION ON STATION.ID = G.STATIONID "\
          "INNER JOIN GENSET GS ON GS.ID = G.GENSETID "\
          "INNER JOIN CO2E_ENERGY_SOURCE CE ON CE.ID = G.CO2E_ENERGY_SOURCE "\
          "WHERE STATION.STATIONID = '{0}'"
          #GS.GENSETID, CE.CO2E_ENERGY_SOURCE, CD.CO2E_DATA_SOURCE, "
          #"G.MAXCAPACITY, G.CO2E_EMISSIONS_FACTOR, G.VOLTLEVEL "\
          #"INNER JOIN CO2E_DATA_SOURCE CD ON CD.ID = G.CO2E_DATA_SOURCE "\
    return pd.read_sql(sql.format(stationid), con=engine, index_col = "GENSETID")

def select_meta(engine=legacy):
    #df_fuel_tech?
    sql = "SELECT F.DUID as duid, F.STATION_NAME as station_name, F.REGIONID as regionid, FT.openNEM_keys as fuel_tech, "\
          "F.REG_CAP as reg_cap, NT.FIRST_RUN as first_run FROM nemweb_meta.FULL_REGISTER F "\
          "LEFT JOIN nemweb_meta.FUEL_TECHS FT "\
              "ON FT.FUEL_TECH = F.FUEL_TECH "\
          "LEFT JOIN nemweb_meta.NTNDP_TECHNICAL_DATA NT "\
              "ON NT.DUID = F.ID"
    return pd.read_sql(sql, con=legacy)
    
def file_upload(d, client, keyname="facility_registry.json", keyroot=aws_settings['key_root']):
    json_str = simplejson.dumps(d, ignore_nan=True).encode()
    string_buffer = BytesIO(json_str)
    gz_buffer = stream_to_gzip(string_buffer)
            
    #client = aws_client()
    
    metadata = {'ContentType'        : 'application/json', 
                'ContentEncoding'    : 'gzip',
                'CacheControl'        : 'max-age={0}'.format(10)}
    
    file_key = "{0}{1}".format(keyroot, keyname)

    client.upload_fileobj(  gz_buffer,
                            aws_settings['data_bucket'], 
                            file_key,
                            ExtraArgs = metadata)

def upload_master_registry():
    client = aws_client()
    station_dict = load_station_dict()

    nd=with_capacity(station_dict)

    file_upload(nd, client, keyname="facility_registry.json")

def upload_master_registry_wa():
    client = aws_client()
    station_dict = load_station_dict()

    nd=with_capacity(station_dict)

    wa_d = wa.load_all()

    x = {**nd, **wa_d}

    file_upload(x, client, keyname="test_facility_registry.json")


def aws_client():
    client = boto3.client('s3', 
                          aws_access_key_id = aws_settings['aws_access_key_id'],
                          aws_secret_access_key=aws_settings['aws_secret_access_key'])
    return client

def stream_to_gzip(json_buffer):
    json_buffer.seek(0)
    gzip_buffer = BytesIO()
    with gzip.GzipFile(filename="test", fileobj=gzip_buffer, mode="wb") as gz:
        shutil.copyfileobj(json_buffer, gz)
    gzip_buffer.seek(0)
    return gzip_buffer

def load_station_dict():
    s = load_all_stations()
    df_fuel_tech = select_meta()
    station_dict = {}
    for station in s.STATIONID:
        station_dict[station] = load_station(df_fuel_tech, stationid=station)

    swanbank(station_dict)
    callide(station_dict)
    snowy(station_dict)
    drop_loads(station_dict)
    del_station_units(station_dict)
    station_cap_map(df_fuel_tech, station_dict)

    manual_station(station_dict)
    landfil_gas(station_dict)
    incorrect(station_dict)
    wcmg_update(station_dict)

    d = missing_station_data(df_fuel_tech)
    station_dict.update(d)
    station_dict["COOGAPWF"]['duid_data']['COOPGWF1']['registered_capacity'] =452

    for tech in ['solar', 'wind', 'black_coal', 'brown_coal', 'biomass', 'gas_recip', 'gas_ocgt', 'gas_ccgt', 'gas_steam', 'hydro','distillate', 'pumps','none', 'battery_discharging']:
        missing_loc(station_dict, tech=tech)

    return station_dict

def with_capacity(station_dict):
    nd = {}
    print ("Missing capacity data")
    print ("=====================")
    for station_id, station in station_dict.items():
        if station['duid_data']:
            for u,v in station['duid_data'].items():
                if 'registered_capacity' in v:
                    nd[station_id] = station
                else:
                    station_skip(station)
        else:
            station_skip(station)
    return nd

def station_cap_map(df_fuel_tech, station_dict):
    for station_id, fuel_tech in  {"BERWICK": "gas_lfg", "COPTNHYD":"hydro", "EASTCRK2":"gas_lfg", "GLBWNHYD":"hydro", "LUCASHGT": "gas_lfg"}.items():
        dx=df_fuel_tech[df_fuel_tech.duid == station_id]
        station_dict[station_id]['duid_data'] = {station_id: {"fuel_tech": fuel_tech, "registered_capacity": dx.reg_cap.values[0] }}

def station_skip(station):
    if station['station_id'] not in ["MURRAY1", "MURRAY2",
                          "TUMUT1", "TUMUT2", "TUMUT3",
                          "SNOWY4", "SNOWY5", "SNOWYP", "SNOWY",
                          "JINDABYNE", "LYGS", "GUTHEGA"]:
        print (station['station_id'], station['duid_data'])#, station['display_name'])

def tech_print(s,i, tech_check="solar"): #wind
    try: 
        tech = s['duid_data'][i]['fuel_tech'] 
        if tech == tech_check: 
            print (s['station_id'], s['display_name']) 
    except: 
        pass

def missing_loc(station_dict, tech="solar", _print=True):
    dx = df_latlon.rename(columns={col:col.lower() for col in df_latlon})
    dx.set_index("stationid", inplace=True)
    latlon_dict = dx.to_dict(orient="index")

    if _print:
        print ("Missing {0} lat/lon data".format(tech))
        print ("========================")
    for station_id, station in station_dict.items():
        for duid, duid_data in station['duid_data'].items():
            if fuel_tech_check(duid_data, tech):
                if (station['location']['latitude']) == None:
                    try:
                        station['location'].update(latlon_dict[station_id])
                    except:
                        if _print:
                            print (station_id, station['display_name'])
                        else:
                            pass
    print ("\n")

def fuel_tech_check(duid_data, tech):
    try:
        return duid_data['fuel_tech'] == tech
    except:
        return False

def find_missing_stations(df_fuel_tech):
    #df_fuel_tech = select_meta()
    station_list = list(df_fuel_tech.station_name.unique())
    alias = load_alias_map()
    alias_set = set(alias["STATION_ALIAS"])

    mms_stations = load_all_stations()
    mms_set = set(mms_stations['STATIONNAME'])

    full_set = mms_set.union(alias_set)

    missing_stations = []
    for station in station_list:
        if station not in full_set:
            missing_stations.append(station)
    return missing_stations

def missing_station_data(df_fuel_tech):
    missing_stations = find_missing_stations(df_fuel_tech)
    
    df_missing = df_fuel_tech[df_fuel_tech.station_name.isin(missing_stations)].copy()
    d = {}

    for station_name, data in df_missing.groupby(df_missing.station_name):
        regionid  = data.regionid.values[0]
        station_data = temp_station_id(station_name)
        station_data["location"] = {"latitude": None, "longitude": None, "state":regionid[:-1], "postcode":None}
        station_data["region_id"] = regionid
        station_data['duid_data'] = {}
        station_data['status'] = {'state': 'Commissioned'}

        for duid, ddata in data.groupby(data.duid):
            station_data['duid_data'][duid] =  {"fuel_tech":  ddata["fuel_tech"].values[0],
                                                "registered_capacity": ddata["reg_cap"].values[0]}
        del(station_data['fuel_tech'])
        d[station_data["station_id"]] = station_data

    return d

def temp_station_id(station_name):
    replace = {"Wind Farm" : "WF",
               "Solar Farm": "SF",
               "solar Farm": "SF",
               "Solar Park": "SF",
               "Solar PV Power Station": "SF",
               "Battery Energy Storage System" : "SS",
               "Energy Storage System" : "SS",
               "Solar Project": "SF"}

    techs = {"WF" : "wind", "SF" : "solar", "SS" : "battery_charging"}

    tech = None
    for text, abbr  in replace.items():
        if text in station_name:
            station_name = station_name.replace(text, abbr)
            tech = techs[abbr]

    other_replace = {"Creek" : "CK",
                     "One": "1",
                     "Two":"2"}

    for text, abbr  in other_replace.items():
        station_name = station_name.replace(text, abbr)

    station_name = station_name.split(", Units")[0]

    word_list = station_name.split(" ")
    if len(word_list) == 2:
        station_id =  "".join([word_list[0][:6],word_list[1]]).upper()
    elif len(word_list) >2:
        _min =  min([len(i) for i in word_list[:2]])
        if _min <3:
            _max = 6-_min
            station_id =  "".join([word_list[0][:_max], word_list[1][:_max], word_list[2]]).upper()
        else:
            station_id =  "".join([word_list[0][:3], word_list[1][:3], word_list[2]]).upper()
    else: 
        station_id = station_name
    display_name = " ".join(station_name.split(" ")[:-1])
    return {"station_id": station_id, "display_name" : display_name, "fuel_tech" : tech}
    
def swanbank(station_dict):
    swan_e = station_dict["SWANBANK"].copy()
    swan_e['duid_data'] = {i : swan_e['duid_data'][i] for i in ['SWAN_E']}
    swan_e['display_name'] = "Swanbank E"
    swan_e['station_id'] = "SWAN_E"

    swan_b = station_dict["SWANBANK"].copy()
    swan_b['duid_data'] = {i : swan_b['duid_data'][i] for i in ['SWAN_B_1', 'SWAN_B_2', 'SWAN_B_3', 'SWAN_B_4']}
    swan_b['display_name'] = "Swanbank B"
    swan_b['status'] = {'state': 'Decommissioned', 'date': '2012-05-01'}
    swan_b['station_id'] = "SWAN_B"

    del(station_dict["SWANBANK"])

    station_dict.update({"SWAN_E": swan_e, "SWAN_B" : swan_b})

def del_station_units(d):
    station_unit_dict = {'TREVALLN' : ['TREV_1', 'TREV_2', 'TREV_3', 'TREV_4'],
                         'GORDON': ['GORDON1', 'GORDON2', 'GORDON3'],
                         'PTINA110' : ['POATINA1', 'POATINA2'],
                         'DRYCGT' : ['DRYCNL', 'DRYCNL3'],
                         'TUNGATIN' : ['TUNGA_3', 'TUNGA_4'],
                         'LI_WY_CA' : ['L_W_CNL1', 'WYA252B1', 'WYB252B1', 'CATA_1', 'CATA_2'],
                         'LEM_WIL' : ['LEMONTME'],
                         'BHILLGT' : ['GB02'],
                         'KAREEYA' : ['KARYNL1'],
                         'GEORGTWN' : ['GEORGTN2'],
                         'SNUG' : ['SNUG4'],
                         'OSBORNE' : ["OSB01", "OSB02"],
                         'VPGS':['VPGS'],
                         'LNGS': ['LAVNORTH']}

    for station_id, unit_list in station_unit_dict.items():
        for unit in unit_list:
            if unit in d[station_id]['duid_data']:
                del(d[station_id]['duid_data'][unit])

def callide(station_dict):
    call_a = station_dict["CALLIDE"].copy()
    call_a['duid_data'] = {i : call_a['duid_data'][i] for i in ['CALL_A_2', 'CALL_A_4']}
    call_a['display_name'] = "Callide A"
    call_a['station_id'] = "CALL_A"
    call_a['status'] = {'state': 'Decommissioned', 'date': '2015-03-01'}

    call_b = station_dict["CALLIDE"].copy()
    call_b['duid_data'] = {i : call_b['duid_data'][i] for i in ['CALL_B_1', 'CALL_B_2']}
    call_b['display_name'] = "Callide B"
    call_b['station_id'] = "CALL_B"

    del(station_dict["CALLIDE"])

    station_dict.update({"CALL_A": call_a, "CALL_B" : call_b})

def snowy(station_dict):
    station_dict["SNOWY1"]['duid_data'] = {"TUMUT3" : {'fuel_tech': 'hydro', 'registered_capacity': 1500.0},
                                           "SNOWYP" : {'fuel_tech': 'pumps', 'registered_capacity': 600.0}}
    station_dict["SNOWY1"]['region_id'] = "NSW1"
    station_dict["SNOWY2"]['region_id'] = "NSW1"
    station_dict["SNWYGJP2"]['duid_data'] = {"SNWYGJP2" : {'fuel_tech': 'pumps', 'registered_capacity': 70.0}}
    station_dict["BLOWER"]['duid_data'] = {"BLOWERNG" : {'fuel_tech': 'hydro', 'registered_capacity': 70.0}}

    del(station_dict["SNOWY2"]['duid_data']['SNOWY2'])
    del(station_dict["SNOWY3"]['duid_data']['SNOWY3'])
    del(station_dict["SNOWY6"]['duid_data']['SNOWY6'])
    del(station_dict["MURRAY"]['duid_data']['MURAYNL1'])
    del(station_dict["MURRAY"]['duid_data']['MURAYNL2'])
    del(station_dict["MURRAY"]['duid_data']['MURAYNL3'])
    del(station_dict["MURRAY1"])
    del(station_dict["MURRAY2"])
    del(station_dict["SNOWYP"])
    del(station_dict["SNOWY"])
    del(station_dict["BLOWER"])

    station_dict["CLOVER"]['duid_data'] = {"CLOVER" : {'fuel_tech': 'hydro', 'registered_capacity': 29.0}}
    station_dict["CALLIDEC1"]['duid_data'] = {"CPP_3" : {'fuel_tech': 'black_coal', 'registered_capacity': 420},
                                             "CPP_4" : {'fuel_tech': 'black_coal', 'registered_capacity': 420}}
    del(station_dict["CALLIDEC"])
    station_dict["GANNBESS"]['duid_data']['GANNBL1'] = {'fuel_tech': 'battery_charging', 'registered_capacity': 30.0}
    station_dict["BALBESS"]['duid_data']['BALBL1'] = {'fuel_tech': 'battery_charging', 'registered_capacity': 30.0}
    station_dict["LGAPWF1"]['duid_data']["LGAPWF1"] = {'fuel_tech': 'wind', 'registered_capacity': 212.4}
    station_dict["BELLBAY"]['duid_data']  = {"BELLBAY1": {'fuel_tech': 'gas_steam', 'registered_capacity': 120.0},
                                             "BELLBAY2": {'fuel_tech': 'gas_steam', 'registered_capacity': 120.0}}
                                             
    station_dict["TVCCPS"]['duid_data']['TVCC201'] = {'fuel_tech': 'gas_ccgt', 'registered_capacity': 208.6}

    for station in ['GLBWNHYD', 'COPTNHYD', 'EASTCRK2', "LUCASHGT"]:
        station_dict[station]['location']['state'] = "NSW"
        station_dict[station]['region_id'] = "NSW1"

    station_dict['BERWICK']['location']['state'] = "VIC"
    station_dict['BERWICK']['region_id'] = "VIC1"

    station_dict['LGAPWF1']['location']['state'] = "SA"
    station_dict['LGAPWF1']['region_id'] = "SA1"

    station_dict['EILDONPD']['display_name'] = 'Eildon (Run of River)'
    station_dict['DALNTH']['display_name'] = 'Dalrymple North Battery'
    station_dict['HORNSDPR']['display_name'] = 'Hornsdale Power Reserve'


def drop_loads(station_dict):
    for load in ['ERG_AS', 'TOMAGO','PORTLAND', 'PTH', 'ASNACTEW', 'ASTHYD1', "ENOCSA"]:
        del(station_dict[load])

def manual_station(d={}):
    stations = {"Blayney": {"latitude": -33.621944, "longitude": 149.198333, "capacity":9.9, "fuel_tech": "wind", "state":"NSW"},
                "Crookwell" : {"latitude": -34.516500, "longitude" : 149.542480, "capacity":4.8, "fuel_tech": "wind", "state":"NSW"},
                "Hampton" : {"latitude": -33.649722, "longitude":150.050000, "capacity":1.32, "fuel_tech":"wind", "state":"NSW"}}

    for display_name, station in stations.items():
        station_id = temp_station_id(display_name[:8].upper())["station_id"]
        #(should check for clashes)
        d.update({station_id: {'station_id': station_id,
         'display_name': display_name,
         'location': {'state': station['state'],
          'postcode': None,
          'latitude': station["latitude"],
          'longitude': station["longitude"]},
         'duid_data': {station_id: {'fuel_tech': station["fuel_tech"],
           'registered_capacity': station["capacity"]}},
         'region_id': station['state']+"1",
         'status': {'state': 'Commissioned'}}})
    return d

def landfil_gas(sd):
    for station_id, station in sd.items():
        for duid, duid_data in station['duid_data'].items():
            if duid in FUELTECH_OVERRIDE['gas_lfg']:
                duid_data.update({'fuel_tech': "gas_lfg"})

def incorrect(sd):
    duids = ["BWTR1"]
    for station_id, station in sd.items():
        for duid, duid_data in station['duid_data'].items():
            if duid in duids:
                duid_data.update({'fuel_tech': "biomass"})

def wcmg_update(sd):
    duids = ["GLENNCRK",
             "OAKY2",
             "TAHMOOR1",
             "TERALBA",
             "GERMCRK",
             "GERMCRK",
             "GROSV1",
             "GROSV2",
             "MBAHNTH",
             "APPIN",
             "TOWER"]
    for station_id, station in sd.items():
        for duid, duid_data in station['duid_data'].items():
            if duid in duids:
                duid_data.update({'fuel_tech': "gas_wcmg"})
