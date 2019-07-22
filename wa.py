import os
import pandas as pd
from duid_meta import CONFIG, MODULE_DIR, display_names

def load_participants():
    #http://wa.aemo.com.au/aemo/data/wa/infographic/participant.csv
    path = os.path.join(MODULE_DIR, 'data', "wa_participant.csv")
    return pd.read_csv(path)

def load_facilities():
    #http://wa.aemo.com.au/aemo/data/wa/infographic/facility-meta.csv
    path = os.path.join(MODULE_DIR, 'data', "facility-meta.csv")
    return pd.read_csv(path)

def load_stations():
    #Robins work
    path = os.path.join(MODULE_DIR, 'data', "facilities_wa_extra.csv")
    df = pd.read_csv(path)
    exclude = ['dsm', 'industrial_load']
    return df[~df['Fuel Tech'].isin(exclude)].reset_index()

def display(df):
    df['Display Name'] = df['Display Name'].apply(display_names.display_names)

def remap_fueltech(df):
    maps = {'gas_lfg': 'bioenergy_biogas',
            'gas_biogas': 'bioenergy_biogas',
            'biomass' : 'bioenergy_biomass'}
    for a,b in maps.items():
        df.loc[df['Fuel Tech']==a,"Fuel Tech"]=b

def station_metadata(station_df):
    station_meta = station_df[['Station Name', 'Display Name', 'State']].drop_duplicates()
    if len(station_meta)>1:
        print (station_meta)
        if station_meta['Display Name'].values[0]=="Kwinana":
            pass
        elif station_meta['Display Name'].values[0]=="Wagerup":
            pass
        else:
            raise Exception

    station_meta.rename(columns = {'Station Name':"station_id", 
                                   'Display Name': "display_name",
                                   'State': 'status'}, inplace=True)
    return station_meta.to_dict(orient="records")[0]

def duid_data(station_df):
    dx = station_df[['Facility Code', 'Maximum Capacity (MW)', 'Fuel Tech']].copy()
    dx.rename(columns={'Facility Code': 'DUID', 
                       'Maximum Capacity (MW)': 'registered_capacity',
                       'Fuel Tech': 'fuel_tech'},inplace=True)
    return dx.set_index('DUID').T.to_dict()

def location_data(station_df):
    loc_df = station_df[['Postcode','Postcode.1','Latitude','Longitude']].drop_duplicates()
    if len(loc_df)>1:
        print (station_df)
        if station_df['Display Name'].values[0] ==  "Wagerup":
            loc_df = loc_df.iloc[0]
        else:
            raise Exception

    loc_df.rename(columns = {'Postcode':"state", 
                             'Postcode.1': "postcode",
                             'Latitude': 'latitude',
                             'Longitude': 'longitude'}, inplace=True)
    return loc_df.to_dict(orient="records")[0]

def parse_station(station_df):
    station_data = station_metadata(station_df)
    station_data['status'] = {'state':station_data['status']}
    station_data['region_id'] = 'WA1'
    station_data['location'] = location_data(station_df)
    station_data['duid_data'] = duid_data(station_df)
    return station_data

def load_all():
    df = load_stations()
    display(df)
    remap_fueltech(df)
    sd = {}
    for i,station_df in df.groupby(df['Display Name']):
        station_data = parse_station(station_df)
        sd[station_data['station_id']]=station_data

    return sd

