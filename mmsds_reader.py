import requests
from nemweb import nemfile_reader
from io import BytesIO

base_url = "http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{0}/MMSDM_{0}_{1:02d}/MMSDM_Historical_Data_SQLLoader/DATA/"

datasets = {'dudetail'       :    {'link' :   'PUBLIC_DVD_DUDETAIL_{0}{1:02d}010000.zip',
                                   'table':   'PARTICIPANT_REGISTRATION_DUDETAIL'},
            'dudetailsummary':    {'link' :   'PUBLIC_DVD_DUDETAILSUMMARY_{0}{1:02d}010000.zip',
                                   'table':   'PARTICIPANT_REGISTRATION_DUDETAILSUMMARY'},
            'participant'    :    {'link' :   'PUBLIC_DVD_PARTICIPANT_{0}{1:02d}010000.zip',
                                   'table':   'PARTICIPANT_REGISTRATION_PARTICIPANT'},
            'genunits'       :    {'link' :   'PUBLIC_DVD_GENUNITS_{0}{1:02d}010000.zip',
                                   'table':   'PARTICIPANT_REGISTRATION_GENUNITS'},
            'station'        :    {'link' :   'PUBLIC_DVD_STATION_{0}{1:02d}010000.zip',
                                   'table':   'PARTICIPANT_REGISTRATION_STATION'},
            'operatingstatus':    {'link' :   'PUBLIC_DVD_STATIONOPERATINGSTATUS_{0}{1:02d}010000.zip',
                                   'table':   'PARTICIPANT_REGISTRATION_STATIONOPERATINGSTATUS'},
            'dispatchableunit':   {'link' :   'PUBLIC_DVD_DISPATCHABLEUNIT_{0}{1:02d}010000.zip',
                                   'table':   'PARTICIPANT_REGISTRATION_DISPATCHABLEUNIT'},
            'stationowner'   :    {'link' :   'PUBLIC_DVD_STATIONOWNER_{0}{1:02d}010000.zip',
                                   'table':   'PARTICIPANT_REGISTRATION_STATIONOWNER'}}

def url_generator(dataset='dudetail', y=2019, m=2):
    link = base_url+datasets[dataset]['link']
    return link.format(y,m)

def download(dataset='dudetail', y=2018,m=1):
    """Dowloads nemweb zipfile from link into memory as a byteIO object.
    nemfile object is returned from the byteIO object """
    link = url_generator(dataset=dataset, y=2018, m=1)
    response = requests.get(link)
    zip_bytes = BytesIO(response.content)
    nemfile = nemfile_reader.nemzip_reader(zip_bytes)
    return nemfile[datasets[dataset]['table']]


DUDETAILSUMMARY = ['I', 'PARTICIPANT_REGISTRATION', 'DUDETAILSUMMARY', '4', 'DUID',
                   'START_DATE', 'END_DATE', 'DISPATCHTYPE', 'CONNECTIONPOINTID',
                   'REGIONID', 'STATIONID', 'PARTICIPANTID', 'LASTCHANGED',
                   'TRANSMISSIONLOSSFACTOR', 'STARTTYPE', 'DISTRIBUTIONLOSSFACTOR',
                   'MINIMUM_ENERGY_PRICE', 'MAXIMUM_ENERGY_PRICE', 'SCHEDULE_TYPE',
                   'MIN_RAMP_RATE_UP', 'MIN_RAMP_RATE_DOWN', 'MAX_RAMP_RATE_UP',
                   'MAX_RAMP_RATE_DOWN', 'IS_AGGREGATED']

DUDETAIL = ['I', 'PARTICIPANT_REGISTRATION', 'DUDETAIL', '3', 'EFFECTIVEDATE',
                   'DUID', 'VERSIONNO', 'CONNECTIONPOINTID', 'VOLTLEVEL',
                   'REGISTEREDCAPACITY', 'AGCCAPABILITY', 'DISPATCHTYPE', 'MAXCAPACITY',
                   'STARTTYPE', 'NORMALLYONFLAG', 'PHYSICALDETAILSFLAG',
                   'SPINNINGRESERVEFLAG', 'AUTHORISEDBY', 'AUTHORISEDDATE', 'LASTCHANGED',
                   'INTERMITTENTFLAG', 'SEMISCHEDULE_FLAG', 'MAXRATEOFCHANGEUP',
                   'MAXRATEOFCHANGEDOWN']

STATION = ['I', 'PARTICIPANT_REGISTRATION', 'STATION', '1', 'STATIONID',
           'STATE', 'POSTCODE', 'LASTCHANGED', 'CONNECTIONPOINTID',
           'STATIONNAME', 'ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'ADDRESS4', 'CITY']

GENUNITS = ['I', 'PARTICIPANT_REGISTRATION', 'GENUNITS', '2', 'GENSETID',
           'STATIONID', 'SETLOSSFACTOR', 'CDINDICATOR', 'AGCFLAG', 'SPINNINGFLAG',
           'VOLTLEVEL', 'REGISTEREDCAPACITY', 'DISPATCHTYPE', 'STARTTYPE',
           'MKTGENERATORIND', 'NORMALSTATUS', 'MAXCAPACITY', 'GENSETTYPE',
           'GENSETNAME', 'LASTCHANGED', 'CO2E_EMISSIONS_FACTOR',
           'CO2E_ENERGY_SOURCE', 'CO2E_DATA_SOURCE']

PARTICIPANTS = ['I', 'PARTICIPANT_REGISTRATION', 'PARTICIPANT', '1', 'PARTICIPANTID',
       'PARTICIPANTCLASSID', 'NAME', 'DESCRIPTION', 'ACN', 'PRIMARYBUSINESS',
       'LASTCHANGED']

STATIONOWNER = ['I', 'PARTICIPANT_REGISTRATION', 'STATIONOWNER', '1', 'EFFECTIVEDATE',
       'PARTICIPANTID', 'STATIONID', 'VERSIONNO', 'LASTCHANGED']

DISPATCHABLEUNITS = ['I', 'PARTICIPANT_REGISTRATION', 'DISPATCHABLEUNIT', '1', 'DUID',
       'DUNAME', 'UNITTYPE', 'LASTCHANGED']

OPERATINGSTATUS = ['I', 'PARTICIPANT_REGISTRATION', 'STATIONOPERATINGSTATUS', '1',
       'EFFECTIVEDATE', 'STATIONID', 'VERSIONNO', 'STATUS', 'AUTHORISEDBY',
       'AUTHORISEDDATE', 'LASTCHANGED']
