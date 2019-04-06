import os
from sqlalchemy import Column, Integer, String, MetaData, create_engine, Table, ForeignKey, Numeric, DateTime, Float
from duid_meta import CONFIG

PATH = os.path.join(CONFIG['local_settings']['test_folder'],"testdb.db")
SQLITE = create_engine("sqlite:///{0}".format(PATH))
ROOT = create_engine("mysql://{username}:{password}@{hostname}/meta?unix_socket={socket}".format(**CONFIG['root_sql']))

def drop_tables(schema="meta"):
    with ROOT.connect() as conn:
        conn.execute("USE {0}".format(schema))
        conn.execute("SET FOREIGN_KEY_CHECKS=0")

        query = conn.execute("SHOW TABLES")
        tables = query.fetchall()

        for table, in tables:
            conn.execute("DROP TABLE {0}".format(table))

        conn.execute("SET FOREIGN_KEY_CHECKS=1")

def key_table(tablename, fieldname, metadata, str_length=10, allow_null=False):
    return Table(tablename, metadata,
           Column('ID', Integer, primary_key=True),
           Column(fieldname, String(str_length), nullable=allow_null, unique=True))

def data_table(tablename, foreign_keys, data_cols, metadata):
    table = Table(tablename, metadata,
           Column('ID', Integer, primary_key=True))

    for column, id_table in foreign_keys.items():
        table.append_column(Column(column, Integer, ForeignKey("{0}.ID".format(id_table))))

    for column, dtype in data_cols.items():
        table.append_column(Column(column, dtype))

def id_table(tablename, metadata):
    return key_table(tablename, tablename+"ID", metadata)

def create_test_table(engine=SQLITE):
    if os.path.exists(PATH):
        os.remove(PATH)

    metadata = MetaData()

    for tablename in ['CONNECTIONPOINT', 'REGION', 'DU']:
            id_table(tablename, metadata)

    key_table('PARTICIPANTCLASS', 'PARTICIPANTCLASSID', metadata, str_length=20)

    #('UNITTYPE' same as DISPATCH_TYPE)
    for tablename, str_length in [['DISPATCHTYPE', 10],
                                  ['STATUS', 20],
                                  ['SCHEDULE_TYPE', 20]]:
        key_table(tablename, tablename, metadata, str_length=str_length)
    
    key_table('STARTTYPE', 'STARTTYPE', metadata, str_length=20, allow_null=True)
    key_table('CO2E_ENERGY_SOURCE', 'CO2E_ENERGY_SOURCE', metadata, str_length=50, allow_null=True)
    key_table('CO2E_DATA_SOURCE', 'CO2E_DATA_SOURCE', metadata, str_length=20, allow_null=True)

    state = key_table('STATE', 'STATE', metadata)
    state.append_column(Column('REGIONID', Integer, ForeignKey("REGION.ID")))
    state.append_column(Column('STATENAME', String(30)))

    substance = key_table('SUBSTANCE', 'SUBSTANCE_NAME', metadata , str_length = 50)

    station = id_table('STATION', metadata)
    station.append_column(Column('STATIONNAME', String(80), nullable=False))
    station.append_column(Column('STATE', Integer, ForeignKey("STATE.ID")))
    station.append_column(Column('POSTCODE', Integer))
    station.append_column(Column('LATITUDE', Float))
    station.append_column(Column('LONGITUDE', Float))

    station_alias = key_table('STATION_ALIAS', 'STATION_ALIAS', metadata, str_length =80)
    station_alias.append_column(Column('STATIONID', Integer, ForeignKey("STATION.ID")))

    participant = id_table('PARTICIPANT', metadata)
    participant.append_column(Column('NAME', String(80), nullable=False, unique=False))
    participant.append_column(Column('PARTICIPANTCLASSID', Integer, ForeignKey("PARTICIPANTCLASS.ID")))

    foreign_keys = {'DUID':'DU', 'REGIONID':'REGION', 'STATIONID':'STATION', 'PARTICIPANTID':'PARTICIPANT', 'CONNECTIONPOINTID':'CONNECTIONPOINT',
       'DISPATCHTYPE':'DISPATCHTYPE', 'SCHEDULE_TYPE':'SCHEDULE_TYPE', 'STARTTYPE': 'STARTTYPE'}
    data_cols = {'TRANSMISSIONLOSSFACTOR': Float, 'DISTRIBUTIONLOSSFACTOR':Float, 'MIN_RAMP_RATE_UP': Float, 'MIN_RAMP_RATE_DOWN': Float,
       'MAX_RAMP_RATE_UP':Float, 'MAX_RAMP_RATE_DOWN':Float, 'IS_AGGREGATED':Float, 'START_DATE': DateTime, 'END_DATE':DateTime, 'LASTCHANGED':DateTime}
    dudetailsummary = data_table("DUDETAILSUMMARY", foreign_keys=foreign_keys, data_cols=data_cols, metadata=metadata)

    metadata.create_all(engine)
