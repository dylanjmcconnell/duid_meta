import os
from sqlalchemy import Column, Integer, String, MetaData, create_engine, Table, ForeignKey, Numeric, DateTime

SQLITE = create_engine("sqlite:///{0}".format(PATH))

def key_table(tablename, fieldname, metadata, str_length=10):
    return Table(tablename, metadata,
           Column('ID', Integer, primary_key=True),
           Column(fieldname, String(str_length), nullable=False, unique=True))

def id_table(tablename, metadata):
    return key_table(tablename, tablename+"ID", metadata)

def create_test_table(engine=MYSWL):
    if os.path.exists(PATH):
        os.remove(PATH)

    metadata = MetaData()

    for tablename in ['CONNECTIONPOINT','PARTICIPANTCLASS', 'REGION']:
        id_table(tablename, metadata)

    for tablename, str_length in [['STARTTYPE', 20],
                                  ['DUID', 10],
                                  ['DISPATCHTYPE', 10],
                                  ['SCHEDULE_TYPE', 20],
                                  ['CO2E_ENERGY_SOURCE', 30],
                                  ['CO2E_DATA_SOURCE', 20]:
        key_table(tablename, tablename, metadata, str_length=str_length)

    state = key_table('STATE', 'STATE', metadata)
    state.append_column(Column('REGIONID', Integer, ForeignKey("REGION.ID")))

    station = id_table('STATION', metadata)
    station.append_column(Column('REGIONID', Integer, ForeignKey("REGION.ID")))
    station.append_column(Column('CONNECTIONPOINTID', Integer, ForeignKey("CONNECTIONPOINT.ID")))
    station.append_column(Column('STATIONNAME', String(80), nullable=False, unique=True))

    participant = id_table('PARTICIPANT', metadata)
    participant.append_column(Column('NAME', String(80), nullable=False, unique=True))
    participant.append_column(Column('PARTICPANTCLASSID', Integer, ForeignKey("PARTICIPANTCLASS.ID")))

    metadata.create_all(engine)
