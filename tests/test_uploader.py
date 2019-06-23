import os
import pytest
import pickle
import yaml
from duid_meta import uploader, MODULE_DIR

TEST_DATA_PATH = os.path.join(MODULE_DIR, "data","test_data")
DATA_PATH = os.path.join(MODULE_DIR, "data")

SD = uploader.load_station_dict()

def test_station_dict():
    PATH = os.path.join(TEST_DATA_PATH, "station_dict.pickle")
    with open(PATH, "rb") as f:
        sd = pickle.load(f)
    assert SD == sd

def test_lfg():
    with open(os.path.join(MODULE_DIR, "data", "fueltech_override.yml"), 'r') as ymlfile:
        FUELTECH_OVERRIDE = yaml.safe_load(ymlfile)
    for station_id, station in SD.items():
        for duid, duid_data in station['duid_data'].items():
            if duid in FUELTECH_OVERRIDE['gas_lfg']:
                assert duid_data['fuel_tech'] == "gas_lfg"
