import os
import pytest
import pickle
from duid_meta import uploader, MODULE_DIR

PATH = os.path.join(MODULE_DIR, "data","test_data", "station_dict.pickle")

def test_station_dict():
    with open(PATH, "rb") as f:
        sd = pickle.load(f)
    assert uploader.load_station_dict() == sd
