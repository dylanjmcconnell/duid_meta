import pytest
from duid_meta import mmsds_reader

@pytest.mark.parametrize("dataset, year, month, url", [
    ["dudetail", 2018, 1, "http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2018/MMSDM_2018_01/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_DUDETAIL_201801010000.zip"],
    ["dudetailsummary", 2018, 11, "http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2018/MMSDM_2018_11/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_DUDETAILSUMMARY_201811010000.zip"],
    ["participant", 2018, 10, "http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2018/MMSDM_2018_10/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_PARTICIPANT_201810010000.zip"],
    ["genunits", 2018, 2, "http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2018/MMSDM_2018_02/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_GENUNITS_201802010000.zip"],
    ["station", 2018,10, "http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2018/MMSDM_2018_10/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_STATION_201810010000.zip"])
def test_url_generator(dataset, year, month, url):
    test_url = mmsds_reader.url_generator(dataset=dataset, y=year, m=month)
    assert test_url == url
