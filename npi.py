import pandas as pd
import requests

def load_facility(facility_id="Q019SIE001", filename="test_npi.csv"):
    #doesn't work
    url = "http://www.npi.gov.au/npidata/action/load/emission-by-individual-facility-result/criteria/state/QLD/year/2017/jurisdiction-facility/Q019SIE001?exportCsv=true"
    r = requests.get(url)
    return r
    with open(filename, "w") as f:
        f.write(r.contents)
        
def gov_data():
    url = "https://data.gov.au/dataset/ds-dga-043f58e0-a188-4458-b61c-04e5b540aea4/distribution/dist-dga-258f16fc-5e91-40a9-8ffb-00d62027a036/details?q="
