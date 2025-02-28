import os
import pandas as pd
from duid_meta import MODULE_DIR

def load_and_parse():
    path = os.path.join(MODULE_DIR, "data","display_names.csv")
    df = pd.read_csv(path)
    df.DISPLAYNAME = df.STATIONNAME.apply(display_names)
    dx = exclude(df)
    dx.to_csv(path, index=None)

def display_names(x):
    display_str = x.title()
    for name in ["Power Station",
                "Wind Farm",
                "Windfarm",
                "Solar Farm",
                "Solar Park",
                "Solar Pv",
                "Solar Project",
                "Power Plant",
                "Solar Plant",
                "Complex",
                "Generation Facility",
                "Gas Turbines",
                "Gas Turbine",
                "Combined Cycle",
                "Facility",
                "Plant",
                "Stage ",
                "Station",
                "Diesel",
                "Generator",
                "Mountain Streams",
                "Hydro",
                "Pumps",
                "Power",
                "Ps",
                '"',
                "'",
                "Lfg",
                "LFG",
                "Landfill Gas",
                "Waste Coal Mine Gas"]:
        display_str = display_str.replace(name,"")

    for name, replace in {"Nsw": "NSW",
                          "Agl": "AGL",
                          "Vic": "VIC",
                          "Sa ": "SA ",
                          "Hrl": "HRL",
                          "Krc": "KRC" ,
                          "Scsf": "SCSF",
                          "Renewable Energy": "RE Facility",
                          "Battery Energy Storage System" : "Battery",
                          "Energy Storage System": "Battery",
                          "BESS": "Battery",
                          "Mini" : "(Mini Hydro)",
                          "Small": "(Mini Hydro)",
                          "Cogeneration": "Co-gen"}.items():
        display_str=display_str.replace(name, replace)

    #special_case
    for case, replace in {"Swanbank B  & Swanbank E" : "Swanbank",
                          "Amcor Glass, Gawler Plant": "Amcor Glass, Gawler",
                          "Hepburn Community": "Hepburn Wind",
                          "Basslink Hvdc Link": "Basslink",
                          "Ballarat Base Hospital Plant" : "Ballarat Hospital",
                          "Bankstown Sports Club Plant Units" : "Bankstown Sports Club",
                          "Woodlawn Bioreactor Energy Generation": "Woodlawn Bioreactor",
                          "Woolnorth Studland Bay / Bluff Point" : "Woolnorth",
                          'Shoalhaven  (Bendeela And Kangaroo Valley  And )': "Shoalhaven",
                          "Wingfield 1 Landfill": "Wingfield 1",
                          "Wingfield 2 Landfill": "Wingfield 2",
                          "Woy Woy Landfill ": "Woy Woy",
                          "Wyndham Waste Disposal " : "Wyndham",
                          "Brooklyn  U1-3": "Brooklyn",
                          "Browns Plains () Ps": "Brown Plains",
                          "Bogong / Mackay" : "Bogong/Mackay",
                          "Catagunya / Liapootah / Wayatinah": "Catagunya/Liapootah/Wayatinah",
                          "Lemonthyme / Wilmot" : "Lemonthyme/Wilmot",
                          "Callide C Nett Off" : "Callide C",
                          "Eastern Creek 2 Gas Utilisation" : "Eastern Creek 2",
                          "Eastern Creek   Units 1-4" : "Eastern Creek",
                          "Grosvenor 1 Waste Coal Mine Gas" : "Grosvenor",
                          "Isis Central Sugar Mill Co-Generation" : "Isis Central Sugar Mill",
                          "Smithfield Energy": "Smithfield",
                          "Jindabyne Pump At Guthega": "Jindabyne Pump",
                          "Ergon Frequency Control A.S.": "Ergon FCAS",
                          "Dlink Interconnector": "Directlink",
                          "Uws Co-Gen Unit":   "UWS Co-gen",
                          "Rocky Point Cogeneration Plant" : "Rocky Point Co-gen",
                          "Murlink Intconnector": "Murraylink",
                          "Snowtown  Units 1 And 47": "Snowtown",
                          "Ballarat Base Hospital": "Ballarat Hospital",
                          "Amcor Glass, Gawler": "Amcor Glass",
                          "Southbank Institute Of Technology Unit 1": "Southbank Institute Of Technology",
                          "Lucas Heights Ii": "Lucas Heights 2",
                          "Western Suburbs League Club (Campbelltown)": "Western Suburbs League Club",
                          "Bankstown Sports Club  Units": "Bankstown Sports Club",
                          "St George Leagues" : "St George Leagues Club",
                          "Nine Network Willoughby": "Willoughby",
                          "Valley Power Peaking" : "Valley Power",
                          "Pedler Creek   Units 1-3": "Pedler Creek",
                          "Kincumber Landfill Site": "Kincumber",
                          "SA Water Bolivar Waste Water Treatment (Wwt)": "Bolivar Waste Water Treatment",
                          "Melbourne Regional Landfill" : "Melbourne Regional",
                          "Snowtown  2 North": "Snowtown North",
                          "Hume (NSW) Hydro" : "Hume Hydro (NSW)",
                          "Hume (VIC ) Hydro" : "Hume Hydro (VIC)",
                          "South East Water - Halllam" : "Hallam",
                          "Ross River , Units 1-64" : "Ross River",
                          "Tailem Bend  1, Units 1-54" : "Tailem Bend",
                          "Wemen , Units 1-39": "Wemen",
                          "Murray1" : "Murray 1",
                          "Murray2" : "Murray 2",
                          "Tumut1" : "Tumut 1",
                          "Tumut2" : "Tumut 2",
                          "Tumut3" : "Tumut 3",
                          "VICtoria Mill" : "Victoria Mill",
                          "AGL Kemps Creek" : "Kemps Creek",
                          "Stapylton RE Facility" : "Stapylton",
                          "Rochedale RE Facility" : "Rochedale",
                          "Hallam Road RE Facility" : "Hallam Road", 
                          "Wollert RE Facility" :  "Wollert",
                          "Whitwood Road RE Facility" : "Whitwood Road",
                          "Dalrymple North BESS" : "Dalrymple North",
                          "Moranbah Generation Project" : "Moranbah",
                          "Mount Emerald": "Mt Emerald",
                          "Pt Stanvac" : "Port Stanvac"
                          }.items():
        if case  in  display_str: 
            display_str = replace
    return display_str.strip()

def exclude(df):
    exclude_list = ["Reserve Trader NSW1",
                    "Reserve Trader Qld1",
                    "Reserve Trader Sa1",
                    "Reserve Trader VIC1",
                    "Bhas Zinc  Sa",
                    "VICtorian Smelter Ancillary Services",
                    "NSW1 Dummy",
                    "Qld1 Dummy",
                    "Sa1 Dummy",
                    "VIC1 Dummy",
                    "Tas1 Dummy",
                    "Reserve Trader Tas1",
                    "Basslink",
                    "Murraylink",
                    "Directlink",
                    "Id - Dummy (Nscas)",
                    "Traralgon Network Support Station"
                    "Ergon FCAS",
                    "Enoc Msap NSW",
                    "Enoc Masp Qld",
                    "Enoc Msap VIC"]

    other = [       "Northern Gas Turbine"]

    return df[~df.DISPLAYNAME.isin(exclude_list)]
