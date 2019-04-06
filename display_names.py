def display_names(x):
    display_str = x.title()
    for name in ["Power Station",
                "Wind Farm",
                "Solar Farm",
                "Power Plant",
                "Solar Plant",
                "Complex",
                "Facility",
                "Gas Turbine",
                "Station"]:
        display_str = display_str.replace(name,"")

    for name, replace in {"Landfill Gas": "(LFG)",
                          "Nsw": "NSW",
                          "Agl": "AGL"}.items():
        display_str=display_str.replace(name, replace)

    #special_case
    for case, replace in {"Amcor Glass, Gawler Plant": "Amcor Glass, Gawler",
                          "Hepburn Community": "Hepburn wind",
                          "Basslink Hvdc Link": "Basslink",
                          "Ballarat Base Hospital Plant" : "Ballarat Hospital",
                          "Bankstown Sports Club Plant Units" : "Bankstown Sports Club",
                          "Woodlawn Bioreactor Energy Generation": "Woodlawn Bioreactor",
                          "Wollert Renewable Energy " : "Wollert RE Facility",
                          "Woolnorth Studland Bay / Bluff Point" : "Woolnorth",
                          'Shoalhaven  (Bendeela And Kangaroo Valley  And Pumps)': "Shoalhaven",
                          "Wingfield 1 Landfill": "Wingfield (LFG)",
                          "Wingfield 2 Landfill": "Wingfield (LFG)",
                          "Woy Woy Landfill ": "Woy Woy (LFG)",
                          "Wyndham Waste Disposal " : "Wyndham (LFG)",
                          "Brooklyn Lfg U1-3": "Brooklyn (LFG)",
                          "Browns Plains (LFG) Ps": "Brown Plains (LFG)",
                          "Bogong / Mackay" : "Bogong/Mackay",
                          "Catagunya / Liapootah / Wayatinah": "Catagunya/Liapootah/Wayatinah",
                          "Callide C Nett Off" : "Callide C",
                          "Eastern Creek 2 Gas Utilisation" : "Eastern Creek (LFG)",
                          "Eastern Creek Lfg Ps Units 1-4" : "Eastern Creek (LFG)",
                          "Hallam Road Renewable Energy" : "Hallam Rd RE Facility",
                          "Grosvenor 1 Waste Coal Mine Gas" : "Grosvenor",
                          "Isis Central Sugar Mill Co-Generation Plant" : "Isis Central Sugar Mill"




                          }.items():
        if case  in  display_str: 
            display_str = replace
    return display_str.strip()
