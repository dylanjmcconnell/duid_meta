from duid_meta import ENGINE
import pandas as pd

def load_simple_table():
    sql = "SELECT * FROM nemweb_meta.STATION_NAMES"
    return pd.read_sql(sql, con=ENGINE)

def load_full_table():
    sql = "SELECT * FROM nemweb_meta.FULL_REGISTER"
    return pd.read_sql(sql, con=ENGINE)
