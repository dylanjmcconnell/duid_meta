from sqlalchemy import create_engine
import os
import configparser

CONFIG = configparser.RawConfigParser()
MODULE_DIR = os.path.dirname(__file__)
CONFIG.read(os.path.join(MODULE_DIR,'config.ini'))

ENGINE = create_engine("mysql://{username}:{password}@{hostname}/nemweb?unix_socket={socket}".format(**CONFIG['basic_sql']))
