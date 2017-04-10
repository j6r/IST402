"""
    Helper functions
"""

from dateutil import parser
import sqlite3
from settings import settings



def get_database_connection():

    cfg = settings.get_config()
    return sqlite3.connect(cfg['ingestion_settings']['staging_db_location'])



