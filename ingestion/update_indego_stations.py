"""
    Updates the Indego station data ingested from trip data with the station
    data in Indego's station files.

    Unlike the other systems, Indego's station data is not complete and the
    majority of station metadata is in the trips file. Due to the design of
    CachedDimension, ensure, and update, this can't be handled in the
    expected ways. so we're doing it manually via SQL. Stations that are
    not in trip data are not added.

    Add_elevation.py should be run again to update Indego stations with
    elevation data if it was run before trip ingestion.
"""

import csv
import os
from settings import settings
from util import util

QUERY_GET_START_STATIONS = """
SELECT ss.start_station_short_name
FROM start_station ss LEFT JOIN system s on ss.system_id=s.system_id
WHERE s.system_name='indego'
"""

QUERY_UPDATE_START_STATIONS = """
UPDATE start_station
SET start_station_name = ?
WHERE start_station_short_name = ?
"""

QUERY_UPDATE_END_STATIONS = """
UPDATE end_station
SET end_station_name = ?
WHERE end_station_short_name = ?
"""


def main():
    cfg = settings.get_config()
    db_conn = util.get_database_connection()
    data_dir = os.path.join(cfg['ingestion_settings']['data_directory'])

    source = 'indego'

    stations_dir = os.path.join(data_dir, source, 'stations')
    if os.path.isdir(stations_dir):

        mappings = cfg['datasources'][source]['station_fields']
        names = []

        for file in os.listdir(stations_dir):
            if file.endswith('.csv'):
                print('Processing ' + source + ' ' + file)

                with open(os.path.join(stations_dir, file), 'r') as fh:
                    data = csv.DictReader(fh)

                    for row in data:
                        fix_mappings(row, cfg['datasources']['indego']['station_fields'])
                        names.append((row['name'], row['short_name']))
                        
                db_conn.executemany(QUERY_UPDATE_START_STATIONS, names)
                db_conn.executemany(QUERY_UPDATE_END_STATIONS, names)
                db_conn.commit()
                db_conn.close()


def fix_mappings(row, mappings):
    """
    Standardizes field names

    Some of the bike share systems don't have consistent headings and file formats
    in all of the data files, so we need to detect and correct as needed. This
    situation also prevents the use of pygrametl's namemapping function.

    :param row: data row
    :param mappings: known field mappings for the system
    :return: updates the row in place
    """

    for k in mappings:
        if k in row.keys():
            row[mappings[k]] = row.pop(k)
    


if __name__ == "__main__": main()