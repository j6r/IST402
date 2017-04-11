"""
    Ingest station data.
"""

import os
from pygrametl.datasources import CSVSource
from model.DW import DW
from settings import settings


def main():
    cfg = settings.get_config()
    data_dir = os.path.join(cfg['ingestion_settings']['data_directory'])
    dw = DW()

    for source in cfg['datasources'].keys():
        stations_dir = os.path.join(data_dir, source, 'stations')
        if os.path.isdir(stations_dir):

            mappings = cfg['datasources'][source]['station_fields']

            for file in os.listdir(stations_dir):
                if file.endswith('.csv'):
                    print('Processing ' + source + ' ' + file)

                    with open(os.path.join(stations_dir, file), 'r') as fh:

                        station_data = CSVSource(fh)

                        for row in station_data:
                            fix_mappings(row, mappings)
                            if 'capacity' not in row.keys():
                                row['capacity'] = -1
                            row['system_name'] = source
                            row['system_id'] = dw.system_dimension.ensure(row)

                            dw.start_station_dimension.ensure(row, namemapping={
                                'start_station_short_name': 'short_name',
                                'start_station_name': 'name',
                                'start_station_longitude': 'longitude',
                                'start_station_latitude': 'latitude',
                                'start_station_capacity': 'capacity'
                            })

                            dw.end_station_dimension.ensure(row, namemapping={
                                'end_station_short_name': 'short_name',
                                'end_station_name': 'name',
                                'end_station_longitude': 'longitude',
                                'end_station_latitude': 'latitude',
                                'end_station_capacity': 'capacity'
                            })

                        dw.get_db_connection().commit()


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