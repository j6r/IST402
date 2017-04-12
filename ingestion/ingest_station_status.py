#! /usr/bin/env python

"""
Ingests the Bay Area Bike Share system status data
"""

import os

from dateutil import parser
from pygrametl.datasources import CSVSource

from model.DW import DW
from settings import settings

cfg = settings.get_config()
dw = DW()
DATA_DIR = os.path.join(cfg['ingestion_settings']['data_directory'], 'babs', 'status')


def main():
    system_id = dw.system_dimension.lookup({'system_name': 'babs'})
    for file in os.listdir(DATA_DIR):
        if file.endswith('.csv'):
            print('Processing ' + file)
            with open(os.path.join(DATA_DIR, file), 'r') as fh:
                data = CSVSource(fh)

                for row in data:
                    row['system_id'] = system_id
                    row['short_name'] = row.pop('station_id')
                    row['station_id'] = dw.station_dimension.lookup(row)
                    if row['station_id'] is not -1:
                        insert_datetime_dimensions(row)
                        dw.station_status_fact_table.insert(row)
                    else:
                        print('\nMissing station')
                        print(row)
                        print()


def insert_datetime_dimensions(row):
    """
    Inserts/ensures a trip's date and time information in the date and time dimensions

    :param row: the row corresponding to a trip
    :return: none, row is updated
    """

    d = parser.parse(timestr=row['time'])
    row['start_date_string'] = d.strftime('%Y-%m-%d')
    row['start_time_string'] = d.strftime('%H:%M')

    # update dimensions
    row['start_date_id'] = dw.start_date_dimension.ensure(row)
    row['start_time_id'] = dw.start_time_dimension.ensure(row)

if __name__ == '__main__': main()
