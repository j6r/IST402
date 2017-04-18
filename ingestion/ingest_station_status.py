#! /usr/bin/env python

"""
Ingests the Bay Area Bike Share system status data
"""

import os
import time

from dateutil import parser
from pygrametl.datasources import CSVSource

from model.DW import DW
from settings import settings

cfg = settings.get_config()
dw = DW()
DATA_DIR = os.path.join(cfg['ingestion_settings']['data_directory'], 'babs', 'status')


def main():
    overall_start_time = time.time()
    overall_row_count = 0

    system_id = dw.system_dimension.lookup({'system_name': 'babs'})
    for file in os.listdir(DATA_DIR):
        if file.endswith('.csv'):
            print('Processing ' + file)
            with open(os.path.join(DATA_DIR, file), 'r') as fh:
                data = CSVSource(fh)

                file_row_count = 0
                file_start_time = time.time()
                set_row_count = 0
                set_start_time = time.time()

                for row in data:
                    row['system_id'] = system_id
                    row['start_station_short_name'] = row.pop('station_id')
                    row['start_station_id'] = dw.start_station_dimension.lookup(row)
                    if row['start_station_id'] is not -1:
                        insert_datetime_dimensions(row)
                        dw.station_status_fact_table.insert(row)
                    else:
                        print('\nMissing station')
                        print(row)
                        print()

                    set_row_count += 1
                    file_row_count +=1
                    overall_row_count += 1
                    if set_row_count == 10000:
                        log_time_row(set_start_time, set_row_count)
                        set_start_time = time.time()
                        set_row_count = 0

                print('\nFinished processing {}'.format(file))
                log_time_row(file_start_time, file_row_count)
                print('\n--------------------------------------------------\n\n')

    print('\n\nCompleted system status ingestion')
    log_time_row(overall_start_time, overall_row_count)


def insert_datetime_dimensions(row):
    """
    Inserts/ensures a trip's date and time information in the date and time dimensions

    :param row: the row corresponding to a trip
    :return: none, row is updated
    """

    d = parser.parse(timestr=row['time'])
    row['date_string'] = d.strftime('%Y-%m-%d')
    row['time_string'] = d.strftime('%H:%M')

    # update dimensions
    row['date_id'] = dw.date_dimension.ensure(row)
    row['time_id'] = dw.time_dimension.ensure(row)


def log_time_row(time_begin, num_rows):
    elapsed = time.time() - time_begin
    avg_row = elapsed / num_rows
    print("{:d} rows processed in {:.4f}s, {:.4f}s per row".format(num_rows, elapsed, avg_row))

if __name__ == '__main__': main()
