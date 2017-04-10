"""
    Ingest trip data into the staging database.
"""

import logging
import os
import time
from dateutil import parser
from pygrametl.datasources import CSVSource
from model.DW import DW
from settings import settings

cfg = settings.get_config()
dw = DW()
logging.basicConfig(level=logging.DEBUG)


def main():

    data_dir = os.path.join(cfg['ingestion_settings']['data_directory'])

    # for each system in the config file
    for source in cfg['datasources'].keys():
        trip_dir = os.path.join(data_dir, source, cfg['ingestion_settings']['trips_directory'])
        mappings = cfg['datasources'][source]['trip_fields']

        # if the system's trip directory is present
        if os.path.isdir(trip_dir):

            # for every file in the trip directory
            for file in os.listdir(trip_dir):

                # Abandon the current file if a parsing exception occurs and move on to the next
                try:

                    if file.endswith('.csv'):
                        with open(os.path.join(trip_dir, file), 'r') as fh:
                            data = CSVSource(fh)
                            logging.info("Processing " + file)

                            # Process each row
                            for row in data:
                                row['system_name'] = source
                                row['system_id'] = dw.system_dimension.ensure(row)

                                fix_mappings(row, mappings)
                                insert_datetime_dimensions(row)
                                row['bike_id'] = dw.bike_dimension.ensure(row)
                                row['start_station_id'] = dw.start_station_dimension.ensure(row)
                                row['end_station_id'] = dw.end_station_dimension.ensure(row)
                                row['customer_gender_id'] = dw.customer_gender_dimension.ensure(row)
                                row['customer_birthyear_id'] = dw.customer_birthyear_dimension.ensure(row)
                                row['customer_type_id'] = dw.customer_type_dimension.ensure(row)

                                dw.trip_fact_table.insert(row)

                            logging.info('Completed processing ' + file)
                            dw.get_db_connection().commit()

                except Exception as e:
                    logging.exception('Exception occurred while parsing: ' + file,
                                      exc_info=True)


def insert_datetime_dimensions(row):
    """
    Inserts/ensures a trip's date and time information in the date and time dimensions

    :param row: the row corresponding to a trip
    :return: none, row is updated
    """

    d = parser.parse(row['time_start'])
    row['start_date_string'] = d.strftime('%Y-%m-%d')
    row['start_time_string'] = d.strftime('%H:%M')
    d = parser.parse(row['time_stop'])
    row['end_date_string'] = d.strftime('%Y-%m-%d')
    row['end_time_string'] = d.strftime('%H:%M')

    # update dimensions
    row['end_date_id'] = dw.end_date_dimension.ensure(row)
    row['start_date_id'] = dw.start_date_dimension.ensure(row)
    row['start_time_id'] = dw.start_time_dimension.ensure(row)
    row['end_time_id'] = dw.end_time_dimension.ensure(row)


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


def insert_missing_fields(row):
    """
    Inserts missing keyrefs.

    Bike share systems do not include all fields in their data. This method inserts
    keys for missing lookuprefs in the trips fact table.
    :param row: data row
    :return: updates the row in place
    """
    for k in dw.trip_fact_table.keyrefs:
        if k not in row:
            row[k] = -1


if __name__ == '__main__': main()
