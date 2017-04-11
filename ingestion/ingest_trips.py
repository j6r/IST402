"""
    Ingest trip data into the staging database.
"""

import logging
import os
import time
from dateutil import parser
from pygrametl.datasources import CSVSource
import pygrametl
from model.DW import DW
from settings import settings

cfg = settings.get_config()
dw = DW()
logging.basicConfig(filename='/Users/redjen/Desktop/trips_ingestion.log', level=logging.DEBUG)


def main():

    data_dir = os.path.join(cfg['ingestion_settings']['data_directory'])

    overall_start_time = time.time()
    overall_row_count = 0

    # for each system in the config file
    for source in cfg['datasources'].keys():
        trip_dir = os.path.join(data_dir, source, cfg['ingestion_settings']['trips_directory'])
        mappings = cfg['datasources'][source]['trip_fields']

        # if the system's trip directory is present
        if os.path.isdir(trip_dir):

            logging.info('\n--------------------------------------------------' +
                         '\nProcessing trip data for ' + source)
            source_start_time = time.time()
            source_rowcout = 0

            # for every file in the trip directory
            for file in os.listdir(trip_dir):

                # Abandon the current file if a parsing exception occurs and move on to the next
                try:

                    if file.endswith('.csv'):
                        with open(os.path.join(trip_dir, file), 'r') as fh:
                            data = CSVSource(fh)
                            logging.info("Processing " + file)

                            file_rowcount = 0
                            file_start_time = time.time()
                            set_rowcount = 0
                            set_start_time = time.time()

                            # Process each row
                            for row in data:
                                row['system_name'] = source
                                row['system_id'] = dw.system_dimension.ensure(row)

                                fix_mappings(row, mappings)
                                insert_datetime_dimensions(row)
                                insert_missing_fields(row)
                                insert_customer_dimensions(row)

                                row['bike_id'] = dw.bike_dimension.ensure(row)
                                row['start_station_id'] = dw.start_station_dimension.ensure(row)
                                row['end_station_id'] = dw.end_station_dimension.ensure(row)

                                dw.trip_fact_table.insert(row)
                                set_rowcount += 1
                                file_rowcount += 1
                                source_rowcout += 1
                                overall_row_count += 1

                                if set_rowcount == 1000:
                                    log_time_row(set_start_time, set_rowcount)
                                    set_rowcount = 0
                                    set_start_time = time.time()

                            logging.info('\nCompleted processing ' + file)
                            log_time_row(file_start_time, file_rowcount)

                except Exception as e:
                    logging.exception('Exception occurred while parsing: ' + file,
                                      exc_info=True)

            logging.info('\nFinished processing ' + source)
            log_time_row(source_rowcout, source_start_time)

    logging.info('\n\n\nCompleted ingestion of trip data')
    log_time_row(overall_start_time, overall_row_count)


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


def insert_customer_dimensions(row):
    defaults = [
        ('customer_gender', 'unspecified'),
        ('customer_birthyear', -1),
        ('customer_type', 'unspecified')
    ]
    pygrametl.setdefaults(row, defaults)
    row['customer_gender_id'] = dw.customer_gender_dimension.ensure(row)
    row['customer_birthyear_id'] = dw.customer_birthyear_dimension.ensure(row)
    row['customer_type_id'] = dw.customer_type_dimension.ensure(row)


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


def log_time_row(time_begin, num_rows):
    elapsed = time.time() - time_begin
    avg_row = elapsed / num_rows
    logging.info(
        "Processed {:d} rows, {:.2f} s, {:.4f} per row".format(num_rows, elapsed, avg_row)
    )


if __name__ == '__main__': main()
