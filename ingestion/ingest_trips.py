"""
    Ingest trip data
"""


import os
from dateutil import parser
from pygrametl.datasources import CSVSource
from model.DW import DW
from settings import settings

cfg = settings.get_config()
dw = DW()


def main():

    # Insert "unknown" row for optional keyrefs

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
                            print('Processing ' + file)

                            # Process each row
                            for row in data:
                                row['system_name'] = source
                                row['system_id'] = dw.system_dimension.ensure(row)

                                fix_mappings(row, mappings)
                                insert_datetime_dimensions(row)
                                insert_missing_fields(row)
                                dw.bike_dimension.ensure(row)

                                # TODO: pygrametl calls ensure before remapping keys?
                                # We have to get the stations key here instead of pygrametl doing it automatically
                                row['start_station_id'] = dw.start_station_dimension.lookup({
                                    'system_id': row['system_id'],
                                    'start_station_short_name': row['start_station_short_name']
                                })
                                row['end_station_id'] = dw.end_station_dimension.lookup({
                                    'system_id': row['system_id'],
                                    'end_station_short_name': row['end_station_short_name']
                                })

                                dw.trip_fact_table.ensure(row)

                except IndentationError as e:
                    print(e)


def insert_datetime_dimensions(row):
    """
    Inserts/ensures a trip's date and time information in the date and time dimensions

    :param row: the row corresponding to a trip
    :return: none, row is updated
    """

    datetime_row = {}

    # start date and time
    d = parser.parse(row['time_start'])

    datetime_row['start_date_string'] = d.strftime('yyyy-MM-dd')
    datetime_row['start_year'] = d.year
    datetime_row['start_month'] = d.month
    datetime_row['start_day'] = d.day
    datetime_row['start_day_of_week'] = d.isoweekday()
    datetime_row['start_hour'] = d.hour
    datetime_row['start_minute'] = d.minute
    datetime_row['start_time_of_day'] = get_time_of_day(datetime_row['start_hour'])

    # end date and time
    d = parser.parse(row['time_stop'])
    datetime_row['end_date_string'] = d.strftime('yyyy-MM-dd')
    datetime_row['end_year'] = d.year
    datetime_row['end_month'] = d.month
    datetime_row['end_day'] = d.day
    datetime_row['end_day_of_week'] = d.isoweekday()
    datetime_row['end_hour'] = d.hour
    datetime_row['end_minute'] = d.minute
    datetime_row['end_time_of_day'] = get_time_of_day(datetime_row['end_hour'])

    # update dimensions

    row['start_date_id'] = dw.start_date_dimension.ensure(datetime_row)
    row['start_time_id'] = dw.start_time_dimension.ensure(datetime_row)
    row['end_date_id'] = dw.end_date_dimension.ensure(datetime_row)
    row['end_time_id'] = dw.end_time_dimension.ensure(datetime_row)


def get_time_of_day(hour):
    """
    Determines the time of day by hour
    :param hour: hour of day
    :return: time of day from early morning, morning, afternoon, evening
    """
    if hour < 7:
        return 'early_morning'
    elif hour < 12:
        return 'morning'
    elif hour < 6:
        return 'afternoon'
    else:
        return 'evening'


def fix_mappings(row, mappings):
    """
    Sets the field mappings for each file.
    Some of the bike share systems don't have consistent headings and file formats in all of the data files, so we
     need to detect and correct as needed.
    :param csvfile: the csv data source
    :param file_mappings: known field mappings for the system
    :return: updates the csvfile in place
    """

    for k in mappings:
        if k in row.keys():
            row[mappings[k]] = row.pop(k)


def insert_missing_fields(row):
    for k in dw.trip_fact_table.keyrefs:
        if k not in row:
            row[k] = -1




if __name__ == '__main__': main()
