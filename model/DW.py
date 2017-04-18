"""
    Database model.
"""

from pygrametl.tables import Dimension, FactTable, CachedDimension
from pygrametl import ConnectionWrapper
from dateutil import parser
from util import util


class DW:

    def get_db_connection(self):
        """
        Returns the existing database connection.
        :return: database connection wrapper
        """
        return self.__targetconnection


    def __init__(self):
        db_conn = util.get_database_connection()
        self.__targetconnection = ConnectionWrapper(connection=db_conn)
        self.__create_tables()
        print('Connected database.')

    def __del__(self):
        self.__targetconnection.commit()
        self.__targetconnection.close()
        print('Closed database.')

    def __create_tables(self):

        # Systems
        self.system_dimension = CachedDimension(
            name='system',
            key='system_id',
            attributes=['system_name'],
            lookupatts=['system_name']
        )

        # Stations

        self.start_station_dimension = CachedDimension(
            name='start_station',
            key='start_station_id',
            attributes=['system_id', 'start_station_short_name', 'start_station_name', 'start_station_latitude',
                        'start_station_longitude', 'start_station_capacity'],
            lookupatts=['system_id', 'start_station_short_name'],
            rowexpander=start_station_missing_data_expander
        )
        
        self.end_station_dimension = CachedDimension(
            name='end_station',
            key='end_station_id',
            attributes=['system_id', 'end_station_short_name', 'end_station_name', 'end_station_latitude',
                        'end_station_longitude', 'end_station_capacity'],
            lookupatts=['system_id', 'end_station_short_name'],
            rowexpander=end_station_missing_data_expander
        )

        # Trip dates and times

        self.date_dimension = CachedDimension(
            name='bdate',
            key='date_id',
            attributes=['year', 'month', 'day', 'day_of_week', 'date_string'],
            lookupatts=['date_string'],
            rowexpander=date_row_expander
        )

        self.time_dimension = CachedDimension(
            name='btime',
            key='time_id',
            attributes=['hour', 'minute', 'time_string', 'time_of_day'],
            lookupatts=['time_string'],
            rowexpander=time_row_expander
        )


        # Trips

        self.trip_fact_table = FactTable(
            name='trips',
            measures=['duration_s'],
            keyrefs=['system_id', 'start_station_id', 'end_station_id', 'date_id', 'time_id',
                     'customer_birthyear_id', 'customer_gender_id', 'customer_type_id', 'bike_id', 'trip_category_id']
        )

        # weather fact table and date dimension

        self.weather_fact_table = FactTable(
            name='weather',
            measures=['precipitation_in', 'snow_in', 'temp_avg_f', 'temp_min_f', 'temp_max_f', 'wind_mph'],
            keyrefs=['system_id', 'date_id']
        )

        self.trip_category = CachedDimension(
            name='trip_category',
            key='trip_category_id',
            attributes=['trip_category']
        )

        self.bike_dimension = CachedDimension(
            name='bikes',
            key='bike_id',
            attributes=['system_id', 'bike_name'],
            lookupatts=['system_id', 'bike_name'],
            defaultidvalue=-1
        )

        self.customer_gender_dimension = CachedDimension(
            name='customer_gender',
            key='customer_gender_id',
            attributes=['customer_gender'],
            lookupatts=['customer_gender']
        )

        self.customer_birthyear_dimension = CachedDimension(
            name='customer_birthyear',
            key='customer_birthyear_id',
            attributes=['customer_birthyear'],
            lookupatts=['customer_birthyear']
        )

        self.customer_type_dimension = CachedDimension(
            name='customer_type',
            key='customer_type_id',
            attributes=['customer_type'],
            lookupatts=['customer_type']
        )

        # Station status
        self.station_status_fact_table = FactTable(
            name='station_status',
            keyrefs=['system_id', 'start_station_id', 'date_id', 'time_id'],
            measures=['bikes_available', 'docks_available']
        )

        # Non-cached version of stations for use only with updating Indego stations.

        self.start_station_noncached_dimension = Dimension(
            name='start_station',
            key='start_station_id',
            attributes=['system_id', 'start_station_short_name', 'start_station_name', 'start_station_latitude',
                        'start_station_longitude', 'start_station_capacity'],
            lookupatts=['system_id', 'start_station_short_name'],
            rowexpander=start_station_missing_data_expander,
            defaultidvalue=-1
        )

        self.end_station_noncached_dimension = Dimension(
            name='end_station',
            key='end_station_id',
            attributes=['system_id', 'end_station_short_name', 'end_station_name', 'end_station_latitude',
                        'end_station_longitude', 'end_station_capacity'],
            lookupatts=['system_id', 'end_station_short_name'],
            rowexpander=end_station_missing_data_expander,
            defaultidvalue=-1
        )


def date_row_expander(row, namemapping):
    d = parser.parse(row['date_string'])
    row['year'] = d.year
    row['month'] = d.month
    row['day'] = d.day
    row['day_of_week'] = d.isoweekday()
    row['date_string'] = row['date_string']
    return row


def time_row_expander(row, namemapping):
    d = parser.parse(row['time_string'])
    row['hour'] = d.hour
    row['minute'] = d.minute
    row['time_of_day'] = get_time_of_day(row['hour'])
    return row


def start_station_missing_data_expander(row, namemapping):
    for attr in ['start_station_name', 'start_station_latitude', 'start_station_longitude', 'start_station_capacity']:
        if attr not in row.keys():
            row['attr'] = ''
    return row


def end_station_missing_data_expander(row, namemapping):
    for attr in ['end_station_name', 'end_station_latitude', 'end_station_longitude', 'end_station_capacity']:
        if attr not in row.keys():
            row['attr'] = ''
    return row


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