"""
    Database model.
"""

from pygrametl.tables import Dimension, FactTable
from pygrametl import ConnectionWrapper
import pygrametl
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
        self.start_station_dimension = None
        self.end_station_dimension = None

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
        self.system_dimension = Dimension(
            name='system',
            key='system_id',
            attributes=['system_name'],
            lookupatts=['system_name']
        )

        # Stations

        self.start_station_dimension = Dimension(
            name='start_station',
            key='start_station_id',
            attributes=['system_id', 'start_station_short_name', 'start_station_name', 'start_station_latitude',
                        'start_station_longitude', 'start_station_capacity'],
            lookupatts=['system_id', 'start_station_short_name']
        )
        
        self.end_station_dimension = Dimension(
            name='end_station',
            key='end_station_id',
            attributes=['system_id', 'end_station_short_name', 'end_station_name', 'end_station_latitude',
                        'end_station_longitude', 'end_station_capacity'],
            lookupatts=['system_id', 'end_station_short_name']
        )

        # Trip dates and times

        self.start_date_dimension = Dimension(
            name='start_date',
            key='start_date_id',
            attributes=['start_year', 'start_month', 'start_day', 'start_day_of_week', 'start_date_string'],
            lookupatts=['start_date_string'],
            rowexpander=start_date_row_expander
        )

        self.end_date_dimension = Dimension(
            name='end_date',
            key='end_date_id',
            attributes=['end_year', 'end_month', 'end_day', 'end_day_of_week', 'end_date_string'],
            lookupatts=['end_date_string'],
            rowexpander=end_date_row_expander
        )

        self.start_time_dimension = Dimension(
            name='start_time',
            key='start_time_id',
            attributes=['start_hour', 'start_minute', 'start_time_string', 'start_time_of_day'],
            lookupatts=['start_time_string'],
            rowexpander=start_time_row_expander
        )

        self.end_time_dimension = Dimension(
            name='end_time',
            key='end_time_id',
            attributes=['end_hour', 'end_minute', 'end_time_string', 'end_time_of_day'],
            lookupatts=['end_time_string'],
            rowexpander=end_time_row_expander
        )

        # Trips

        self.trip_fact_table = FactTable(
            name='trips',
            measures=['duration_s'],
            keyrefs=['system_id', 'start_station_id', 'end_station_id', 'start_date_id', 'end_date_id', 'start_time_id',
                     'end_time_id', 'customer_birthyear_id', 'customer_gender_id', 'customer_type_id', 'bike_id']
        )

        # weather fact table and date dimension

        self.weather_fact_table = FactTable(
            name='weather',
            measures=['precipitation_in', 'snow_in', 'temp_avg_f', 'temp_min_f', 'temp_max_f', 'wind_mph', 'weather_date_string','weather_year', 'weather_month', 'weather_day'],
            keyrefs=['system_id', 'weather_year', 'weather_month', 'weather_day']
        )

        self.bike_dimension = Dimension(
            name='bikes',
            key='bike_id',
            attributes=['system_id', 'bike_name'],
            lookupatts=['system_id', 'bike_name']
        )

        self.customer_gender_dimension = Dimension(
            name='customer_gender',
            key='customer_gender_id',
            attributes=['customer_gender'],
            lookupatts=['customer_gender'],
            defaultidvalue=-1
        )

        self.customer_birthyear_dimension = Dimension(
            name='customer_birthyear',
            key='customer_birthyear_id',
            attributes=['customer_birthyear'],
            lookupatts=['customer_birthyear'],
            defaultidvalue=-1
        )

        self.customer_type_dimension = Dimension(
            name='customer_type',
            key='customer_type_id',
            attributes=['customer_type'],
            lookupatts=['customer_type'],
            defaultidvalue=-1
        )

    # # Station status data
    # station_status__date_dimension = Dimension(
    #     name='station_status_date',
    #     key='station_status_id',
    #     attributes=['station_status_year', 'station_status_month', 'station_status_day', 'station_status_day_of_week'],
    #     lookupatts=['station_status_year', 'station_status_month', 'station_status_day']
    # )
    #
    # station_status_time_dimension = Dimension(
    #     name='station_status_time',
    #     key='station_status_time_id',
    #     attributes=['station_status_hour', 'station_status_minute'],
    #     lookupatts=['station_status_hour', 'station_status_minute']
    # )
    #
    # station_status_station_station_dimension = Dimension(
    #     name='station_status_station',
    #     key='station_status_station_id',
    #     attributes=['station_status_station_system_id', 'station_status_station_short_name', 'station_status_station_name', 'station_status_station_latitude', 'station_status_station_longitude', 'station_status_station_elevation', 'station_status_station_capacity'],
    #     lookupatts=['station_status_station_system_id', 'station_status_station_short_name']
    # )
    #
    # station_status_system_dimension = Dimension(
    #     name='station_status_system',
    #     key='station_status_system_dimension_id',
    #     attributes=['station_status_system_name'],
    #     lookupatts=['station_status_system_name']
    # )
    #
    # station_status_fact_table = FactTable(
    #     name='station_status',
    #     key='station_status_id',
    #     keyrefs=['station_status_date_id', 'station_status_time_id', 'station_status_station_id']
    # )


def end_date_row_expander(row, namemapping):
    d = parser.parse(row['end_date_string'])
    row['end_year'] = d.year
    row['end_month'] = d.month
    row['end_day'] = d.day
    row['end_day_of_week'] = d.isoweekday()
    row['end_date_string'] = row['end_date_string']
    return row


def start_date_row_expander(row, namemapping):
    d = parser.parse(row['start_date_string'])
    row['start_year'] = d.year
    row['start_month'] = d.month
    row['start_day'] = d.day
    row['start_day_of_week'] = d.isoweekday()
    row['start_date_string'] = row['start_date_string']
    return row


def start_time_row_expander(row, namemapping):
    d = parser.parse(row['start_time_string'])
    row['start_hour'] = d.hour
    row['start_minute'] = d.minute
    row['start_time_of_day'] = get_time_of_day(row['start_hour'])
    return row


def end_time_row_expander(row, namemapping):
    d = parser.parse(row['end_time_string'])
    row['end_hour'] = d.hour
    row['end_minute'] = d.minute
    row['end_time_of_day'] = get_time_of_day(row['end_hour'])
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


