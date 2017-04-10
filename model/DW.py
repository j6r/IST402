"""
    Database model.
"""

from pygrametl.tables import Dimension, FactTable
from pygrametl import ConnectionWrapper
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
        
        self.station_dimension = Dimension(
            name='station',
            key='bike_station_id',
            attributes=['system_id', 'short_name', 'name', 'latitude',
                        'longitude', 'capacity'],
            lookupatts=['system_id', 'short_name']
        )

        # Trip dates and times

        self.start_date_dimension = Dimension(
            name='start_date',
            key='start_date_id',
            attributes=['start_year', 'start_month', 'start_day', 'start_day_of_week'],
            lookupatts=['start_year', 'start_month', 'start_day']
        )

        self.end_date_dimension = Dimension(
            name='end_date',
            key='end_date_id',
            attributes=['end_year', 'end_month', 'end_day', 'end_day_of_week'],
            lookupatts=['end_year', 'end_month', 'end_day']
        )

        self.start_time_dimension = Dimension(
            name='start_time',
            key='start_time_id',
            attributes=['start_hour', 'start_minute'],
            lookupatts=['start_hour', 'start_minute']
        )

        self.end_time_dimension = Dimension(
            name='end_time',
            key='end_time_id',
            attributes=['end_hour', 'end_minute'],
            lookupatts=['end_hour', 'end_minute']
        )

        # Trips

        self.trip_fact_table = FactTable(
            name='trips',
            measures=['duration_s'],
            keyrefs=['system_id', 'start_station_id', 'end_station_id', 'start_date_id', 'end_date_id', 'start_time_id',
                     'end_time_id', 'customer_birthyear_id', 'customer_gender_id', 'customer_type_id']
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
            attributes=['system_id', 'bike_name']
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


    # trips_aggregate_station_daily_fact_table = FactTable(
    #     name='trips_aggregate_station_daily',
    #     measures='num_trips',
    #     keyrefs=['from_station_id', 'to_station_id', 'start_date_id']
    # )

    #trips_aggregate_system_daily_fact_table = FactTable(
    #     name='trips_aggregate_station_daily',
    #     measures='num_trips',
    #     keyrefs=['from_station_id', 'to_station_id']
    # )
    #
    #
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
