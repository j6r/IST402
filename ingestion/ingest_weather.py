"""
Ingests weather data and associates it with systems.

Everything is hard-coded as it won't change and I'm short on time.
"""

import os
from pygrametl.datasources import CSVSource
from model.DW import DW


dw = DW()
field_mappings = {
    'precipitation_in': 'PRCP',
    'snow_in': 'SNOW',
    'temp_avg_f': 'TAVG',
    'temp_min_f': 'TMIN',
    'temp_max_f': 'TMAX',
    'wind_mph': 'AWND'
}
station_mappings = {
    'GHCND:USW00094290': 'pronto',
    'GHCND:USW00014762': 'healthyride',
    'GHCND:USW00014819': 'divvy',
    'GHCND:USW00013882': 'bike_chattanooga'
}


def main():

    data_source = os.path.join('/Users/redjen/Box Sync/IST402/data/raw/weather/weather.csv')

    with open(data_source, 'r') as fh:
        data = CSVSource(fh)

        for row in data:
            row['system_name'] = station_mappings[row['STATION']]
            row['system_id'] = dw.system_dimension.ensure(row)
            insert_datetime(row)
            dw.weather_fact_table.insert(row, namemapping=field_mappings)


def insert_datetime(row):
    # dateutil.parser doesn't detect this format
    row['weather_year'] = row['DATE'][:4]
    row['weather_month'] = row['DATE'][4:6]
    row['weather_day'] = row['DATE'][6:8]
    row['weather_date_string'] = row['weather_year'] + '-' + row['weather_month'] + '-' + row['weather_day']

if __name__ == '__main__': main()



