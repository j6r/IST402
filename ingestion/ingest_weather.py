"""
Ingests weather data and associates it with systems.
"""

import os
from pygrametl.datasources import CSVSource
from model.DW import DW
from settings import settings

cfg = settings.get_config()
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
    'GHCND:USW00013882': 'bike_chattanooga',
    'GHCND:USW00023234': 'babs'
}


def main():

    data_dir = os.path.join(cfg['weather']['weather_data_dir'])
    for file in os.listdir(data_dir):
            if file.endswith('.csv'):

                with open(os.path.join(data_dir, file), 'r') as fh:
                    data = CSVSource(fh)

                    for row in data:
                        if row['STATION'] in station_mappings.keys():
                            row['system_name'] = station_mappings[row['STATION']]
                            row['system_id'] = dw.system_dimension.ensure(row)
                            insert_datetime(row)
                            dw.weather_fact_table.insert(row, namemapping=field_mappings)


def insert_datetime(row):
    # dateutil.parser doesn't detect this format
    row['date_string'] = row['DATE'][:4] + '-' +  row['DATE'][4:6] + '-' + row['DATE'][6:8]
    row['date_id'] = dw.date_dimension.ensure(row)

if __name__ == '__main__': main()



