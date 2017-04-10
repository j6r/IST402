"""
    Ingest station data.
"""

import os
from pygrametl.datasources import CSVSource
from model.DW import DW
from settings import settings


def main():
    cfg = settings.get_config()
    data_dir = os.path.join(cfg['ingestion_settings']['data_directory'])
    dw = DW()

    for source in cfg['datasources'].keys():

        if os.path.isdir(os.path.join(data_dir, source)):
            print('Processing: ' + source)

            mappings = cfg['datasources'][source]['station_fields']

            with open(os.path.join(data_dir, source, 'stations.csv'), 'r') as fh:

                station_data = CSVSource(fh)

                for row in station_data:
                    row['missing'] = None
                    row['system_name'] = source
                    row['system_id'] = dw.system_dimension.ensure(row)
                    station_id = dw.station_dimension.ensure(row, mappings)
                    row = dw.station_dimension.getbykey(station_id)

                    dw.start_station_dimension.ensure(row, namemapping={
                        'start_station_short_name': 'short_name',
                        'start_station_name': 'name',
                        'start_station_longitude': 'longitude',
                        'start_station_latitude': 'latitude',
                        'start_station_capacity': 'capacity'
                    })

                    dw.end_station_dimension.ensure(row, namemapping={
                        'end_station_short_name': 'short_name',
                        'end_station_name': 'name',
                        'end_station_longitude': 'longitude',
                        'end_station_latitude': 'latitude',
                        'end_station_capacity': 'capacity'
                    })



                dw.get_db_connection().commit()



if __name__ == "__main__": main()