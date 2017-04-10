"""
Extracts the headings from trip files and dumps them into a CSV.
"""

from csv import writer, DictReader
import os
from settings import settings


cfg = settings.get_config()


data_dir = os.path.join(cfg['ingestion_settings']['data_directory'])

# for each system in the config file
for source in cfg['datasources'].keys():
    trip_dir = os.path.join(data_dir, source, cfg['ingestion_settings']['trips_directory'])
    mappings = cfg['datasources'][source]['trip_fields']

    # if the system's trip directory is present
    if os.path.isdir(trip_dir):

        # for every file in the trip directory
        for file in os.listdir(trip_dir):

            if file.endswith('.csv'):
                with open(os.path.join(trip_dir, file), 'r') as ifh, open('/Users/redjen/Desktop/headings.csv', 'a') as ofh:
                    line = ifh.readline()
                    write_headings = writer(ofh)
                    headings = [source] + [file] + line.split(sep=',')
                    write_headings.writerow(headings)