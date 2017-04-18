from googlemaps import Client
from googlemaps.elevation import elevation
from settings import settings
from util import util

API_KEY = settings.get_config()['google_api']['api_key']

GET_START_STATIONS_QUERY = """
SELECT start_station_id, start_station_latitude, start_station_longitude
    FROM start_station
    WHERE start_station_latitude IS NOT NULL AND start_station_longitude IS NOT null
        AND start_station_latitude IS NOT '' AND start_station_longitude IS NOT ''
        AND start_station_elevation IS NULL
    LIMIT 500
"""

GET_END_STATIONS_QUERY = """
SELECT end_station_id, end_station_latitude, end_station_longitude
    FROM end_station
    WHERE end_station_latitude IS NOT NULL AND end_station_longitude IS NOT null
        AND end_station_latitude IS NOT '' AND end_station_longitude IS NOT ''
        AND end_station_elevation IS NULL
    LIMIT 500
"""

UPDATE_START_STATION_QUERY = """
    UPDATE start_station
    SET start_station_elevation = ?
    WHERE start_station_latitude = ? AND start_station_longitude = ?
"""

UPDATE_END_STATION_QUERY = """
    UPDATE end_station
    SET end_station_elevation = ?
        WHERE end_station_latitude = ? AND end_station_longitude = ?
"""

GET_TRIPS_QUERY = """
    UPDATE trips

"""


def main():

    # Get missing station elevations
    # [{'elevation': 182.4646606445312, 'location': {'lat': 41.93625, 'lng': -87.65357}, 'resolution': 152.7032318115234}]

    gmaps = Client(key=API_KEY)

    # print(elevation.elevation(gmaps, (41.936253, -87.653566)))

    db_conn = util.get_database_connection()

    while True:
        stations = db_conn.execute(GET_START_STATIONS_QUERY).fetchall()
        locations = list(map(lambda x: (x[1], x[2]), stations))
        if len(locations) > 0:
            location_results = elevation(gmaps, locations)
            updates = []
            for l, s in zip(location_results, stations):
                updates.append((l['elevation'], s[1], s[2]))

            db_conn.executemany(UPDATE_START_STATION_QUERY, updates)
            db_conn.commit()
        else:
            break

    while True:
        stations = db_conn.execute(GET_END_STATIONS_QUERY).fetchall()
        locations = list(map(lambda x: (x[1], x[2]), stations))
        if len(locations) > 0:
            location_results = elevation(gmaps, locations)
            updates = []
            for l, s in zip(location_results, stations):
                updates.append((l['elevation'], s[1], s[2]))

            db_conn.executemany(UPDATE_END_STATION_QUERY, updates)
            db_conn.commit()
        else:
            break

    # Add elevation difference to trips




    db_conn.close()


if __name__ == "__main__": main()

