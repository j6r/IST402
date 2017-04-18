create table system(
    system_id integer primary key,
    system_name text
);

create table station(
    station_id integer primary key,
    system_id text not null,
    short_name text not null,
    name text not null,
    latitude real,
    longitude real,
    elevation real,
    capacity integer
);

create table start_station(
    start_station_id integer primary key,
    system_id text not null,
    start_station_short_name text not null,
    start_station_name text not null,
    start_station_latitude real,
    start_station_longitude real,
    start_station_elevation real,
    start_station_capacity integer
);

create table end_station(
    end_station_id integer primary key,
    system_id text not null,
    end_station_short_name text not null,
    end_station_name text not null,
    end_station_latitude real,
    end_station_longitude real,
    end_station_elevation real,
    end_station_capacity integer
);

create table bdate(
    date_id integer primary key,
    date_string text,
    year integer,
    month integer,
    day integer,
    day_of_week integer
);

create table btime(
    time_id integer primary key,
    time_string text,
    hour integer,
    minute integer,
    time_of_day text
);

create table trip_category(
    trip_category_id integer primary key,
    trip_category text
);

create table customer_gender(
    customer_gender_id integer primary key,
    customer_gender text
);
insert into customer_gender VALUES(-1, 'unspecified');

create table customer_birthyear(
    customer_birthyear_id integer primary key,
    customer_birthyear integer
);
insert into customer_birthyear VALUES(-1, -1);

create table customer_type(
    customer_type_id integer primary key,
    customer_type text
);
insert into customer_type VALUES(-1, 'unspecified');

create table bikes(
    bike_id integer primary key,
    system_id integer,
    bike_name text
);

/*
 * FACT TABLES
 */

create table trips(
    trip_id integer primary key,
    system_id integer,
    duration_s integer,
    start_station_id integer,
    end_station_id integer,
    date_id integer,
    time_id integer,
    customer_birthyear_id integer,
    customer_type_id integer,
    customer_gender_id integer,
    bike_id integer,
    trip_category_id integer
    elevation_diff_m real
);


/*
 * WEATHER
 */


create table weather(
    weather_id integer primary key,
    system_id integer,
    date_id integer,
    precipitation_in real,
    snow_in real,
    temp_min_f real,
    temp_max_f real,
    temp_avg_f real,
    wind_mph real
);

create table station_status(
    system_id integer,
    station_id integer,
    date_id integer,
    time_id integer,
    bikes_available integer,
    docks_available integer
);


/*
 *  Indexes
 */

CREATE INDEX station_latlong_index ON station (
    latitude,
    longitude
);

CREATE INDEX start_station_latlong_index ON start_station (
    start_station_latitude,
    start_station_longitude
);

CREATE INDEX end_station_latlong_index ON end_station (
    end_station_latitude,
    end_station_longitude
);

CREATE INDEX date_index ON bdate (
    year,
    month,
    day
);

CREATE INDEX time_index ON btime (
    hour,
    minute
);