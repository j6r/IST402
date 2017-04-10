create table system(
    system_id integer primary key,
    system_name text
);

create table start_station(
    start_station_id integer primary key,
    system_id text not null,
    start_station_short_name text not null,
    start_station_name text not null,
    start_station_latitude real not null,
    start_station_longitude real not null,
    start_station_elevation real,
    start_station_capacity integer
);

create table end_station(
    end_station_id integer primary key,
    system_id text not null,
    end_station_short_name text not null,
    end_station_name text not null,
    end_station_latitude real not null,
    end_station_longitude real not null,
    end_station_elevation real,
    end_station_capacity integer
);

create table station(
    bike_station_id integer primary key,
    system_id text not null,
    short_name text not null,
    name text not null,
    latitude real not null,
    longitude real not null,
    elevation real,
    capacity integer
);

create table start_date(
    start_date_id integer primary key,
    start_date_string text,
    start_year integer,
    start_month integer,
    start_day integer,
    start_day_of_week integer   
);

create table end_date(
    end_date_id integer primary key,
    end_date_string text,
    end_year integer,
    end_month integer,
    end_day integer,
    end_day_of_week integer   
);

create table start_time(
    start_time_id integer primary key,
    start_time_string text,
    start_hour integer,
    start_minute integer,
    start_time_of_day text
);

create table end_time(
    end_time_id integer primary key,
    end_time_string text,
    end_hour integer,
    end_minute integer,
    end_time_of_day text
);

-- fact tables

create table trips(
    trip_id integer primary key,
    system_id integer,
    duration_s integer,
    start_station_id integer,
    end_station_id integer,
    start_date_id integer,
    end_date_id integer,
    start_time_id integer,
    end_time_id integer,
    customer_birthyear_id integer,
    customer_type_id integer,
    customer_gender_id integer
);

create table weather(
    weather_id integer primary key,
    system_id integer,
    precipitation_in real,
    snow_in real,
    temp_min_f real,
    temp_max_f real,
    temp_avg_f real,
    wind_mph real,
    weather_date_string text,
    weather_year integer,
    weather_month integer,
    weather_day integer
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
