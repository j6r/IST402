-- This query creates a new table and aggregates trip counts by day by system.

drop table if exists trips_aggregated_system_daily_count;

create table trips_aggregated_system_daily_count(
    system_id integer,
    start_date_id integer,
    daily_count integer
);

insert into trips_aggregated_system_daily_count(system_id, start_date_id, daily_count)
    VALUES (t.system_id, t.start_date_id, count(*))
    from trips t
    group by system_id, start_date_id;
