-- extracts the complete weather fact table and system name

select s.system_name as 'system name', w.weather_date_string as 'date', weather_year, weather_month,
    weather_day, precipitation_in, snow_in, temp_min_f, temp_max_f, temp_avg_f, wind_mph
from weather w left join system s on w.system_id=s.system_id