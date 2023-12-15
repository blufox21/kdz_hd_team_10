drop table if exists kdz_10_etl.flights;
create table if not exists kdz_10_etl.flights as 
select distinct
	year,
	quarter,
	month,
	TO_DATE(flight_date, 'MM/DD/YYYY') as flight_date,
	to_timestamp(dep_time, 'HH24MI')::time as dep_time,
	to_timestamp(crs_dep_time, 'HH24MI')::time as crs_dep_time,
	air_time,
	dep_delay_minutes,
	CAST(CAST(cancelled as int) as bool) as cancelled,
	cancellation_code,
	weather_delay,
	reporting_airline,
	tail_number,
	flight_number,
	distance,
	origin,
	dest
from kdz_10_src.flights;

drop table if exists kdz_10_etl.weather;
create table if not exists kdz_10_etl.weather as 
select distinct
	to_timestamp(local_datetime, 'DD.MM.YYYY HH24:MI') as local_datetime,
	t_air_temperature,
	p0_sea_lvl,
	p_station_lvl,
	u_humidity,
	dd_wind_direction,
	ff_wind_speed,
	ff10_max_gust_value,
	ww_present,
	ww_recent,
	c_total_clouds,
	vv_horizontal_visibility,
	td_temperature_dewpoint
from kdz_10_src.weather;
