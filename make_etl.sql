-- Обработаем добавление лишь новых данных
drop table if exists kdz_10_etl.flights_loaded;
create table if not exists kdz_10_etl.flights_loaded(
	loaded_ts timestamp not NULL
);

drop table if exists kdz_10_etl.weather_loaded;
create table if not exists kdz_10_etl.weather_loaded(
	loaded_ts timestamp not NULL
);

-- шаг 01. Какие правки самые свежие?
drop table if exists kdz_10_etl.flights_ts;
create table if not exists kdz_10_etl.flights_ts as 
select 
	min(loaded_ts) as ts1, 
	max(loaded_ts) as ts2 
from kdz_10_src.flights
where loaded_ts > coalesce((select max(loaded_ts) from kdz_10_etl.flights_loaded), '1970-01-01');

drop table if exists kdz_10_etl.weather_ts;
create table if not exists kdz_10_etl.weather_ts as 
select 
	min(loaded_ts) as ts1, 
	max(loaded_ts) as ts2 
from kdz_10_src.weather
where loaded_ts > coalesce((select max(loaded_ts) from kdz_10_etl.weather_loaded), '1970-01-01');

-- шаг 02. Чтение сырых данных, которые ранее не были обработаны
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
from kdz_10_src.flights, kdz_10_etl.flights_ts
where loaded_ts > ts1 and loaded_ts <=ts2;

drop table if exists kdz_10_etl.weather;
create table if not exists kdz_10_etl.weather as 
select distinct
	'KCMH' as icao,
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
from kdz_10_src.weather, kdz_10_etl.weather_ts
where loaded_ts > ts1 and loaded_ts <=ts2;


-- шаг 03. Запись в целевую таблицу в режиме upsert
insert into kdz_10_staging.weather
select 
  icao,
  local_datetime,
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
from kdz_10_etl.weather
  on conflict(icao, local_datetime)
  do update
  set 
  local_datetime = excluded.local_datetime,
  t_air_temperature = excluded.t_air_temperature,
  p0_sea_lvl = excluded.p0_sea_lvl,
  p_station_lvl = excluded.p_station_lvl,
  u_humidity = excluded.u_humidity,
  dd_wind_direction = excluded.dd_wind_direction,
  ff_wind_speed = excluded.ff_wind_speed,
  ff10_max_gust_value = excluded.ff10_max_gust_value,
  ww_present = excluded.ww_present,
  ww_recent = excluded.ww_recent,
  c_total_clouds = excluded.c_total_clouds,
  vv_horizontal_visibility = excluded.vv_horizontal_visibility,
  td_temperature_dewpoint = excluded.td_temperature_dewpoint,
  loaded_ts = now()
;

insert into kdz_10_staging.flights
  select
  year,
  quarter,
  month,
  flight_date,
  dep_time,
  crs_dep_time,
  air_time,
  dep_delay_minutes,
  cancelled bool,
  cancellation_code,
  weather_delay,
  reporting_airline,
  tail_number,
  flight_number,
  distance,
  origin,
  dest
  from kdz_10_etl.flights
  on conflict(flight_date, flight_number, origin, dest, crs_dep_time)
  do update
  set 
  year = excluded.year,
  quarter = excluded.quarter,
  month = excluded.month,
  flight_date = excluded.flight_date,
  dep_time = excluded.dep_time,
  crs_dep_time = excluded.crs_dep_time,
  air_time = excluded.air_time,
  dep_delay_minutes = excluded.dep_delay_minutes,
  cancelled = excluded.cancelled,
  cancellation_code = excluded.cancellation_code,
  weather_delay = excluded.weather_delay,
  reporting_airline = excluded.reporting_airline,
  tail_number = excluded.tail_number,
  flight_number = excluded.flight_number,
  distance = excluded.distance,
  origin = excluded.origin,
  dest = excluded.dest,
  loaded_ts = now()
;

insert into kdz_10_staging.flights_cancellation 
  select code, description, now() as loaded_ts from kdz_10_src.flights_cancellation;

insert into kdz_10_staging.flights_carriers
  select code, description, now() as loaded_ts from kdz_10_src.flights_carriers;

-- шаг 04. обновление последней известной метки loaded_ts
delete from kdz_10_etl.flights_loaded 
where exists (select 1 from kdz_10_etl.flights_ts);

delete from kdz_10_etl.weather_loaded 
where exists (select 1 from kdz_10_etl.weather_ts);

insert into kdz_10_etl.flights_loaded
select ts2 
from kdz_10_etl.flights_ts
where exists (select 1 from kdz_10_etl.flights_ts);

insert into kdz_10_etl.weather_ts
select ts2 
from kdz_10_etl.weather_ts
where exists (select 1 from kdz_10_etl.weather_ts);


--drop table etl.load_cust_03_01;

