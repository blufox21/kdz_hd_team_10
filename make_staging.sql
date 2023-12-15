drop table if exists kdz_10_staging.flights;
CREATE TABLE kdz_10_staging.weather (
  icao_code varchar(10) NOT NULL,
  local_datetime timestamptz NOT NULL,
  t_air_temperature numeric(3, 1)  NULL,
  p0_sea_lvl numeric(4, 1) NULL,
  p_station_lvl numeric(4, 1) NULL,
  u_humidity int4 NULL,
  dd_wind_direction varchar(100) NULL,
  ff_wind_speed int4 NULL,
  ff10_max_gust_value int4 NULL,
  ww_present varchar(100) NULL,
  ww_recent varchar(50) NULL,
  c_total_clouds varchar(200) NULL,
  vv_horizontal_visibility numeric(3, 1) NULL,
  td_temperature_dewpoint numeric(3, 1) NULL,
  loaded_ts timestamp DEFAULT now(),
  PRIMARY KEY (icao_code, local_datetime)
); 

drop table if exists kdz_10_staging.flights;
CREATE TABLE kdz_10_staging.flights (
  year int NOT NULL,
  quarter int NOT NULL,
  month int NOT NULL,
  flight_date date NOT NULL,
  dep_time time NULL,
  crs_dep_time time NOT NULL,
  air_time float NULL,
  dep_delay_minutes float NULL,
  cancelled bool NOT NULL,
  cancellation_code char(1) NULL,
  weather_delay float NULL,
  reporting_airline varchar(10) NOT NULL,
  tail_number varchar(10) NULL,
  flight_number varchar(15) NOT NULL,
  distance float NOT NULL,
  origin varchar(10) NOT NULL,
  dest varchar(10) NOT NULL,
  loaded_ts timestamp default(now()),
  CONSTRAINT flights_pkey PRIMARY KEY (flight_date, flight_number, origin, dest, crs_dep_time)
);

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
  select code, description, loaded_ts from kdz_10_src.flights_cancellation

insert into kdz_10_staging.flights_carriers
  select code, description, loaded_ts from kdz_10_src.flights_carriers
