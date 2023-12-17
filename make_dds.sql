create schema kdz_10_dds;

drop table if exists kdz_10_dds.airport_weather;
CREATE TABLE kdz_10_dds.airport_weather (
  airport_dk int NOT NULL, -- постоянный ключ аэропорта. нужно взять из таблицы аэропортов
  weather_type_dk char(6) NOT NULL, -- постоянный ключ типа погоды. заполняется по формуле
  cold smallint default(0),
  rain smallint default(0),
  snow smallint default(0),
  thunderstorm smallint default(0),
  drizzle smallint default(0),
  fog_mist smallint default(0),
  t int NULL,
  max_gws int NULL,
  w_speed int NULL,
  date_start timestamp NOT NULL,
  date_end timestamp NOT NULL default('3000-01-01'::timestamp),
  loaded_ts timestamp default(now()),
  PRIMARY KEY (airport_dk, date_start)
);

drop table if exists kdz_10_dds.flights;
CREATE TABLE kdz_10_dds.flights (
  year int NULL,
  quarter int NULL,
  month int NULL,
  flight_scheduled_date date NULL,
  flight_actual_date date NULL,
  flight_dep_scheduled_ts timestamp NOT NULL,
  flight_dep_actual_ts timestamp NULL,
  report_airline varchar(10) NOT NULL,
  tail_number varchar(10) NULL,
  flight_number_reporting_airline varchar(15) NOT NULL,
  airport_origin_dk int NULL, --постоянный ключ аэропорта. нужно взять из таблицы аэропортов
  origin_code varchar(5) null,
  airport_dest_dk int NULL,  --постоянный ключ аэропорта. нужно взять из таблицы аэропортов
  dest_code varchar(5) null,
  dep_delay_minutes float NULL,
  cancelled int NOT NULL,
  cancellation_code char(1) NULL,
  weather_delay float NULL,
  air_time float NULL,
  distance float NULL,
  loaded_ts timestamp default(now()),
  CONSTRAINT lights_pk PRIMARY KEY (flight_dep_scheduled_ts, flight_number_reporting_airline, origin_code, dest_code)
);

-- drop table if exists kdz_10_etl.flights_dds_loaded;
create table if not exists kdz_10_etl.flights_dds_loaded(
	loaded_ts timestamp not NULL
);

-- drop table if exists kdz_10_etl.airport_weather_dds_loaded;
create table if not exists kdz_10_etl.airport_weather_dds_loaded(
	loaded_ts timestamp not NULL
);

-- шаг 01. Какие правки самые свежие?
drop table if exists kdz_10_etl.flights_dds_ts;
create table if not exists kdz_10_etl.flights_dds_ts as 
select 
	min(loaded_ts) as ts1, 
	max(loaded_ts) as ts2 
from kdz_10_staging.flights
where loaded_ts > coalesce((select max(loaded_ts) from kdz_10_etl.flights_dds_loaded), '1970-01-01');

drop table if exists kdz_10_etl.airport_weather_dds_ts;
create table if not exists kdz_10_etl.airport_weather_dds_ts as 
select 
	min(loaded_ts) as ts1, 
	max(loaded_ts) as ts2 
from kdz_10_staging.weather
where loaded_ts > coalesce((select max(loaded_ts) from kdz_10_etl.airport_weather_dds_loaded), '1970-01-01');

-- Добавляем данные в таблицу airport_weather 
insert into kdz_10_dds.airport_weather(airport_dk, rain, snow, thunderstorm, fog_mist, drizzle, w_speed, max_gws, t, cold, date_start, date_end, weather_type_dk)
select
  *,
  CONCAT(cold,rain,snow,thunderstorm,drizzle,fog_mist) as weather_type_dk
from
  (select
    a.airport_dk as airport_dk,
    case when (LOWER(ww_present) like '%rain%') or (LOWER(ww_recent) like '%rain%') then 1 else 0 end as rain,
     case when (LOWER(ww_present) like '%snow%') or (LOWER(ww_recent) like '%snow%') then 1 else 0 end as snow,
    case when (LOWER(ww_present) like '%thunderstorm%') or (LOWER(ww_recent) like '%thunderstorm%') then 1 else 0 end as thunderstorm,
    case when (LOWER(ww_present) like '%fog%') or (LOWER(ww_recent) like '%fog%') or (LOWER(ww_present) like '%mist%') or (LOWER(ww_recent) like '%mist%') then 1 else 0 end as fog_mist,
    case when (LOWER(ww_present) like '%drizzle%') or (LOWER(ww_recent) like '%drizzle%') then 1 else 0 end as drizzle,
    ff_wind_speed as w_speed,
    ff10_max_gust_value as max_gws,
    t_air_temperature as t,
    cast(t_air_temperature<0 as int) as cold,
    w.local_datetime as date_start,
    coalesce(lead(w.local_datetime) over (order by w.local_datetime), '3000-01-01'::timestamp) as date_end
    from (kdz_10_staging.weather w join dds.airport a on w.icao=a.icao_code),kdz_10_etl.airport_weather_dds_ts
    where w.loaded_ts>=ts1 and w.loaded_ts<=ts2) ww;

insert into kdz_10_dds.flights
select
  year,
  quarter,
  month,
  flight_date as flight_scheduled_date,
  (flight_date + crs_dep_time + (dep_delay_minutes * interval '1 minute')) ::date as flight_actual_date,
  flight_date + crs_dep_time as flight_dep_scheduled_ts,
  flight_date + crs_dep_time + (dep_delay_minutes * interval '1 minute') as flight_dep_actual_ts, 
  reporting_airline as report_airline,
  tail_number,
  flight_number as flight_number_reporting_airline,
  a.airport_dk as airport_origin_dk,
  origin as origin_code,
  a1.airport_dk as airport_dest_dk,
  dest as dest_code,
  dep_delay_minutes,
  cast(cancelled as int) as cancelled,
  cancellation_code,
  weather_delay,
  air_time,
  distance
from (kdz_10_staging.flights f join dds.airport a on f.origin=a.iata_code join dds.airport a1 on f.dest=a1.iata_code),kdz_10_etl.flights_dds_ts
where f.loaded_ts>=ts1 and f.loaded_ts<=ts2;

insert into kdz_10_etl.flights_dds_loaded
select coalesce(ts2, (SELECT max(loaded_ts) from kdz_10_etl.flights_dds_loaded))
from kdz_10_etl.flights_dds_ts
where exists (select 1 from kdz_10_etl.flights_dds_ts);

insert into kdz_10_etl.airport_weather_dds_loaded
select coalesce(ts2, (SELECT max(loaded_ts) from kdz_10_etl.airport_weather_dds_loaded))
from kdz_10_etl.airport_weather_dds_ts
where exists (select 1 from kdz_10_etl.airport_weather_dds_ts);