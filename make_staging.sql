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

