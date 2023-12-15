create table if not exists kdz_10_etl.flights (
	year int,
	quarter int,
	month int,
	flight_date varchar(25),
	dep_time varchar(25),
	crs_dep_time varchar(25),
	air_time float,
	dep_delay_minutes float,
	cancelled float,
	cancellation_code char(1),
	weather_delay float,
	reporting_airline varchar(10),
	tail_number varchar(10),
	flight_number varchar(15),
	distance float,
	origin varchar(10),
	dest varchar(10),
	loaded_ts timestamp not null default (now()) 
);

create table if not exists kdz_10_etl.weather (
	local_datetime varchar(25),
	t_air_temperature numeric(3, 1),
	p0_sea_lvl numeric(4, 1),
	p_station_lvl numeric(4, 1),
	u_humidity int4,
	dd_wind_direction varchar(100),
	ff_wind_speed int4,
	ff10_max_gust_value int4,
	ww_present varchar(100),
	ww_recent varchar(50),
	c_total_clouds varchar(200),
	vv_horizontal_visibility numeric(3, 1),
	td_temperature_dewpoint numeric(3, 1),
	loaded_ts timestamp not null default (now()) 
);

