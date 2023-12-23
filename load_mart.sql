delete from mart.fact_departure where author='10';
insert into mart.fact_departure
SELECT 
  airport_origin_dk,
  airport_dest_dk,
  weather_type_dk,
  flight_dep_scheduled_ts,
  flight_dep_actual_ts,
  flight_number_reporting_airline,
  distance,
  tail_number,
  report_airline,
  dep_delay_minutes,
  cancelled,
  cancellation_code,
  t,
  max_gws,
  w_speed,
  air_time,
  '10',
  now()
FROM 
  kdz_10_dds.flights f left join kdz_10_dds.airport_weather w on (f.flight_dep_scheduled_ts >= w.date_start) and (f.flight_dep_scheduled_ts<w.date_end)
where 
    tail_number is not null;