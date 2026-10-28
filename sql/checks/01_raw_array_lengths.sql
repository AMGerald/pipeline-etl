-- severity: error
-- description: respuestas raw cuyos arrays horarios no tienen todos la longitud esperada (24 h por día de la ventana)
SELECT r.response_id,
       r.location_code,
       (r.window_end - r.window_start + 1) * 24        AS expected_hours,
       jsonb_array_length(r.payload #> '{hourly,time}')                 AS n_time,
       jsonb_array_length(r.payload #> '{hourly,temperature_2m}')       AS n_temp,
       jsonb_array_length(r.payload #> '{hourly,relative_humidity_2m}') AS n_hum,
       jsonb_array_length(r.payload #> '{hourly,precipitation}')        AS n_prec,
       jsonb_array_length(r.payload #> '{hourly,wind_speed_10m}')       AS n_wind,
       jsonb_array_length(r.payload #> '{hourly,weather_code}')         AS n_code
FROM raw.open_meteo_responses r
WHERE r.window_start = %(local_date)s
  AND (   jsonb_array_length(r.payload #> '{hourly,time}')                 IS DISTINCT FROM (r.window_end - r.window_start + 1) * 24
       OR jsonb_array_length(r.payload #> '{hourly,temperature_2m}')       IS DISTINCT FROM jsonb_array_length(r.payload #> '{hourly,time}')
       OR jsonb_array_length(r.payload #> '{hourly,relative_humidity_2m}') IS DISTINCT FROM jsonb_array_length(r.payload #> '{hourly,time}')
       OR jsonb_array_length(r.payload #> '{hourly,precipitation}')        IS DISTINCT FROM jsonb_array_length(r.payload #> '{hourly,time}')
       OR jsonb_array_length(r.payload #> '{hourly,wind_speed_10m}')       IS DISTINCT FROM jsonb_array_length(r.payload #> '{hourly,time}')
       OR jsonb_array_length(r.payload #> '{hourly,weather_code}')         IS DISTINCT FROM jsonb_array_length(r.payload #> '{hourly,time}'))
