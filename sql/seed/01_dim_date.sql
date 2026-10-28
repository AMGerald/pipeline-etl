-- Genera el calendario 2020–2030. Idempotente: re-ejecutarlo no duplica nada.
INSERT INTO core.dim_date (
    date_key, full_date, year, quarter, month, month_name,
    day_of_month, day_of_week, day_name, is_weekend
)
SELECT
    to_char(d, 'YYYYMMDD')::int,
    d::date,
    extract(year    FROM d)::smallint,
    extract(quarter FROM d)::smallint,
    extract(month   FROM d)::smallint,
    (ARRAY['enero','febrero','marzo','abril','mayo','junio','julio',
           'agosto','septiembre','octubre','noviembre','diciembre'])[extract(month FROM d)],
    extract(day     FROM d)::smallint,
    extract(isodow  FROM d)::smallint,
    (ARRAY['lunes','martes','miércoles','jueves','viernes','sábado','domingo'])[extract(isodow FROM d)],
    extract(isodow  FROM d) IN (6, 7)
FROM generate_series('2020-01-01'::date, '2030-12-31'::date, interval '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;
