-- Ciudades a monitorear. Esta tabla es la FUENTE DE VERDAD: el extractor
-- leerá aquí qué ciudades consultar (WHERE is_active).
-- ON CONFLICT ... DO UPDATE = SCD tipo 1: si corriges un dato, se sobrescribe.
INSERT INTO core.dim_location
    (location_code, city_name, region, country_code, latitude, longitude, timezone)
VALUES
    ('LIM', 'Lima',     'Lima',        'PE', -12.04640, -77.04280, 'America/Lima'),
    ('TRU', 'Trujillo', 'La Libertad', 'PE',  -8.11160, -79.02880, 'America/Lima'),
    ('CUZ', 'Cusco',    'Cusco',       'PE', -13.53200, -71.96750, 'America/Lima'),
    ('AQP', 'Arequipa', 'Arequipa',    'PE', -16.40900, -71.53750, 'America/Lima')
ON CONFLICT (location_code) DO UPDATE SET
    city_name    = EXCLUDED.city_name,
    region       = EXCLUDED.region,
    country_code = EXCLUDED.country_code,
    latitude     = EXCLUDED.latitude,
    longitude    = EXCLUDED.longitude,
    timezone     = EXCLUDED.timezone,
    updated_at   = now();
