# Formación: conceptos clave

Se va ampliando etapa por etapa.

## Etapa 1: exploración y modelo de datos

### Grain
Qué representa **una fila** de una tabla. Se define antes que las columnas.
Mezclar grains en una tabla es el error de modelado más común. Aquí: raw = una
llamada a la API; staging = ciudad × hora UTC; fact = ciudad × día local.

### Capas raw → staging → core
- **Raw**: copia fiel de lo que devolvió la fuente (JSON). Append-only. Permite
  reprocesar sin volver a llamar a la API y auditar.
- **Staging**: datos tipados, limpios, sin duplicados; mismo grain que la fuente.
- **Core**: modelo para consumo (esquema estrella).

### Esquema estrella (Kimball)
Tabla de **hechos** (medidas numéricas, grain fijo) rodeada de **dimensiones**
(contexto: ciudad, fecha). `date_key = YYYYMMDD` es legible y ordenable.
Llave **subrogada** (`location_key`) vs **natural** (`location_code`): la
subrogada aísla el warehouse de cambios en la llave del negocio.

### SCD tipo 1
Las correcciones en una dimensión sobrescriben el valor; no hay historial.
Tipo 2 crearía una fila nueva por cambio (con vigencia desde/hasta).

### UTC vs día local
Se almacena en UTC (`timestamptz`) y el día local se deriva en core. Lima es
UTC−5: el día local D va de D 05:00 UTC a D+1 05:00 UTC, por eso para cerrar un
día hay que extraer también el día UTC siguiente. Pregunta típica de
entrevista: «¿por qué no guardar la hora local?» (ambigüedad en cambios de
horario, no se pueden comparar ciudades de distinta zona).

### Arrays paralelos
La API devuelve `{"time": [...], "temperature_2m": [...], ...}` en vez de una
lista de objetos. Hay que "zipearlos" por posición para obtener filas
(pivotado). Todos deben tener la misma longitud; verificarlo es un check barato.

### Completitud vs. ausencia (`hours_observed`)
Un `0` de precipitación es un dato; un `NULL` es ausencia. Guardar cuántas
horas se observaron permite distinguir «no llovió» de «faltan datos».

### Latencia de disponibilidad y calidad del dato (`LAG_DAYS`)
Una API histórica puede entregar datos recientes *provisionales* (modelo o
pronóstico) y reemplazarlos después por un reanálisis definitivo. Procesar
`fecha_lógica − LAG_DAYS` evita cargar valores que van a cambiar. Compromiso:
frescura vs. estabilidad. Lección: medir el retraso por nulos habría dado una
conclusión falsa; la exploración sirve para descartar hipótesis.

### Coordenadas de la grilla
Los modelos de reanálisis trabajan en una grilla; la API ajusta tu coordenada a
la celda más cercana. Guardar lo devuelto (no solo lo pedido) es linaje: sabes
exactamente qué punto representa el dato.

### Errores de cliente vs. servidor
HTTP 4xx (p. ej. 400 por fechas inválidas) no se reintenta: fallará siempre.
Se reintenta con backoff exponencial 429, 5xx y timeouts.

### Idempotencia del DDL
`CREATE TABLE IF NOT EXISTS` y seeds con `ON CONFLICT` permiten ejecutar `make
ddl` varias veces con el mismo resultado. Ojo: `IF NOT EXISTS` **no** altera una
tabla existente; un cambio de columnas requiere `ALTER` o recrear.

## Etapa 2: extracción

### Reintentos: qué sí y qué no
Se reintenta lo **transitorio** (429, 5xx, timeouts, errores de conexión) con
**backoff exponencial + jitter** (`wait_exponential_jitter`): esperas 2, 4, 8 s…
con aleatoriedad para que muchos clientes no reintenten sincronizados
(*thundering herd*). Un 4xx (p. ej. 400) es un error permanente: se lanza
`ExtractionError` sin reintentar. `reraise=True` devuelve el error original, no
`RetryError`.

### Separar lógica de orquestación
`extract.py` no importa Airflow: recibe el `hook` por parámetro. Así se puede
probar con un mock y reutilizar fuera de Airflow. El DAG solo orquesta.

### Raw guarda solo respuestas exitosas
`payload` y `http_status` son NOT NULL: una respuesta de error no es dato. Los
fallos se ven en logs/estado de la tarea (y en `audit` en la etapa 7).

### Transacciones con psycopg2
`with conn:` hace commit al salir bien y rollback si hay excepción (no cierra la
conexión; por eso el `conn.close()` en `finally`). `%s::jsonb` + `json.dumps`
inserta el payload como JSONB.

### Extraer D y D+1 para un día local
El día local D de Lima es D 05:00 UTC → D+1 05:00 UTC. Cada extracción trae 48
horas; staging deduplica por `(location_code, observed_at)` y core usa solo las
24 h del día local.

### Bind mounts y reinicios de WSL
Si WSL/Docker Desktop reinicia, los contenedores pueden arrancar antes de que el
sistema de archivos esté disponible: los bind mounts (`dags/`, `src/`) quedan
vacíos y el puerto publicado puede no abrirse. Solución: `docker compose up -d
--force-recreate` (no toca los volúmenes con datos).

## Etapa 3: transformación y carga

### Pivotar arrays paralelos en SQL
`jsonb_array_elements_text(...) WITH ORDINALITY` devuelve cada elemento con su
posición; unir los arrays por esa posición equivale al `zip` de Python, pero
set-based y dentro de la base (sin traer el JSON a la app). JSON `null` llega
como SQL `NULL`. Un `LEFT JOIN` conserva la hora aunque falte una variable.

### Upsert e idempotencia
`INSERT ... ON CONFLICT (pk) DO UPDATE` hace que repetir la carga no duplique ni
falle. Idempotente = misma entrada, mismo estado final: es lo que permite
reintentos y backfills seguros en Airflow.

### Proteger contra reprocesos fuera de orden
El `WHERE staging.source_response_id <= EXCLUDED.source_response_id` impide que
reprocesar una respuesta antigua sobrescriba datos de una más nueva. Las
ventanas se solapan (D+1 aparece en la extracción de D y de D+1), así que el
orden de llegada no está garantizado.

### Agregar por día local, no por ventana
Una ventana de 48 h UTC contiene días locales completos y parciales en los
bordes. Por eso `build_fact_daily` recibe una fecha local concreta: solo se
escribe lo que se puede cerrar. `hours_observed` (24 = completo) detecta lo que
aun así falte.

### Rango en vez de `::date` para filtrar
`observed_at >= inicio AND observed_at < fin` (semiabierto) puede usar el índice
de la PK y evita errores en los bordes; envolver la columna en una función
(`observed_at AT TIME ZONE tz)::date`) lo impediría. `timestamp AT TIME ZONE tz`
interpreta la fecha local y devuelve el instante UTC (`timestamptz`).

### Semántica de los agregados con NULL
`avg`, `min`, `max`, `sum` ignoran NULL; `count(*) FILTER (WHERE ...)` cuenta
condicionalmente. Por eso `hours_observed` cuenta temperaturas no nulas y no
filas: una hora sin datos no debe parecer observada.

## Etapa 4: DAG diario en Airflow 3

### TaskFlow y dynamic task mapping
`@task` convierte funciones en tareas y el valor de retorno viaja por XCom.
`.expand(arg=lista)` crea **una instancia por elemento en tiempo de ejecución**
(aquí, una por ciudad): añadir una ciudad a `dim_location` no cambia el DAG.
`.partial(x=...)` fija los argumentos comunes. Mapear `load_staging` sobre la
salida de `extract_city` da reintentos independientes por ciudad: si falla
Cusco, solo se reintenta Cusco. Una tarea normal que recibe la salida mapeada
(`build_fact`) espera a que terminen todas las instancias.

### XCom solo con datos serializables y pequeños
Por eso `get_locations` devuelve dicts y no dataclasses, y entre tareas viajan
`response_id` (ids), no payloads. Los datos pesados van por la base de datos.

### Fechas en Airflow 3
- El timetable por defecto de un cron es `CronTriggerTimetable`: la ejecución
  de las 00:00 del día X tiene `data_interval_start == data_interval_end == X`
  (no el "intervalo anterior" de Airflow 2). `LAG_DAYS` se cuenta desde ahí.
- Un trigger manual **no tiene `logical_date` ni `data_interval_*`** en el
  contexto (de ahí el `KeyError`): se usa `ctx.get(...)` con respaldo en
  `dag_run.run_after`.
- Corolario: la lógica de negocio recibe la fecha como parámetro y el DAG la
  calcula; así es reproducible y testeable.

### Variables y configuración
`Variable.get` se llama **dentro de la tarea**, nunca a nivel de módulo (el
dag-processor re-parsea el archivo continuamente y cada llamada sería una
consulta). `LAG_DAYS` cambia desde la UI sin redeploy.

### Idempotencia a nivel de DAG
Tres runs sobre el mismo día dejaron: raw con 3 copias (append-only, historial),
staging deduplicado por PK y un único registro en el hecho. Reintentar o
re-ejecutar un run es seguro; esa es la propiedad que hace viable el backfill.

### Otras decisiones
- `catchup=False` + backfill explícito (`airflow backfill create`): el
  histórico se carga a propósito, no por accidente al despausar.
- `max_active_runs=1`: las ventanas D y D+1 se solapan en un día UTC; serializar
  runs evita carreras (el guard de `source_response_id` es la segunda defensa).
- `retries=2` de Airflow se suma a los reintentos de tenacity dentro de la
  llamada HTTP: tenacity cubre fallos de segundos; Airflow, caídas más largas.
- El DAG aplica DDL (`apply_schema`) pero **no** el seed de `dim_location`:
  esa tabla es fuente de verdad y el seed pisaría ediciones manuales.

## Etapa 5: calidad de datos

### Checks como consultas de aserción
Cada check es un `SELECT` que devuelve **las filas que violan la regla**: cero
filas = pasa. Ventajas: la muestra de filas malas sale gratis para depurar, y
agregar un check es agregar un archivo. Es el patrón de los *tests* de dbt
(`not_null`, `unique`, `accepted_values`, tests singulares), que verás en el
siguiente proyecto.

### Dimensiones de calidad que cubrimos
- **Completitud**: 24 h por día y ciudad; que exista la fila (04).
- **Validez**: valores en rangos físicos plausibles (03); arrays con la longitud
  esperada (01).
- **Consistencia / reconciliación**: el hecho coincide con lo recalculado desde
  staging (05). Detecta bugs de transformación o ediciones manuales.
- **Frescura (freshness)**: el último dato no es más viejo de lo tolerable (06).
- **Unicidad**: la garantiza la PK; un check aparte sería redundante. Regla:
  restringe en el esquema lo que puedas y comprueba con checks lo que no.

### Severidad: error vs warn
`error` bloquea (el run falla y no se confía en el dato); `warn` avisa sin
bloquear. Un check demasiado estricto genera *alert fatigue*; demasiado laxo
deja pasar datos malos. Nulos = warn porque la completitud ya se exige en 04.

### Trampas de SQL con NULL
`x <> 48` con `x` NULL devuelve NULL (no true), y el `WHERE` lo descarta: un
array ausente pasaría inadvertido. Se usa `IS DISTINCT FROM`, que trata NULL
como un valor comparable. Por la misma razón `NOT BETWEEN` no evalúa NULL (de
ahí que haya un check de nulos aparte).

### Probar los checks, no solo ejecutarlos
Un check que nunca falla puede estar roto. Cada uno se validó provocando su
violación dentro de una transacción con `ROLLBACK`. Es la idea de los tests de
mutación aplicada a datos.

### Dónde ejecutar los checks
Aquí, después de cargar core y dentro del DAG: si fallan, el run queda en rojo.
Alternativa más estricta (*write-audit-publish*): cargar a una tabla temporal,
auditar y solo entonces publicar. Cuesta más complejidad; el upsert idempotente
hace que re-ejecutar tras corregir sea seguro.

### Freshness depende del reloj
06 compara contra `now()`, no contra la fecha del run: mide la salud del
pipeline. En un backfill histórico no se activa porque mira el último día
cargado por ciudad, no el recién procesado.
