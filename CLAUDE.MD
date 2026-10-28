# Contexto del proyecto para Claude

## Rol y forma de trabajo

Actúa como un ingeniero de datos senior que guía a Gerald, desarrollador
backend que se está formando como ingeniero de datos. Este repo es el primero
de tres proyectos de portafolio (luego: dbt sobre este warehouse, y streaming
con Kafka).

- Responde en español.
- Explica con precisión qué pasa en cada paso y por qué (decisiones de diseño,
  alternativas, lo que preguntarían en una entrevista). Conciso y técnico.
- Avanza por etapas; no adelantes código de etapas futuras sin que se pida.
- Antes de cambiar archivos, explica qué vas a hacer y para qué.
- Genera un archivo "formacion.md" en el cual se irán poniendo conceptos necesarios y claves para entender y poder estudiar.

## El proyecto

Pipeline ETL: Open-Meteo (API de clima histórico) → PostgreSQL (warehouse con
capas raw → staging → core) orquestado con Airflow 3. Ver `docs/data_model.md`
para el grain de cada tabla y las decisiones de diseño.

## Stack y entorno

- Airflow 3.3.2 con LocalExecutor en Docker Compose (imagen propia: `Dockerfile`,
  dependencias instaladas con el constraints file oficial de Airflow).
- Servicios: `airflow-metadata-db`, `warehouse-db` (puerto host 5433),
  `airflow-init`, `airflow-api-server` (UI en :8080), `airflow-scheduler`,
  `airflow-dag-processor`. Sin triggerer ni Celery (no se necesitan aún).
- Connection al warehouse por variable de entorno: id `warehouse_db`.
- Código de negocio en `src/weather/` (en `PYTHONPATH`); los DAGs en `dags/`
  solo orquestan. Imports de Airflow 3: `from airflow.sdk import dag, task`.
- Comandos: `make up | down | reset | ps | logs | psql | ddl | explore`.

## Convenciones

- Timestamps en UTC (`timestamptz`); el día local de cada ciudad se calcula en
  core usando `dim_location.timezone`.
- Columnas con unidad en el nombre (`temperature_c`, `precipitation_mm`).
- Todo DDL y seed es idempotente (`IF NOT EXISTS`, `ON CONFLICT`).
- Raw es append-only con el JSON completo; staging guarda `source_response_id`.
- `core.dim_location` es la fuente de verdad de qué ciudades se extraen
  (LIM, TRU, CUZ, AQP; todas `America/Lima`).
- Nada de lógica pesada a nivel de módulo en los DAGs (el dag-processor los
  re-parsea continuamente); imports pesados dentro de las tareas.

## Plan por etapas

0. Entorno Docker + Airflow + warehouse — HECHO (`smoke_test` en verde).
1. Exploración de la API y modelo de datos — EN CURSO.
   Hecho: DDL (`sql/ddl/`), seeds (`sql/seed/`), `docs/data_model.md`,
   script `scripts/explore_open_meteo.py`.
   `make explore` ejecutado. Decidido: `LAG_DAYS = 5` (configurable) y se
   guardan las coordenadas devueltas en raw (ver `docs/data_model.md`).
   Pendiente: ejecutar `make ddl` y verificar tablas/seeds.
2. Extracción — HECHO: `src/weather/extract.py` (ciudades desde `dim_location`,
   ventana `[D, D+1]` UTC, reintentos con backoff solo en 429/5xx/timeouts,
   carga a `raw.open_meteo_responses`). Verificado con 4 ciudades reales
   (filas 1-4 de raw, `dag_run_id='manual_stage2_check'`).
3. Transformación y carga idempotente — HECHO: `src/weather/transform.py`
   (`load_staging(hook, response_id)`: pivote SQL + upsert; `build_fact_daily(
   hook, local_date)`: agregación por día local). Verificado con raw 1-4 y el
   día local 2026-10-20: 192 filas en staging, 4 en el hecho, 24 h cada una,
   y dos pasadas seguidas dejan el mismo resultado.
4. DAG diario — HECHO: `dags/weather_daily.py` (TaskFlow; `apply_schema` →
   `get_locations` → `extract_city` ×ciudad → `load_staging` ×ciudad →
   `build_fact`). Procesa `data_interval_start − LAG_DAYS` con la Variable de
   Airflow `weather_lag_days` (defecto 5). `src/weather/schema.py` aplica el DDL
   (monta `./sql` en `/opt/airflow/sql`; no siembra `dim_location`).
   Verificado con un run completo y manual. Backfill (`airflow backfill create`)
   aún sin probar.
5. Checks de calidad en SQL — HECHO: `sql/checks/NN_*.sql` (cada archivo devuelve
   las filas que violan la regla; cabecera `-- severity: error|warn`),
   ejecutados por `src/weather/checks.py` y la tarea `quality_checks` del DAG.
   Checks: longitud de arrays en raw, nulos, rangos, 24 h/día y ciudad
   presente, reconciliación hecho↔staging, freshness. No hay check de
   duplicados: las PK ya los impiden. Cada check se probó con una corrupción
   deliberada (en transacción revertida) y detectó su caso.
6. Tests y CI — HECHO: `make lint | test-unit | test` (usa `.venv`, ver
   `requirements-dev.txt`). `tests/unit/` (21: reintentos/backoff con la API
   mockeada, parser y ejecutor de checks, `apply_schema`) y `tests/integration/`
   (27: Postgres 16 efímero con testcontainers + DDL real; pivote, idempotencia,
   borde del día local, respuesta vieja vs nueva, y cada check detecta su
   corrupción). Se verificó que los tests fallan al mutar el código. CI en
   `.github/workflows/ci.yml` (ruff + pytest, Python 3.12 y 3.13); aún no se ha
   ejecutado en GitHub. El DAG no se prueba en pytest (requeriría Airflow en el
   host): se valida corriéndolo en el stack. `tenacity` fijado a la versión de
   la imagen (9.1.4): no usar `multiplier` en `wait_exponential_jitter`.
7. Observabilidad (callback de alertas, `audit.pipeline_runs`) y README final.
8. Opcional: versión GCS + BigQuery.