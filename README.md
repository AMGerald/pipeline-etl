# Weather Pipeline

Pipeline ETL que extrae datos de clima de Open-Meteo, los carga a un data
warehouse en PostgreSQL (capas raw → staging → core) y lo orquesta con Airflow 3.

## Requisitos
- Docker y Docker Compose v2.14+ (mínimo 4 GB de RAM asignados a Docker)
- `make` y `python3` (en Windows: WSL2)

## Arranque rápido
```bash
make up          # genera .env, construye la imagen y levanta los servicios
make ps          # espera a que todo esté "healthy"
```
- UI de Airflow: http://localhost:8080 (credenciales en `.env`)
- Warehouse: `localhost:5433` (o `make psql`)

Ejecuta el DAG `smoke_test` desde la UI para validar el entorno.

## Estructura
| Carpeta | Contenido |
|---|---|
| `dags/` | Definición de DAGs (solo orquestación) |
| `src/weather/` | Lógica: extract, transform, load, checks |
| `sql/init/` | DDL que se ejecuta al crear el warehouse |
| `tests/` | Tests con pytest |
