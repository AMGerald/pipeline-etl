# Atajos para el día a día. En Windows, úsalos desde WSL2.
.PHONY: init build up down reset logs ps psql ddl explore lint test test-unit

PY ?= .venv/bin/python

init:  ## Crea .env con secretos generados (solo si no existe)
	@test -f .env && echo ".env ya existe, no lo toco" || ( \
	  cp .env.example .env && \
	  sed -i.bak "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$$(id -u)/" .env && \
	  sed -i.bak "s/^FERNET_KEY=.*/FERNET_KEY=$$(python3 -c 'import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())')/" .env && \
	  sed -i.bak "s/^JWT_SECRET=.*/JWT_SECRET=$$(python3 -c 'import secrets;print(secrets.token_hex(32))')/" .env && \
	  rm -f .env.bak && echo ".env creado")

build:  ## Construye la imagen de Airflow con nuestras dependencias
	docker compose build

up: init  ## Levanta todo en segundo plano
	docker compose up -d --build

down:  ## Detiene los contenedores (conserva los datos)
	docker compose down

reset:  ## Detiene y BORRA volúmenes (metadatos + warehouse). Destructivo.
	docker compose down --volumes --remove-orphans

logs:  ## Sigue los logs de todos los servicios
	docker compose logs -f

ps:  ## Estado y salud de los servicios
	docker compose ps

psql:  ## Abre psql dentro del warehouse
	docker compose exec warehouse-db sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

ddl:  ## Crea/actualiza tablas y carga seeds en el warehouse (idempotente)
	cat sql/ddl/*.sql sql/seed/*.sql | docker compose exec -T warehouse-db \
	  sh -c 'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

explore:  ## Explora la API de Open-Meteo
	python3 scripts/explore_open_meteo.py

lint:  ## Ruff: estilo y errores (requiere .venv con requirements-dev.txt)
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .

test-unit:  ## Tests unitarios (sin Docker ni red)
	$(PY) -m pytest -m "not integration"

test:  ## Todos los tests; los de integración levantan un Postgres con testcontainers
	$(PY) -m pytest
