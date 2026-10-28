# Imagen de Airflow extendida con las dependencias del proyecto.
# Fijamos versión exacta: builds reproducibles.
FROM apache/airflow:3.3.2

COPY requirements.txt /tmp/requirements.txt

# El archivo de "constraints" fija versiones compatibles entre sí para
# TODAS las dependencias de Airflow. Instalar sin él puede romper Airflow.
RUN PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" \
 && pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
