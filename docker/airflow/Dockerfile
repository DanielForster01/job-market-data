FROM apache/airflow:3.3.2-python3.10

# ============================================================
# Image Airflow - projet job-market-data
# ============================================================
#
# Deux environnements Python sont volontairement séparés :
#
# 1. Environnement natif Airflow
#    Utilisé par :
#      - api-server
#      - scheduler
#      - dag-processor
#      - triggerer
#      - CLI airflow
#
# 2. Virtualenv du projet
#    Utilisé par :
#      - scripts Python job-market-data
#      - pandas
#      - pyarrow
#      - boto3
#      - psycopg2
#      - dbt
#
# IMPORTANT :
# le virtualenv projet n'est PAS ajouté au PATH global.
# ============================================================


# ============================================================
# Dépendances du projet
# ============================================================

COPY docker/airflow/requirements-project.txt \
    /tmp/requirements-project.txt


# ============================================================
# Virtualenv isolé du projet
# ============================================================

RUN python -m venv /opt/airflow/project_venv \
    && /opt/airflow/project_venv/bin/pip install \
        --no-cache-dir \
        --upgrade pip \
    && /opt/airflow/project_venv/bin/pip install \
        --no-cache-dir \
        -r /tmp/requirements-project.txt


# ============================================================
# Chemins explicites du projet
# ============================================================

ENV PROJECT_ROOT=/opt/airflow/project

ENV PROJECT_VENV=/opt/airflow/project_venv

ENV PROJECT_PYTHON=/opt/airflow/project_venv/bin/python

ENV PROJECT_DBT=/opt/airflow/project_venv/bin/dbt