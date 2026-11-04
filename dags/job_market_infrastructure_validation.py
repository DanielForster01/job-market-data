from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

import pendulum

from airflow.sdk import dag, task


# ============================================================
# Configuration
# ============================================================

DAG_ID = "job_market_infrastructure_validation"

PROJECT_ROOT = os.environ.get(
    "PROJECT_ROOT",
    "/opt/airflow/project",
)

PROJECT_PYTHON = os.environ.get(
    "PROJECT_PYTHON",
    "/opt/airflow/project_venv/bin/python",
)

PROJECT_DBT = os.environ.get(
    "PROJECT_DBT",
    "/opt/airflow/project_venv/bin/dbt",
)


log = logging.getLogger(__name__)


# ============================================================
# Helpers
# ============================================================

def run_command(
    command: list[str],
    *,
    cwd: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """
    Exécute une commande externe et fait échouer la tâche
    si la commande retourne un code différent de zéro.

    stdout / stderr sont conservés dans les logs Airflow.
    """

    log.info(
        "Exécution commande : %s",
        " ".join(command),
    )

    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )

    if result.stdout:
        log.info(
            "stdout:\n%s",
            result.stdout.rstrip(),
        )

    if result.stderr:
        log.info(
            "stderr:\n%s",
            result.stderr.rstrip(),
        )

    if result.returncode != 0:
        raise RuntimeError(
            "Commande en échec "
            f"(code={result.returncode}) : "
            f"{' '.join(command)}"
        )

    return result


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id=DAG_ID,
    description=(
        "Smoke test de l'infrastructure Airflow "
        "du projet job-market-data."
    ),
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    max_active_runs=1,
    tags=[
        "job-market-data",
        "infrastructure",
        "validation",
    ],
)
def job_market_infrastructure_validation():

    # ========================================================
    # 1. Environnement du projet
    # ========================================================

    @task(
        task_id="check_project_runtime",
        retries=0,
    )
    def check_project_runtime() -> None:

        log.info(
            "PROJECT_ROOT=%s",
            PROJECT_ROOT,
        )

        log.info(
            "PROJECT_PYTHON=%s",
            PROJECT_PYTHON,
        )

        log.info(
            "PROJECT_DBT=%s",
            PROJECT_DBT,
        )

        project_root = Path(PROJECT_ROOT)
        project_python = Path(PROJECT_PYTHON)
        project_dbt = Path(PROJECT_DBT)

        if not project_root.exists():
            raise RuntimeError(
                f"PROJECT_ROOT introuvable : "
                f"{PROJECT_ROOT}"
            )

        if not project_python.exists():
            raise RuntimeError(
                f"Python projet introuvable : "
                f"{PROJECT_PYTHON}"
            )

        if not project_dbt.exists():
            raise RuntimeError(
                f"dbt projet introuvable : "
                f"{PROJECT_DBT}"
            )

        python_check = """
import sys

import pandas
import pyarrow
import boto3
import psycopg2
import requests

import src.storage.r2_paths

print("Python :", sys.version)
print("pandas :", pandas.__version__)
print("pyarrow :", pyarrow.__version__)
print("boto3 :", boto3.__version__)
print("psycopg2 : OK")
print("requests :", requests.__version__)
print("src.storage.r2_paths : OK")
"""

        run_command(
            [
                PROJECT_PYTHON,
                "-c",
                python_check,
            ],
            cwd=PROJECT_ROOT,
        )

        run_command(
            [
                PROJECT_DBT,
                "--version",
            ],
            cwd=PROJECT_ROOT,
        )

        log.info(
            "Environnement projet : VALIDE"
        )

    # ========================================================
    # 2. PostgreSQL métier
    # ========================================================

    @task(
        task_id="check_postgres_snapshot",
        retries=0,
    )
    def check_postgres_snapshot() -> None:

        postgres_check = """
import os
import psycopg2

required = [
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
]

missing = [
    name
    for name in required
    if not os.environ.get(name)
]

if missing:
    raise RuntimeError(
        "Variables PostgreSQL manquantes : "
        + ", ".join(missing)
    )


connection = psycopg2.connect(
    host=os.environ["POSTGRES_HOST"],
    port=int(os.environ["POSTGRES_PORT"]),
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    connect_timeout=10,
)

try:
    with connection.cursor() as cursor:

        cursor.execute(
            '''
            SELECT
                COUNT(*) AS nombre_offres,
                COUNT(DISTINCT id_offre) AS id_uniques,
                COUNT(DISTINCT batch_id) AS nombre_batchs,
                COUNT(
                    DISTINCT processing_run_id
                ) AS nombre_processing_runs
            FROM silver.france_travail_offres
            '''
        )

        (
            nombre_offres,
            id_uniques,
            nombre_batchs,
            nombre_processing_runs,
        ) = cursor.fetchone()

        print(
            "nombre_offres =",
            nombre_offres,
        )

        print(
            "id_uniques =",
            id_uniques,
        )

        print(
            "nombre_batchs =",
            nombre_batchs,
        )

        print(
            "nombre_processing_runs =",
            nombre_processing_runs,
        )

        if nombre_offres <= 0:
            raise RuntimeError(
                "Le snapshot PostgreSQL est vide."
            )

        if nombre_offres != id_uniques:
            raise RuntimeError(
                "Les id_offre ne sont pas uniques : "
                f"{nombre_offres} lignes "
                f"pour {id_uniques} IDs."
            )

        if nombre_batchs != 1:
            raise RuntimeError(
                "PostgreSQL doit exposer un seul "
                "batch courant. "
                f"Trouvé : {nombre_batchs}."
            )

        if nombre_processing_runs != 1:
            raise RuntimeError(
                "PostgreSQL doit exposer un seul "
                "processing_run_id courant. "
                f"Trouvé : {nombre_processing_runs}."
            )

finally:
    connection.close()


print(
    "Snapshot PostgreSQL : VALIDE"
)
"""

        run_command(
            [
                PROJECT_PYTHON,
                "-c",
                postgres_check,
            ],
            cwd=PROJECT_ROOT,
        )

    # ========================================================
    # 3. Cloudflare R2
    # ========================================================

    @task(
        task_id="check_r2_access",
        retries=0,
    )
    def check_r2_access() -> None:

        r2_check = """
import os

import boto3
from botocore.config import Config


required = [
    "R2_BUCKET_NAME",
    "R2_ENDPOINT_URL",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
    "R2_REGION",
]

missing = [
    name
    for name in required
    if not os.environ.get(name)
]

if missing:
    raise RuntimeError(
        "Variables R2 manquantes : "
        + ", ".join(missing)
    )


bucket_name = os.environ[
    "R2_BUCKET_NAME"
]


client = boto3.client(
    "s3",
    endpoint_url=os.environ[
        "R2_ENDPOINT_URL"
    ],
    aws_access_key_id=os.environ[
        "R2_ACCESS_KEY_ID"
    ],
    aws_secret_access_key=os.environ[
        "R2_SECRET_ACCESS_KEY"
    ],
    region_name=os.environ[
        "R2_REGION"
    ],
    config=Config(
        signature_version="s3v4"
    ),
)


client.head_bucket(
    Bucket=bucket_name
)


print(
    "Connexion Cloudflare R2 : OK"
)

print(
    "Bucket accessible :",
    bucket_name,
)

print(
    "Aucune écriture effectuée."
)
"""

        run_command(
            [
                PROJECT_PYTHON,
                "-c",
                r2_check,
            ],
            cwd=PROJECT_ROOT,
        )

    # ========================================================
    # Dépendances
    # ========================================================

    runtime = check_project_runtime()

    postgres = check_postgres_snapshot()
    r2 = check_r2_access()

    runtime >> [
        postgres,
        r2,
    ]


job_market_infrastructure_validation()