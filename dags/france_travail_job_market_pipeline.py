import json
import os
import subprocess
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from airflow.sdk import dag, get_current_context, task


DAG_ID = "france_travail_job_market_pipeline"

PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    "/opt/airflow/project",
)

PROJECT_PYTHON = os.getenv(
    "PROJECT_PYTHON",
    "/opt/airflow/project_venv/bin/python",
)

PROJECT_DBT = os.getenv(
    "PROJECT_DBT",
    "/opt/airflow/project_venv/bin/dbt",
)

DBT_PROJECT_DIR = (
    Path(PROJECT_ROOT)
    / "dbt_project"
)

DBT_PROFILES_DIR = (
    DBT_PROJECT_DIR
)

RESULT_PREFIX = "__AIRFLOW_RESULT_JSON__="


# ============================================================
# HELPERS
# ============================================================


def ensure_equal(
    actual,
    expected,
    label: str,
) -> None:
    """
    Vérifie une égalité de contrat entre deux étapes.
    """

    if actual != expected:
        raise RuntimeError(
            f"{label} incohérent. "
            f"Attendu={expected!r}, "
            f"obtenu={actual!r}"
        )


def ensure_in(
    value,
    allowed: set,
    label: str,
) -> None:
    """
    Vérifie qu'une valeur appartient
    à un ensemble de valeurs autorisées.
    """

    if value not in allowed:
        raise RuntimeError(
            f"{label} inattendu : {value!r}. "
            f"Valeurs autorisées={sorted(allowed)!r}"
        )


def run_project_python(
    python_code: str,
    payload: dict,
) -> dict:
    """
    Exécute du code métier dans le virtualenv du projet.

    Airflow reste dans son environnement Python.
    Les dépendances métier sont isolées dans :

        /opt/airflow/project_venv

    Seules de petites métadonnées JSON remontent via XCom.
    """

    project_root = Path(
        PROJECT_ROOT
    )

    project_python = Path(
        PROJECT_PYTHON
    )

    if not project_root.is_dir():
        raise RuntimeError(
            "PROJECT_ROOT introuvable : "
            f"{project_root}"
        )

    if not project_python.is_file():
        raise RuntimeError(
            "PROJECT_PYTHON introuvable : "
            f"{project_python}"
        )

    command = [
        str(
            project_python
        ),
        "-c",
        textwrap.dedent(
            python_code
        ),
        json.dumps(
            payload,
            ensure_ascii=False,
        ),
    ]

    process = subprocess.Popen(
        command,
        cwd=str(
            project_root
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    if process.stdout is None:
        raise RuntimeError(
            "Impossible de capturer "
            "la sortie du processus projet."
        )

    airflow_result = None

    for raw_line in process.stdout:

        line = raw_line.rstrip()

        print(
            line,
            flush=True,
        )

        if line.startswith(
            RESULT_PREFIX
        ):
            airflow_result = json.loads(
                line[
                    len(
                        RESULT_PREFIX
                    ):
                ]
            )

    return_code = (
        process.wait()
    )

    if return_code != 0:
        raise RuntimeError(
            "Le processus Python projet "
            "a échoué avec le code "
            f"{return_code}."
        )

    if airflow_result is None:
        raise RuntimeError(
            "Le processus projet s'est terminé "
            "sans résultat Airflow."
        )

    return airflow_result


def run_project_command(
    command_args: list[str],
) -> str:
    """
    Exécute une commande CLI avec
    le Python du virtualenv projet.

    Utilisé notamment pour :

    - Silver -> PostgreSQL
    - validation PostgreSQL
    """

    project_root = Path(
        PROJECT_ROOT
    )

    project_python = Path(
        PROJECT_PYTHON
    )

    if not project_root.is_dir():
        raise RuntimeError(
            "PROJECT_ROOT introuvable : "
            f"{project_root}"
        )

    if not project_python.is_file():
        raise RuntimeError(
            "PROJECT_PYTHON introuvable : "
            f"{project_python}"
        )

    command = [
        str(
            project_python
        ),
        *command_args,
    ]

    print(
        "Commande projet : "
        + " ".join(
            command
        ),
        flush=True,
    )

    process = subprocess.Popen(
        command,
        cwd=str(
            project_root
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    if process.stdout is None:
        raise RuntimeError(
            "Impossible de capturer "
            "la sortie de la commande projet."
        )

    output_lines: list[str] = []

    for raw_line in process.stdout:

        line = raw_line.rstrip()

        output_lines.append(
            line
        )

        print(
            line,
            flush=True,
        )

    return_code = (
        process.wait()
    )

    output = "\n".join(
        output_lines
    )

    if return_code != 0:
        raise RuntimeError(
            "La commande projet a échoué "
            f"avec le code {return_code}."
        )

    return output

# ============================================================
# MONITORING CALLBACKS
# ============================================================


def _get_airflow_run_id(
    context: dict,
) -> str:
    """
    Récupère de manière défensive
    l'identifiant du DAG Run Airflow.
    """

    run_id = context.get(
        "run_id"
    )

    if not run_id:

        dag_run = context.get(
            "dag_run"
        )

        run_id = getattr(
            dag_run,
            "run_id",
            None,
        )

    if not run_id:

        ti = (
            context.get(
                "task_instance"
            )
            or context.get(
                "ti"
            )
        )

        run_id = getattr(
            ti,
            "run_id",
            None,
        )

    if not run_id:
        raise RuntimeError(
            "Impossible de récupérer "
            "le run_id Airflow pour "
            "le monitoring."
        )

    return str(
        run_id
    )


def monitoring_success_callback(
    context: dict,
) -> None:
    """
    Finalise le monitoring lorsqu'un DAG Run
    complet se termine avec succès.
    """

    try:

        run_id = _get_airflow_run_id(
            context
        )

        ti = (
            context.get(
                "task_instance"
            )
            or context.get(
                "ti"
            )
        )

        if ti is None:
            raise RuntimeError(
                "TaskInstance absente du "
                "contexte du callback SUCCESS."
            )
        raw_result = ti.xcom_pull(
            task_ids="validate_raw"
        )

        bronze_result = ti.xcom_pull(
            task_ids="validate_bronze"
        )

        silver_result = ti.xcom_pull(
            task_ids="validate_silver"
        )

        postgres_result = ti.xcom_pull(
            task_ids="validate_postgres"
        )

        dbt_result = ti.xcom_pull(
            task_ids="dbt_build"
        )

        if not isinstance(
            raw_result,
            dict,
        ):
            raise RuntimeError(
                "XCom validate_raw introuvable."
            )

        if not isinstance(
            bronze_result,
            dict,
        ):
            raise RuntimeError(
                "XCom validate_bronze introuvable."
            )
        if not isinstance(
            silver_result,
            dict,
        ):
            raise RuntimeError(
                "XCom validate_silver "
                "introuvable."
            )

        if not isinstance(
            postgres_result,
            dict,
        ):
            raise RuntimeError(
                "XCom validate_postgres "
                "introuvable."
            )

        if not isinstance(
            dbt_result,
            dict,
        ):
            raise RuntimeError(
                "XCom dbt_build introuvable."
            )

        if (
            dbt_result.get(
                "dbt_status"
            )
            != "BUILT"
        ):
            raise RuntimeError(
                "Statut dbt inattendu dans "
                "le callback SUCCESS."
            )

        child_code = """
        import json
        import sys

        from src.monitoring.pipeline_monitoring import (
            complete_pipeline_run,
            record_data_quality_metrics,
        )

        payload = json.loads(
            sys.argv[1]
        )

        result = complete_pipeline_run(
            dag_id=payload[
                "dag_id"
            ],
            dag_run_id=payload[
                "dag_run_id"
            ],
            batch_id=payload[
                "batch_id"
            ],
            processing_run_id=payload[
                "processing_run_id"
            ],
            raw_object_key=payload[
                "raw_object_key"
            ],
            bronze_object_key=payload[
                "bronze_object_key"
            ],
            silver_object_key=payload[
                "silver_object_key"
            ],
            raw_row_count=payload[
                "raw_row_count"
            ],
            bronze_row_count=payload[
                "bronze_row_count"
            ],
            silver_row_count=payload[
                "silver_row_count"
            ],
            postgres_row_count=payload[
                "postgres_row_count"
            ],
            dbt_status="SUCCESS",
        )

        quality_result = (
            record_data_quality_metrics(
                dag_id=payload[
                    "dag_id"
                ],
                dag_run_id=payload[
                    "dag_run_id"
                ],
                metrics=payload[
                    "quality_metrics"
                ],
            )
        )

        print(
            "__AIRFLOW_RESULT_JSON__="
            + json.dumps(
                result,
                default=str,
            )
        )
        """

        run_project_python(
            child_code,
            {
                "dag_id": DAG_ID,
                "dag_run_id": run_id,

                "batch_id": (
                    silver_result[
                        "batch_id"
                    ]
                ),

                "processing_run_id": (
                    silver_result[
                        "processing_run_id"
                    ]
                ),

                "raw_object_key": (
                    silver_result[
                        "raw_object_key"
                    ]
                ),

                "bronze_object_key": (
                    silver_result[
                        "bronze_object_key"
                    ]
                ),

                "silver_object_key": (
                    silver_result[
                        "silver_object_key"
                    ]
                ),

                "raw_row_count": (
                    silver_result[
                        "nombre_lignes_raw"
                    ]
                ),

                "bronze_row_count": (
                    silver_result[
                        "nombre_lignes_bronze"
                    ]
                ),

                "silver_row_count": (
                    silver_result[
                        "nombre_lignes_silver"
                    ]
                ),

                "postgres_row_count": (
                    postgres_result[
                        "nombre_lignes"
                    ]
                ),

                "quality_metrics": [
                    {
                        "layer": "RAW",
                        "metric_name": "row_count",
                        "metric_value": (
                            raw_result[
                                "nombre_offres"
                            ]
                        ),
                        "metric_unit": "count",
                        "source_task_id": "validate_raw",
                    },

                    {
                        "layer": "BRONZE",
                        "metric_name": "row_count",
                        "metric_value": (
                            bronze_result[
                                "nombre_lignes_bronze"
                            ]
                        ),
                        "metric_unit": "count",
                        "source_task_id": "validate_bronze",
                    },
                    {
                        "layer": "BRONZE",
                        "metric_name": "unique_id_count",
                        "metric_value": (
                            bronze_result[
                                "nombre_ids_uniques"
                            ]
                        ),
                        "metric_unit": "count",
                        "source_task_id": "validate_bronze",
                    },
                    {
                        "layer": "BRONZE",
                        "metric_name": "column_count",
                        "metric_value": (
                            bronze_result[
                                "nombre_colonnes_bronze"
                            ]
                        ),
                        "metric_unit": "count",
                        "source_task_id": "validate_bronze",
                    },
                    {
                        "layer": "BRONZE",
                        "metric_name": "schema_drift_field_count",
                        "metric_value": (
                            bronze_result[
                                "schema_drift_field_count"
                            ]
                        ),
                        "metric_unit": "count",
                        "source_task_id": "validate_bronze",
                    },
                    {
                        "layer": "BRONZE",
                        "metric_name": "schema_drift_detected",
                        "metric_value": (
                            bronze_result[
                                "schema_drift_detected"
                            ]
                        ),
                        "metric_unit": "boolean",
                        "source_task_id": "validate_bronze",
                    },

                    {
                        "layer": "SILVER",
                        "metric_name": "row_count",
                        "metric_value": (
                            silver_result[
                                "nombre_lignes_silver"
                            ]
                        ),
                        "metric_unit": "count",
                        "source_task_id": "validate_silver",
                    },
                    {
                        "layer": "SILVER",
                        "metric_name": "unique_id_count",
                        "metric_value": (
                            silver_result[
                                "nombre_ids_uniques"
                            ]
                        ),
                        "metric_unit": "count",
                        "source_task_id": "validate_silver",
                    },
                    {
                        "layer": "SILVER",
                        "metric_name": "column_count",
                        "metric_value": (
                            silver_result[
                                "nombre_colonnes_silver"
                            ]
                        ),
                        "metric_unit": "count",
                        "source_task_id": "validate_silver",
                    },
                    {
                        "layer": "SILVER",
                        "metric_name": "localisation_pct",
                        "metric_value": (
                            silver_result[
                                "taux_localisation_pct"
                            ]
                        ),
                        "metric_unit": "percent",
                        "source_task_id": "validate_silver",
                    },
                    {
                        "layer": "SILVER",
                        "metric_name": "coordinates_pct",
                        "metric_value": (
                            silver_result[
                                "taux_coordonnees_pct"
                            ]
                        ),
                        "metric_unit": "percent",
                        "source_task_id": "validate_silver",
                    },
                    {
                        "layer": "SILVER",
                        "metric_name": "salary_available_pct",
                        "metric_value": (
                            silver_result[
                                "taux_salaires_pct"
                            ]
                        ),
                        "metric_unit": "percent",
                        "source_task_id": "validate_silver",
                    },
                    {
                        "layer": "SILVER",
                        "metric_name": "company_information_pct",
                        "metric_value": (
                            silver_result[
                                "taux_information_entreprise_pct"
                            ]
                        ),
                        "metric_unit": "percent",
                        "source_task_id": "validate_silver",
                    },

                    {
                        "layer": "POSTGRES",
                        "metric_name": "row_count",
                        "metric_value": (
                            postgres_result[
                                "nombre_lignes"
                            ]
                        ),
                        "metric_unit": "count",
                        "source_task_id": "validate_postgres",
                    },

                    {
                        "layer": "DBT",
                        "metric_name": "build_success",
                        "metric_value": True,
                        "metric_unit": "boolean",
                        "source_task_id": "dbt_build",
                    },
                ],
            },
        )

        print(
            "Monitoring pipeline : SUCCESS",
            flush=True,
        )

    except Exception as exc:

        # Une panne du monitoring ne doit pas
        # transformer un pipeline métier réussi
        # en échec.
        print(
            "WARNING - Impossible de finaliser "
            "le monitoring SUCCESS : "
            f"{exc}",
            flush=True,
        )


def monitoring_failure_callback(
    context: dict,
) -> None:
    """
    Finalise le monitoring lorsqu'un DAG Run
    se termine en échec.
    """

    try:

        run_id = _get_airflow_run_id(
            context
        )

        ti = (
            context.get(
                "task_instance"
            )
            or context.get(
                "ti"
            )
        )

        failed_task_id = getattr(
            ti,
            "task_id",
            None,
        )

        exception = context.get(
            "exception"
        )

        error_message = (
            str(
                exception
            )
            if exception
            else (
                "DAG Airflow en échec"
                + (
                    f" sur la tâche "
                    f"{failed_task_id}."
                    if failed_task_id
                    else "."
                )
            )
        )

        logical_date = context.get(
            "logical_date"
        )

        if logical_date is None:

            dag_run = context.get(
                "dag_run"
            )

            logical_date = getattr(
                dag_run,
                "logical_date",
                None,
            )

            if logical_date is None:

                logical_date = getattr(
                    dag_run,
                    "run_after",
                    None,
                )

        child_code = """
        import json
        import sys

        from src.monitoring.pipeline_monitoring import (
            fail_pipeline_run,
            start_pipeline_run,
        )

        payload = json.loads(
            sys.argv[1]
        )

        try:

            result = fail_pipeline_run(
                dag_id=payload[
                    "dag_id"
                ],
                dag_run_id=payload[
                    "dag_run_id"
                ],
                failed_task_id=payload.get(
                    "failed_task_id"
                ),
                error_message=payload.get(
                    "error_message"
                ),
            )

        except RuntimeError:

            # Cas exceptionnel :
            # échec avant que start_monitoring
            # ait pu créer la ligne.
            start_pipeline_run(
                dag_id=payload[
                    "dag_id"
                ],
                dag_run_id=payload[
                    "dag_run_id"
                ],
                logical_date=payload.get(
                    "logical_date"
                ),
            )

            result = fail_pipeline_run(
                dag_id=payload[
                    "dag_id"
                ],
                dag_run_id=payload[
                    "dag_run_id"
                ],
                failed_task_id=payload.get(
                    "failed_task_id"
                ),
                error_message=payload.get(
                    "error_message"
                ),
            )

        print(
            "__AIRFLOW_RESULT_JSON__="
            + json.dumps(
                result,
                default=str,
            )
        )
        """

        run_project_python(
            child_code,
            {
                "dag_id": DAG_ID,
                "dag_run_id": run_id,
                "failed_task_id": (
                    failed_task_id
                ),
                "error_message": (
                    error_message
                ),
                "logical_date": (
                    logical_date.isoformat()
                    if logical_date
                    else None
                ),
            },
        )

        print(
            "Monitoring pipeline : FAILED",
            flush=True,
        )

    except Exception as exc:

        # On préserve toujours l'erreur
        # originale du pipeline.
        print(
            "WARNING - Impossible de finaliser "
            "le monitoring FAILED : "
            f"{exc}",
            flush=True,
        )


# ============================================================
# DAG
# ============================================================


@dag(
    dag_id=DAG_ID,
    description=(
        "Pipeline France Travail : "
        "Raw -> Bronze -> Silver -> PostgreSQL."
    ),
    schedule=None,
    start_date=datetime(
        2026,
        11,
        1,
        tzinfo=timezone.utc,
    ),
    catchup=False,
    max_active_runs=1,
    on_success_callback=(
        monitoring_success_callback
    ),
    on_failure_callback=(
        monitoring_failure_callback
    ),
    tags=[
        "france-travail",
        "data-engineering",
        "r2",
        "postgresql",
        "dbt",
    ],
)
def france_travail_job_market_pipeline():

    # ========================================================
    # 0. MONITORING START
    # ========================================================

    @task(
        task_id="start_monitoring",
        retries=0,
    )
    def start_monitoring() -> dict:

        context = (
            get_current_context()
        )

        run_id = _get_airflow_run_id(
            context
        )

        logical_date = context.get(
            "logical_date"
        )

        if logical_date is None:

            dag_run = context.get(
                "dag_run"
            )

            logical_date = getattr(
                dag_run,
                "run_after",
                None,
            )

        child_code = """
        import json
        import sys

        from src.monitoring.pipeline_monitoring import (
            start_pipeline_run,
        )

        payload = json.loads(
            sys.argv[1]
        )

        result = start_pipeline_run(
            dag_id=payload[
                "dag_id"
            ],
            dag_run_id=payload[
                "dag_run_id"
            ],
            logical_date=payload.get(
                "logical_date"
            ),
        )

        print(
            "__AIRFLOW_RESULT_JSON__="
            + json.dumps(
                result,
                default=str,
            )
        )
        """

        result = run_project_python(
            child_code,
            {
                "dag_id": DAG_ID,
                "dag_run_id": run_id,
                "logical_date": (
                    logical_date.isoformat()
                    if logical_date
                    else None
                ),
            },
        )

        print(
            "Monitoring pipeline démarré.",
            flush=True,
        )

        return result

    # ========================================================
    # 1. PREFLIGHT
    # ========================================================

    @task(
        task_id="preflight",
        retries=0,
    )
    def preflight() -> dict:

        project_root = Path(
            PROJECT_ROOT
        )

        project_python = Path(
            PROJECT_PYTHON
        )

        required_files = [
            (
                project_root
                / "src/ingestion/"
                "france_travail_ingestion.py"
            ),
            (
                project_root
                / "src/quality/"
                "raw_r2_quality_check.py"
            ),
            (
                project_root
                / "src/transformation/"
                "raw_to_bronze.py"
            ),
            (
                project_root
                / "src/quality/"
                "bronze_r2_quality_check.py"
            ),
            (
                project_root
                / "src/transformation/"
                "bronze_to_silver.py"
            ),
            (
                project_root
                / "src/quality/"
                "silver_r2_quality_check.py"
            ),
            (
                project_root
                / "src/loading/"
                "load_silver_to_postgres.py"
            ),
            (
                project_root
                / "src/quality/"
                "silver_postgres_validation_report.py"
            ),
            (
                project_root
                / "src/storage/"
                "r2_storage.py"
            ),
        ]

        if not project_root.is_dir():
            raise RuntimeError(
                "PROJECT_ROOT introuvable : "
                f"{project_root}"
            )

        if not project_python.is_file():
            raise RuntimeError(
                "PROJECT_PYTHON introuvable : "
                f"{project_python}"
            )

        missing_files = [
            str(
                path
            )
            for path in required_files
            if not path.is_file()
        ]

        if missing_files:
            raise RuntimeError(
                "Fichiers projet manquants : "
                + ", ".join(
                    missing_files
                )
            )

        runtime_code = (
            "from "
            "src.ingestion.france_travail_ingestion "
            "import run_ingestion; "

            "from "
            "src.quality.raw_r2_quality_check "
            "import validate_raw_object; "

            "from "
            "src.transformation.raw_to_bronze "
            "import run_bronze_transformation; "

            "from "
            "src.quality.bronze_r2_quality_check "
            "import validate_bronze_object; "

            "from "
            "src.transformation.bronze_to_silver "
            "import run_silver_transformation; "

            "from "
            "src.quality.silver_r2_quality_check "
            "import validate_silver_object; "

            "import "
            "src.loading.load_silver_to_postgres; "

            "import "
            "src.quality."
            "silver_postgres_validation_report; "

            "from "
            "src.storage.r2_storage "
            "import R2Storage; "

            "print('PROJECT_RUNTIME_OK')"
        )

        runtime_check = subprocess.run(
            [
                str(
                    project_python
                ),
                "-c",
                runtime_code,
            ],
            cwd=str(
                project_root
            ),
            capture_output=True,
            text=True,
        )

        if (
            runtime_check.returncode
            != 0
        ):

            print(
                runtime_check.stdout
            )

            print(
                runtime_check.stderr
            )

            raise RuntimeError(
                "Le runtime Python projet "
                "n'est pas opérationnel."
            )

        print(
            "Preflight validé."
        )

        print(
            f"PROJECT_ROOT="
            f"{project_root}"
        )

        print(
            f"PROJECT_PYTHON="
            f"{project_python}"
        )

        return {
            "status": "VALID",
            "project_root": str(
                project_root
            ),
            "project_python": str(
                project_python
            ),
        }

    # ========================================================
    # 2. CONTEXTE ACQUISITION
    # ========================================================

    @task(
        task_id="create_acquisition_context",
        retries=0,
    )
    def create_acquisition_context() -> dict:
        """
        Crée un batch_id stable
        pour un même DAG Run.

        Nouveau DAG Run :
            nouveau batch_id

        Retry :
            même batch_id
        """

        context = (
            get_current_context()
        )

        run_id = (
            context.get(
                "run_id"
            )
        )

        if not run_id:

            ti = (
                context.get(
                    "task_instance"
                )
                or context.get(
                    "ti"
                )
            )

            run_id = getattr(
                ti,
                "run_id",
                None,
            )

        if not run_id:
            raise RuntimeError(
                "Impossible de récupérer "
                "le run_id Airflow."
            )

        logical_date = (
            context.get(
                "logical_date"
            )
        )

        if logical_date is None:

            dag_run = (
                context.get(
                    "dag_run"
                )
            )

            logical_date = getattr(
                dag_run,
                "run_after",
                None,
            )

        if logical_date is None:
            raise RuntimeError(
                "Impossible de déterminer "
                "l'horodatage stable "
                "du DAG Run."
            )

        if (
            logical_date.tzinfo
            is None
        ):
            raise RuntimeError(
                "L'horodatage Airflow "
                "doit être timezone-aware."
            )

        ingestion_datetime = (
            logical_date.astimezone(
                timezone.utc
            )
        )

        batch_id = uuid5(
            NAMESPACE_URL,
            (
                f"{DAG_ID}:"
                f"{run_id}:"
                "france_travail_acquisition"
            ),
        )

        result = {
            "batch_id": str(
                batch_id
            ),
            "ingestion_datetime_utc": (
                ingestion_datetime
                .isoformat()
            ),
        }

        print(
            "Contexte d'acquisition créé :"
        )

        print(
            json.dumps(
                result,
                indent=2,
            )
        )

        return result

    # ========================================================
    # 3. INGESTION RAW
    # ========================================================

    @task(
        task_id="ingest_raw",
        retries=2,
        retry_delay=timedelta(
            minutes=2
        ),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(
            minutes=10
        ),
    )
    def ingest_raw(
        acquisition_context: dict,
    ) -> dict:

        child_code = """
        import json
        import sys

        from datetime import datetime

        from src.ingestion.france_travail_ingestion import (
            run_ingestion,
        )

        payload = json.loads(
            sys.argv[1]
        )

        result = run_ingestion(
            batch_id=payload[
                "batch_id"
            ],
            ingestion_datetime=(
                datetime.fromisoformat(
                    payload[
                        "ingestion_datetime_utc"
                    ]
                )
            ),
        )

        compact = {
            "batch_id": result[
                "batch_id"
            ],
            "ingestion_datetime_utc": result[
                "ingestion_datetime_utc"
            ],
            "object_key": result[
                "object_key"
            ],
            "nombre_offres": result[
                "nombre_offres"
            ],
            "size_bytes": result[
                "size_bytes"
            ],
            "sha256": result[
                "sha256"
            ],
            "storage_status": result[
                "storage_status"
            ],
        }

        print(
            "__AIRFLOW_RESULT_JSON__="
            + json.dumps(
                compact,
                ensure_ascii=False,
            )
        )
        """

        result = run_project_python(
            child_code,
            acquisition_context,
        )

        ensure_equal(
            result[
                "batch_id"
            ],
            acquisition_context[
                "batch_id"
            ],
            "batch_id après ingestion",
        )

        ensure_equal(
            result[
                "ingestion_datetime_utc"
            ],
            acquisition_context[
                "ingestion_datetime_utc"
            ],
            (
                "ingestion_datetime "
                "après ingestion"
            ),
        )

        ensure_in(
            result[
                "storage_status"
            ],
            {
                "CREATED",
                "REUSED",
            },
            "storage_status Raw",
        )

        print(
            "Ingestion Raw terminée : "
            f"{result['storage_status']}"
        )

        print(
            "Offres : "
            f"{result['nombre_offres']}"
        )

        print(
            "Objet Raw : "
            f"{result['object_key']}"
        )

        return result

    # ========================================================
    # 4. QUALITY GATE RAW
    # ========================================================

    @task(
        task_id="validate_raw",
        retries=0,
    )
    def validate_raw(
        ingestion_result: dict,
    ) -> dict:

        child_code = """
        import json
        import sys

        from src.quality.raw_r2_quality_check import (
            validate_raw_object,
        )

        payload = json.loads(
            sys.argv[1]
        )

        result = validate_raw_object(
            object_key=payload[
                "object_key"
            ]
        )

        compact = {
            "statut": result[
                "statut"
            ],
            "batch_id": result[
                "batch_id"
            ],
            "nombre_offres": result[
                "nombre_offres"
            ],
            "nombre_ids_uniques": result[
                "nombre_ids_uniques"
            ],
            "nombre_doublons": result[
                "nombre_doublons"
            ],
            "size_bytes": result[
                "size_bytes"
            ],
            "sha256": result[
                "sha256"
            ],
        }

        print(
            "__AIRFLOW_RESULT_JSON__="
            + json.dumps(
                compact,
                ensure_ascii=False,
            )
        )
        """

        quality = run_project_python(
            child_code,
            {
                "object_key": (
                    ingestion_result[
                        "object_key"
                    ]
                ),
            },
        )

        ensure_equal(
            quality[
                "statut"
            ],
            "VALIDE",
            "statut Quality Gate Raw",
        )

        ensure_equal(
            quality[
                "batch_id"
            ],
            ingestion_result[
                "batch_id"
            ],
            "batch_id Raw",
        )

        ensure_equal(
            quality[
                "nombre_offres"
            ],
            ingestion_result[
                "nombre_offres"
            ],
            "volume Raw",
        )

        ensure_equal(
            quality[
                "sha256"
            ],
            ingestion_result[
                "sha256"
            ],
            "SHA-256 Raw",
        )

        ensure_equal(
            quality[
                "nombre_doublons"
            ],
            0,
            "doublons Raw",
        )

        ensure_equal(
            quality[
                "nombre_ids_uniques"
            ],
            quality[
                "nombre_offres"
            ],
            "IDs uniques Raw",
        )

        result = {
            "statut": "VALIDE",
            "batch_id": (
                ingestion_result[
                    "batch_id"
                ]
            ),
            "ingestion_datetime_utc": (
                ingestion_result[
                    "ingestion_datetime_utc"
                ]
            ),
            "raw_object_key": (
                ingestion_result[
                    "object_key"
                ]
            ),
            "nombre_offres": (
                quality[
                    "nombre_offres"
                ]
            ),
            "sha256": (
                quality[
                    "sha256"
                ]
            ),
            "storage_status": (
                ingestion_result[
                    "storage_status"
                ]
            ),
        }

        print(
            "Raw Quality Gate : VALIDE"
        )

        return result

    # ========================================================
    # 5. CONTEXTE PROCESSING
    # ========================================================

    @task(
        task_id="create_processing_context",
        retries=0,
    )
    def create_processing_context(
        raw_validation_result: dict,
    ) -> dict:

        ensure_equal(
            raw_validation_result[
                "statut"
            ],
            "VALIDE",
            (
                "statut Raw "
                "avant processing"
            ),
        )

        context = (
            get_current_context()
        )

        run_id = (
            context.get(
                "run_id"
            )
        )

        if not run_id:

            ti = (
                context.get(
                    "task_instance"
                )
                or context.get(
                    "ti"
                )
            )

            run_id = getattr(
                ti,
                "run_id",
                None,
            )

        if not run_id:
            raise RuntimeError(
                "Impossible de récupérer "
                "le run_id Airflow."
            )

        batch_id = (
            raw_validation_result[
                "batch_id"
            ]
        )

        processing_run_id = uuid5(
            NAMESPACE_URL,
            (
                f"{DAG_ID}:"
                f"{run_id}:"
                f"{batch_id}:"
                "processing_v2"
            ),
        )

        result = {
            "batch_id": batch_id,
            "processing_run_id": str(
                processing_run_id
            ),
            "raw_object_key": (
                raw_validation_result[
                    "raw_object_key"
                ]
            ),
            "raw_sha256": (
                raw_validation_result[
                    "sha256"
                ]
            ),
            "nombre_offres": (
                raw_validation_result[
                    "nombre_offres"
                ]
            ),
        }

        print(
            "Contexte de processing créé :"
        )

        print(
            json.dumps(
                result,
                indent=2,
            )
        )

        return result

    # ========================================================
    # 6. RAW -> BRONZE
    # ========================================================

    @task(
        task_id="raw_to_bronze",
        retries=2,
        retry_delay=timedelta(
            minutes=2
        ),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(
            minutes=10
        ),
    )
    def raw_to_bronze(
        processing_context: dict,
    ) -> dict:

        child_code = """
        import json
        import sys

        from src.transformation.raw_to_bronze import (
            run_bronze_transformation,
        )

        payload = json.loads(
            sys.argv[1]
        )

        result = run_bronze_transformation(
            raw_object_key=payload[
                "raw_object_key"
            ],
            processing_run_id=payload[
                "processing_run_id"
            ],
        )

        drift_count = result.get(
            "schema_drift_field_count"
        )

        if drift_count is None:
            drift_count = len(
                result.get(
                    "schema_drift"
                )
                or {}
            )

        compact = {
            "batch_id": result[
                "batch_id"
            ],
            "processing_run_id": result[
                "processing_run_id"
            ],
            "source": result[
                "source"
            ],
            "ingestion_date": result[
                "ingestion_date"
            ],
            "schema_version": result[
                "schema_version"
            ],
            "path_version": result[
                "path_version"
            ],
            "raw_object_key": result[
                "raw_object_key"
            ],
            "raw_sha256": result[
                "raw_sha256"
            ],
            "bronze_object_key": result[
                "bronze_object_key"
            ],
            "nombre_lignes_raw": result[
                "nombre_lignes_raw"
            ],
            "nombre_lignes_bronze": result[
                "nombre_lignes_bronze"
            ],
            "nombre_colonnes_bronze": result[
                "nombre_colonnes_bronze"
            ],
            "schema_drift_detected": result[
                "schema_drift_detected"
            ],
            "schema_drift_field_count": (
                drift_count
            ),
            "size_bytes": result[
                "size_bytes"
            ],
            "sha256": result[
                "sha256"
            ],
            "storage_status": result[
                "storage_status"
            ],
        }

        print(
            "__AIRFLOW_RESULT_JSON__="
            + json.dumps(
                compact,
                ensure_ascii=False,
            )
        )
        """

        result = run_project_python(
            child_code,
            processing_context,
        )

        ensure_equal(
            result[
                "batch_id"
            ],
            processing_context[
                "batch_id"
            ],
            "batch_id Bronze",
        )

        ensure_equal(
            result[
                "processing_run_id"
            ],
            processing_context[
                "processing_run_id"
            ],
            "processing_run_id Bronze",
        )

        ensure_equal(
            result[
                "raw_object_key"
            ],
            processing_context[
                "raw_object_key"
            ],
            "raw_object_key Bronze",
        )

        ensure_equal(
            result[
                "raw_sha256"
            ],
            processing_context[
                "raw_sha256"
            ],
            "SHA-256 Raw dans Bronze",
        )

        ensure_equal(
            result[
                "nombre_lignes_raw"
            ],
            processing_context[
                "nombre_offres"
            ],
            "volume Raw dans Bronze",
        )

        ensure_equal(
            result[
                "nombre_lignes_bronze"
            ],
            result[
                "nombre_lignes_raw"
            ],
            "non-perte Raw -> Bronze",
        )

        ensure_equal(
            result[
                "schema_version"
            ],
            "2.0.0",
            "schema Bronze",
        )

        ensure_equal(
            result[
                "path_version"
            ],
            "processing_v2",
            "path Bronze",
        )

        ensure_in(
            result[
                "storage_status"
            ],
            {
                "CREATED",
                "REUSED",
            },
            "storage_status Bronze",
        )

        print(
            "Transformation Raw -> Bronze : "
            f"{result['storage_status']}"
        )

        return result

    # ========================================================
    # 7. QUALITY GATE BRONZE
    # ========================================================

    @task(
        task_id="validate_bronze",
        retries=0,
    )
    def validate_bronze(
        bronze_result: dict,
    ) -> dict:

        child_code = """
        import json
        import sys

        from src.quality.bronze_r2_quality_check import (
            validate_bronze_object,
        )

        payload = json.loads(
            sys.argv[1]
        )

        result = validate_bronze_object(
            bronze_object_key=payload[
                "bronze_object_key"
            ]
        )

        compact = {
            "statut": result[
                "statut"
            ],
            "batch_id": result[
                "batch_id"
            ],
            "processing_run_id": result[
                "processing_run_id"
            ],
            "schema_version": result[
                "schema_version"
            ],
            "path_version": result[
                "path_version"
            ],
            "nombre_lignes_raw": result[
                "nombre_lignes_raw"
            ],
            "nombre_lignes_bronze": result[
                "nombre_lignes_bronze"
            ],
            "nombre_ids_uniques": result[
                "nombre_ids_uniques"
            ],
            "nombre_colonnes_bronze": result[
                "nombre_colonnes_bronze"
            ],
            "schema_drift_detected": result[
                "schema_drift_detected"
            ],
            "schema_drift_field_count": result[
                "schema_drift_field_count"
            ],
            "raw_object_key": result[
                "raw_object_key"
            ],
            "bronze_object_key": result[
                "bronze_object_key"
            ],
            "raw_sha256": result[
                "raw_sha256"
            ],
            "bronze_sha256": result[
                "bronze_sha256"
            ],
        }

        print(
            "__AIRFLOW_RESULT_JSON__="
            + json.dumps(
                compact,
                ensure_ascii=False,
            )
        )
        """

        quality = run_project_python(
            child_code,
            {
                "bronze_object_key": (
                    bronze_result[
                        "bronze_object_key"
                    ]
                ),
            },
        )

        ensure_equal(
            quality[
                "statut"
            ],
            "VALIDE",
            "statut Quality Gate Bronze",
        )

        ensure_equal(
            quality[
                "batch_id"
            ],
            bronze_result[
                "batch_id"
            ],
            "batch_id Bronze",
        )

        ensure_equal(
            quality[
                "processing_run_id"
            ],
            bronze_result[
                "processing_run_id"
            ],
            "processing_run_id Bronze",
        )

        ensure_equal(
            quality[
                "raw_object_key"
            ],
            bronze_result[
                "raw_object_key"
            ],
            "raw_object_key Bronze",
        )

        ensure_equal(
            quality[
                "bronze_object_key"
            ],
            bronze_result[
                "bronze_object_key"
            ],
            "bronze_object_key",
        )

        ensure_equal(
            quality[
                "raw_sha256"
            ],
            bronze_result[
                "raw_sha256"
            ],
            "SHA-256 Raw Bronze",
        )

        ensure_equal(
            quality[
                "bronze_sha256"
            ],
            bronze_result[
                "sha256"
            ],
            "SHA-256 Bronze",
        )

        ensure_equal(
            quality[
                "nombre_lignes_bronze"
            ],
            bronze_result[
                "nombre_lignes_bronze"
            ],
            "volume Bronze",
        )

        ensure_equal(
            quality[
                "nombre_ids_uniques"
            ],
            quality[
                "nombre_lignes_bronze"
            ],
            "IDs uniques Bronze",
        )

        ensure_equal(
            quality[
                "schema_version"
            ],
            "2.0.0",
            "schema Bronze QG",
        )

        ensure_equal(
            quality[
                "path_version"
            ],
            "processing_v2",
            "path Bronze QG",
        )

        return {
            **quality,
            "storage_status": (
                bronze_result[
                    "storage_status"
                ]
            ),
        }

    # ========================================================
    # 8. BRONZE -> SILVER
    # ========================================================

    @task(
        task_id="bronze_to_silver",
        retries=2,
        retry_delay=timedelta(
            minutes=2
        ),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(
            minutes=10
        ),
    )
    def bronze_to_silver(
        bronze_validation_result: dict,
    ) -> dict:

        ensure_equal(
            bronze_validation_result[
                "statut"
            ],
            "VALIDE",
            (
                "statut Bronze "
                "avant Silver"
            ),
        )

        child_code = """
        import json
        import sys

        from src.transformation.bronze_to_silver import (
            run_silver_transformation,
        )

        payload = json.loads(
            sys.argv[1]
        )

        result = run_silver_transformation(
            bronze_object_key=payload[
                "bronze_object_key"
            ],
            processing_run_id=payload[
                "processing_run_id"
            ],
        )

        compact = {
            "batch_id": result[
                "batch_id"
            ],
            "bronze_processing_run_id": result[
                "bronze_processing_run_id"
            ],
            "processing_run_id": result[
                "processing_run_id"
            ],
            "replay_silver": result[
                "replay_silver"
            ],
            "source": result[
                "source"
            ],
            "ingestion_date": result[
                "ingestion_date"
            ],
            "schema_version": result[
                "schema_version"
            ],
            "path_version": result[
                "path_version"
            ],
            "raw_object_key": result[
                "raw_object_key"
            ],
            "bronze_object_key": result[
                "bronze_object_key"
            ],
            "silver_object_key": result[
                "silver_object_key"
            ],
            "nombre_lignes_bronze": result[
                "nombre_lignes_bronze"
            ],
            "nombre_lignes_silver": result[
                "nombre_lignes_silver"
            ],
            "nombre_ids_uniques": result[
                "nombre_ids_uniques"
            ],
            "nombre_colonnes_silver": result[
                "nombre_colonnes_silver"
            ],
            "localisation_renseignee": result[
                "localisation_renseignee"
            ],
            "coordonnees_renseignees": result[
                "coordonnees_renseignees"
            ],
            "salaires_renseignes": result[
                "salaires_renseignes"
            ],
            "information_entreprise_disponible": result[
                "information_entreprise_disponible"
            ],
            "raw_sha256": result[
                "raw_sha256"
            ],
            "bronze_sha256": result[
                "bronze_sha256"
            ],
            "size_bytes": result[
                "size_bytes"
            ],
            "sha256": result[
                "sha256"
            ],
            "storage_status": result[
                "storage_status"
            ],
        }

        print(
            "__AIRFLOW_RESULT_JSON__="
            + json.dumps(
                compact,
                ensure_ascii=False,
            )
        )
        """

        result = run_project_python(
            child_code,
            {
                "bronze_object_key": (
                    bronze_validation_result[
                        "bronze_object_key"
                    ]
                ),
                "processing_run_id": (
                    bronze_validation_result[
                        "processing_run_id"
                    ]
                ),
            },
        )

        ensure_equal(
            result[
                "batch_id"
            ],
            bronze_validation_result[
                "batch_id"
            ],
            "batch_id Silver",
        )

        ensure_equal(
            result[
                "bronze_processing_run_id"
            ],
            bronze_validation_result[
                "processing_run_id"
            ],
            (
                "bronze_processing_run_id "
                "Silver"
            ),
        )

        ensure_equal(
            result[
                "processing_run_id"
            ],
            bronze_validation_result[
                "processing_run_id"
            ],
            "processing_run_id Silver",
        )

        ensure_equal(
            result[
                "replay_silver"
            ],
            False,
            "replay_silver",
        )

        ensure_equal(
            result[
                "bronze_object_key"
            ],
            bronze_validation_result[
                "bronze_object_key"
            ],
            "bronze_object_key Silver",
        )

        ensure_equal(
            result[
                "raw_object_key"
            ],
            bronze_validation_result[
                "raw_object_key"
            ],
            "raw_object_key Silver",
        )

        ensure_equal(
            result[
                "raw_sha256"
            ],
            bronze_validation_result[
                "raw_sha256"
            ],
            "SHA-256 Raw Silver",
        )

        ensure_equal(
            result[
                "bronze_sha256"
            ],
            bronze_validation_result[
                "bronze_sha256"
            ],
            "SHA-256 Bronze Silver",
        )

        ensure_equal(
            result[
                "nombre_lignes_bronze"
            ],
            bronze_validation_result[
                "nombre_lignes_bronze"
            ],
            "volume Bronze avant Silver",
        )

        ensure_equal(
            result[
                "nombre_lignes_silver"
            ],
            result[
                "nombre_lignes_bronze"
            ],
            "non-perte Bronze -> Silver",
        )

        ensure_equal(
            result[
                "nombre_ids_uniques"
            ],
            result[
                "nombre_lignes_silver"
            ],
            "IDs uniques Silver",
        )

        ensure_equal(
            result[
                "schema_version"
            ],
            "2.0.0",
            "schema Silver",
        )

        ensure_equal(
            result[
                "path_version"
            ],
            "processing_v2",
            "path Silver",
        )

        ensure_in(
            result[
                "storage_status"
            ],
            {
                "CREATED",
                "REUSED",
            },
            "storage_status Silver",
        )

        print(
            "Transformation Bronze -> Silver : "
            f"{result['storage_status']}"
        )

        return result

    # ========================================================
    # 9. QUALITY GATE SILVER
    # ========================================================

    @task(
        task_id="validate_silver",
        retries=0,
    )
    def validate_silver(
        silver_result: dict,
    ) -> dict:

        child_code = """
        import json
        import sys

        from src.quality.silver_r2_quality_check import (
            validate_silver_object,
        )

        payload = json.loads(
            sys.argv[1]
        )

        result = validate_silver_object(
            silver_object_key=payload[
                "silver_object_key"
            ]
        )

        compact = {
            "statut": result[
                "statut"
            ],
            "batch_id": result[
                "batch_id"
            ],
            "bronze_processing_run_id": result[
                "bronze_processing_run_id"
            ],
            "processing_run_id": result[
                "processing_run_id"
            ],
            "replay_silver": result[
                "replay_silver"
            ],
            "schema_version": result[
                "schema_version"
            ],
            "path_version": result[
                "path_version"
            ],
            "nombre_lignes_raw": result[
                "nombre_lignes_raw"
            ],
            "nombre_lignes_bronze": result[
                "nombre_lignes_bronze"
            ],
            "nombre_lignes_silver": result[
                "nombre_lignes_silver"
            ],
            "nombre_ids_uniques": result[
                "nombre_ids_uniques"
            ],
            "nombre_colonnes_silver": result[
                "nombre_colonnes_silver"
            ],
            "localisation_renseignee": result[
                "localisation_renseignee"
            ],
            "taux_localisation_pct": result[
                "taux_localisation_pct"
            ],
            "coordonnees_renseignees": result[
                "coordonnees_renseignees"
            ],
            "taux_coordonnees_pct": result[
                "taux_coordonnees_pct"
            ],
            "salaires_renseignes": result[
                "salaires_renseignes"
            ],
            "taux_salaires_pct": result[
                "taux_salaires_pct"
            ],
            "information_entreprise_disponible": result[
                "information_entreprise_disponible"
            ],
            "taux_information_entreprise_pct": result[
                "taux_information_entreprise_pct"
            ],
            "raw_object_key": result[
                "raw_object_key"
            ],
            "bronze_object_key": result[
                "bronze_object_key"
            ],
            "silver_object_key": result[
                "silver_object_key"
            ],
            "raw_sha256": result[
                "raw_sha256"
            ],
            "bronze_sha256": result[
                "bronze_sha256"
            ],
            "silver_sha256": result[
                "silver_sha256"
            ],
        }

        print(
            "__AIRFLOW_RESULT_JSON__="
            + json.dumps(
                compact,
                ensure_ascii=False,
            )
        )
        """

        quality = run_project_python(
            child_code,
            {
                "silver_object_key": (
                    silver_result[
                        "silver_object_key"
                    ]
                ),
            },
        )

        ensure_equal(
            quality[
                "statut"
            ],
            "VALIDE",
            (
                "statut "
                "Quality Gate Silver"
            ),
        )

        ensure_equal(
            quality[
                "batch_id"
            ],
            silver_result[
                "batch_id"
            ],
            "batch_id Silver QG",
        )

        ensure_equal(
            quality[
                "bronze_processing_run_id"
            ],
            silver_result[
                "bronze_processing_run_id"
            ],
            (
                "bronze_processing_run_id "
                "Silver QG"
            ),
        )

        ensure_equal(
            quality[
                "processing_run_id"
            ],
            silver_result[
                "processing_run_id"
            ],
            "processing_run_id Silver QG",
        )

        ensure_equal(
            quality[
                "replay_silver"
            ],
            False,
            "replay_silver QG",
        )

        ensure_equal(
            quality[
                "raw_object_key"
            ],
            silver_result[
                "raw_object_key"
            ],
            "raw_object_key Silver QG",
        )

        ensure_equal(
            quality[
                "bronze_object_key"
            ],
            silver_result[
                "bronze_object_key"
            ],
            "bronze_object_key Silver QG",
        )

        ensure_equal(
            quality[
                "silver_object_key"
            ],
            silver_result[
                "silver_object_key"
            ],
            "silver_object_key Silver QG",
        )

        ensure_equal(
            quality[
                "raw_sha256"
            ],
            silver_result[
                "raw_sha256"
            ],
            "SHA-256 Raw Silver QG",
        )

        ensure_equal(
            quality[
                "bronze_sha256"
            ],
            silver_result[
                "bronze_sha256"
            ],
            "SHA-256 Bronze Silver QG",
        )

        ensure_equal(
            quality[
                "silver_sha256"
            ],
            silver_result[
                "sha256"
            ],
            "SHA-256 Silver QG",
        )

        ensure_equal(
            quality[
                "nombre_lignes_silver"
            ],
            silver_result[
                "nombre_lignes_silver"
            ],
            "volume Silver QG",
        )

        ensure_equal(
            quality[
                "nombre_ids_uniques"
            ],
            quality[
                "nombre_lignes_silver"
            ],
            "IDs uniques Silver QG",
        )

        ensure_equal(
            quality[
                "schema_version"
            ],
            "2.0.0",
            "schema Silver QG",
        )

        ensure_equal(
            quality[
                "path_version"
            ],
            "processing_v2",
            "path Silver QG",
        )

        result = {
            **quality,
            "storage_status": (
                silver_result[
                    "storage_status"
                ]
            ),
        }

        print(
            "Silver Quality Gate : VALIDE"
        )

        return result

    # ========================================================
    # 10. SILVER -> POSTGRESQL
    # ========================================================

    @task(
        task_id="load_silver_to_postgres",
        retries=2,
        retry_delay=timedelta(
            minutes=2
        ),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(
            minutes=10
        ),
    )
    def load_silver_to_postgres(
        silver_validation_result: dict,
    ) -> dict:
        """
        Charge le snapshot Silver R2
        dans PostgreSQL.

        PostgreSQL contient uniquement
        le snapshot analytique courant.

        Le loader est idempotent :

        - premier passage :
            VALIDE

        - retry même snapshot :
            DÉJÀ À JOUR
        """

        ensure_equal(
            silver_validation_result[
                "statut"
            ],
            "VALIDE",
            (
                "statut Silver "
                "avant PostgreSQL"
            ),
        )

        silver_object_key = (
            silver_validation_result[
                "silver_object_key"
            ]
        )

        output = run_project_command(
            [
                "-m",
                (
                    "src.loading."
                    "load_silver_to_postgres"
                ),
                "--silver-object-key",
                silver_object_key,
            ]
        )

        load_valid = (
            "STATUT CHARGEMENT : VALIDE"
            in output
        )

        load_already_current = (
            "STATUT CHARGEMENT : DÉJÀ À JOUR"
            in output
        )

        if not (
            load_valid
            or load_already_current
        ):
            raise RuntimeError(
                "Le loader PostgreSQL "
                "s'est terminé sans statut reconnu."
            )

        if load_valid:
            storage_status = (
                "LOADED"
            )

        else:
            storage_status = (
                "ALREADY_CURRENT"
            )

        result = {
            "statut": "VALIDE",
            "storage_status": (
                storage_status
            ),
            "batch_id": (
                silver_validation_result[
                    "batch_id"
                ]
            ),
            "processing_run_id": (
                silver_validation_result[
                    "processing_run_id"
                ]
            ),
            "silver_object_key": (
                silver_object_key
            ),
            "silver_sha256": (
                silver_validation_result[
                    "silver_sha256"
                ]
            ),
            "nombre_lignes_silver": (
                silver_validation_result[
                    "nombre_lignes_silver"
                ]
            ),
            "table_postgresql": (
                "silver.france_travail_offres"
            ),
        }

        print(
            "Chargement PostgreSQL Airflow : "
            f"{storage_status}"
        )

        print(
            json.dumps(
                result,
                indent=2,
            )
        )

        return result

    # ========================================================
    # 11. QUALITY GATE POSTGRESQL
    # ========================================================

    @task(
        task_id="validate_postgres",
        retries=0,
    )
    def validate_postgres(
        postgres_load_result: dict,
    ) -> dict:
        """
        Validation indépendante :

            Silver R2
               ↓
            PostgreSQL

        Contrôles notamment :

        - volumes ;
        - schéma ;
        - IDs ;
        - contenu ;
        - audit ;
        - table temporaire __load absente.
        """

        ensure_equal(
            postgres_load_result[
                "statut"
            ],
            "VALIDE",
            (
                "statut chargement "
                "avant validation PostgreSQL"
            ),
        )

        silver_object_key = (
            postgres_load_result[
                "silver_object_key"
            ]
        )

        output = run_project_command(
            [
                "-m",
                (
                    "src.quality."
                    "silver_postgres_validation_report"
                ),
                "--silver-object-key",
                silver_object_key,
            ]
        )

        expected_status = (
            "STATUT SILVER -> POSTGRESQL : VALIDE"
        )

        if expected_status not in output:
            raise RuntimeError(
                "Quality Gate "
                "Silver -> PostgreSQL "
                "NON VALIDE."
            )

        result = {
            "statut": "VALIDE",
            "batch_id": (
                postgres_load_result[
                    "batch_id"
                ]
            ),
            "processing_run_id": (
                postgres_load_result[
                    "processing_run_id"
                ]
            ),
            "silver_object_key": (
                silver_object_key
            ),
            "silver_sha256": (
                postgres_load_result[
                    "silver_sha256"
                ]
            ),
            "nombre_lignes": (
                postgres_load_result[
                    "nombre_lignes_silver"
                ]
            ),
            "table_postgresql": (
                postgres_load_result[
                    "table_postgresql"
                ]
            ),
            "load_storage_status": (
                postgres_load_result[
                    "storage_status"
                ]
            ),
        }

        print(
            "Quality Gate "
            "Silver -> PostgreSQL : VALIDE"
        )

        print(
            json.dumps(
                result,
                indent=2,
            )
        )

        return result

    @task(
        task_id="dbt_build",
        retries=0,
    )
    def dbt_build(
        postgres_validation_result: dict,
    ) -> dict:
        """
        Construit et teste la couche analytique dbt
        après validation du snapshot PostgreSQL.
        """

        ensure_equal(
            postgres_validation_result.get(
                "statut"
            ),
            "VALIDE",
            (
                "Le Quality Gate PostgreSQL "
                "doit être VALIDE avant dbt."
            ),
        )

        dbt_project_file = (
            DBT_PROJECT_DIR
            / "dbt_project.yml"
        )

        dbt_profiles_file = (
            DBT_PROFILES_DIR
            / "profiles.yml"
        )

        if not Path(
            PROJECT_DBT
        ).exists():
            raise RuntimeError(
                "Binaire dbt introuvable : "
                f"{PROJECT_DBT}"
            )

        if not dbt_project_file.exists():
            raise RuntimeError(
                "dbt_project.yml introuvable : "
                f"{dbt_project_file}"
            )

        if not dbt_profiles_file.exists():
            raise RuntimeError(
                "profiles.yml introuvable : "
                f"{dbt_profiles_file}"
            )

        print()
        print(
            "=" * 70
        )
        print(
            "DBT BUILD"
        )
        print(
            "=" * 70
        )
        print()

        print(
            "Batch ID          : "
            f"{postgres_validation_result.get('batch_id')}"
        )

        print(
            "Processing Run ID : "
            f"{postgres_validation_result.get('processing_run_id')}"
        )

        print(
            "Objet Silver      : "
            f"{postgres_validation_result.get('silver_object_key')}"
        )

        print(
            "Projet dbt        : "
            f"{DBT_PROJECT_DIR}"
        )

        print()

        run_project_command(
            [
                PROJECT_DBT,
                "build",
                "--project-dir",
                str(
                    DBT_PROJECT_DIR
                ),
                "--profiles-dir",
                str(
                    DBT_PROFILES_DIR
                ),
            ]
        )

        print()
        print(
            "STATUT DBT BUILD : VALIDE"
        )

        return {
            "statut": "VALIDE",
            "dbt_status": "BUILT",
            "batch_id": (
                postgres_validation_result.get(
                    "batch_id"
                )
            ),
            "processing_run_id": (
                postgres_validation_result.get(
                    "processing_run_id"
                )
            ),
            "silver_object_key": (
                postgres_validation_result.get(
                    "silver_object_key"
                )
            ),
            "silver_sha256": (
                postgres_validation_result.get(
                    "silver_sha256"
                )
            ),
            "nombre_lignes": (
                postgres_validation_result.get(
                    "nombre_lignes"
                )
            ),
            "table_postgresql": (
                postgres_validation_result.get(
                    "table_postgresql"
                )
            ),
        }

    # ========================================================
    # DEPENDANCES
    # ========================================================
    monitoring_start_result = (
        start_monitoring()
    )

    preflight_result = (
        preflight()
    )

    acquisition_context = (
        create_acquisition_context()
    )
    monitoring_start_result >> preflight_result
    preflight_result >> acquisition_context

    ingestion_result = (
        ingest_raw(
            acquisition_context
        )
    )

    raw_validation_result = (
        validate_raw(
            ingestion_result
        )
    )

    processing_context = (
        create_processing_context(
            raw_validation_result
        )
    )

    bronze_result = (
        raw_to_bronze(
            processing_context
        )
    )

    bronze_validation_result = (
        validate_bronze(
            bronze_result
        )
    )

    silver_result = (
        bronze_to_silver(
            bronze_validation_result
        )
    )

    silver_validation_result = (
        validate_silver(
            silver_result
        )
    )

    postgres_load_result = (
        load_silver_to_postgres(
            silver_validation_result
        )
    )

    postgres_validation_result = (
        validate_postgres(
            postgres_load_result
        )
    )

    dbt_build_result = (
        dbt_build(
            postgres_validation_result
        )
    )


france_travail_job_market_pipeline()