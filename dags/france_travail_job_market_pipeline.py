import json
import os
import subprocess
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from airflow.sdk import (
    dag,
    get_current_context,
    task,
)


# ============================================================
# Configuration du DAG
# ============================================================

DAG_ID = "france_travail_job_market_pipeline"

PROJECT_ROOT = os.getenv(
    "PROJECT_ROOT",
    "/opt/airflow/project",
)

PROJECT_PYTHON = os.getenv(
    "PROJECT_PYTHON",
    "/opt/airflow/project_venv/bin/python",
)

RESULT_PREFIX = "__AIRFLOW_RESULT_JSON__="


# ============================================================
# Helper d'exécution dans le virtualenv projet
# ============================================================


def run_project_python(
    python_code: str,
    payload: dict,
) -> dict:
    """
    Exécute du code Python dans le virtualenv du projet.

    Objectifs :
    - Airflow reste dans son environnement Python natif ;
    - les dépendances métier restent isolées dans le venv projet ;
    - le DAG n'importe pas les dépendances métier au parsing ;
    - seuls de petits résultats JSON remontent dans XCom.
    """

    project_root = Path(PROJECT_ROOT)
    project_python = Path(PROJECT_PYTHON)

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
        str(project_python),
        "-c",
        textwrap.dedent(python_code),
        json.dumps(
            payload,
            ensure_ascii=False,
        ),
    ]

    process = subprocess.Popen(
        command,
        cwd=str(project_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    airflow_result = None

    if process.stdout is None:
        raise RuntimeError(
            "Impossible de capturer la sortie "
            "du processus projet."
        )

    for raw_line in process.stdout:
        line = raw_line.rstrip()

        print(
            line,
            flush=True,
        )

        if line.startswith(RESULT_PREFIX):
            raw_result = line[
                len(RESULT_PREFIX):
            ]

            airflow_result = json.loads(
                raw_result
            )

    return_code = process.wait()

    if return_code != 0:
        raise RuntimeError(
            "Le processus Python projet a échoué "
            f"avec le code {return_code}."
        )

    if airflow_result is None:
        raise RuntimeError(
            "Le processus projet s'est terminé "
            "sans retourner de résultat Airflow."
        )

    return airflow_result


# ============================================================
# Définition du DAG
# ============================================================


@dag(
    dag_id=DAG_ID,
    description=(
        "Pipeline Data Engineering France Travail : "
        "acquisition, qualité, transformation et publication."
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
    tags=[
        "france-travail",
        "data-engineering",
        "r2",
        "dbt",
    ],
)
def france_travail_job_market_pipeline():

    # ========================================================
    # 1. Preflight
    # ========================================================

    @task(
        task_id="preflight",
        retries=0,
    )
    def preflight() -> dict:
        """
        Vérifie les dépendances nécessaires au slice actuel :
        acquisition Raw + Quality Gate Raw + Bronze + Quality Gate Bronze.

        Aucun appel France Travail et aucune écriture R2.
        """

        project_root = Path(PROJECT_ROOT)
        project_python = Path(PROJECT_PYTHON)

        required_files = [
            project_root
            / "src"
            / "ingestion"
            / "france_travail_ingestion.py",

            project_root
            / "src"
            / "quality"
            / "raw_r2_quality_check.py",

            project_root
            / "src"
            / "transformation"
            / "raw_to_bronze.py",

            project_root
            / "src"
            / "quality"
            / "bronze_r2_quality_check.py",

            project_root
            / "src"
            / "storage"
            / "r2_storage.py",
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
            str(path)
            for path in required_files
            if not path.is_file()
        ]

        if missing_files:
            raise RuntimeError(
                "Fichiers projet manquants : "
                + ", ".join(missing_files)
            )

        runtime_check = subprocess.run(
            [
                str(project_python),
                "-c",
                (
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
                    "src.storage.r2_storage "
                    "import R2Storage; "
                    "print('PROJECT_RUNTIME_OK')"
                ),
            ],
            cwd=str(project_root),
            capture_output=True,
            text=True,
        )

        if runtime_check.returncode != 0:
            print(runtime_check.stdout)
            print(runtime_check.stderr)

            raise RuntimeError(
                "Le runtime Python projet "
                "n'est pas opérationnel."
            )

        print("Preflight validé.")
        print(f"PROJECT_ROOT={project_root}")
        print(f"PROJECT_PYTHON={project_python}")

        return {
            "status": "VALID",
            "project_root": str(project_root),
            "project_python": str(project_python),
        }

    # ========================================================
    # 2. Création du contexte d'acquisition
    # ========================================================

    @task(
        task_id="create_acquisition_context",
        retries=0,
    )
    def create_acquisition_context() -> dict:
        """
        Crée le contexte métier d'acquisition.

        Un même DAG Run conserve le même batch_id et le même
        ingestion_datetime lors d'un retry.
        """

        context = get_current_context()

        run_id = context.get("run_id")

        if not run_id:
            task_instance = (
                context.get("task_instance")
                or context.get("ti")
            )

            run_id = getattr(
                task_instance,
                "run_id",
                None,
            )

        if not run_id:
            raise RuntimeError(
                "Impossible de récupérer "
                "le run_id Airflow."
            )

        logical_date = context.get(
            "logical_date"
        )

        if logical_date is None:
            dag_run = context.get("dag_run")

            logical_date = getattr(
                dag_run,
                "run_after",
                None,
            )

        if logical_date is None:
            raise RuntimeError(
                "Impossible de déterminer "
                "l'horodatage stable du DAG Run."
            )

        if logical_date.tzinfo is None:
            raise RuntimeError(
                "L'horodatage Airflow doit être "
                "timezone-aware."
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
            "batch_id": str(batch_id),
            "ingestion_datetime_utc": (
                ingestion_datetime.isoformat()
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
    # 3. Ingestion Raw
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
        """
        Exécute l'ingestion France Travail dans le virtualenv projet.

        Grâce à l'idempotence :
        - premier passage : CREATED ;
        - retry du même DAG Run : REUSED si le Raw existe déjà.
        """

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

        ingestion_datetime = (
            datetime.fromisoformat(
                payload[
                    "ingestion_datetime_utc"
                ]
            )
        )

        result = run_ingestion(
            batch_id=payload[
                "batch_id"
            ],
            ingestion_datetime=(
                ingestion_datetime
            ),
        )

        compact_result = {
            "batch_id": result[
                "batch_id"
            ],
            "ingestion_datetime_utc": (
                result[
                    "ingestion_datetime_utc"
                ]
            ),
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
                compact_result,
                ensure_ascii=False,
            )
        )
        """

        result = run_project_python(
            python_code=child_code,
            payload=acquisition_context,
        )

        if (
            result["batch_id"]
            != acquisition_context["batch_id"]
        ):
            raise RuntimeError(
                "batch_id incohérent après ingestion."
            )

        if (
            result["ingestion_datetime_utc"]
            != acquisition_context[
                "ingestion_datetime_utc"
            ]
        ):
            raise RuntimeError(
                "ingestion_datetime incohérent "
                "après ingestion."
            )

        if result["storage_status"] not in {
            "CREATED",
            "REUSED",
        }:
            raise RuntimeError(
                "storage_status inattendu : "
                f"{result['storage_status']}"
            )

        print(
            "Ingestion Raw terminée : "
            f"{result['storage_status']}"
        )
        print(
            f"Offres : {result['nombre_offres']}"
        )
        print(
            f"Objet : {result['object_key']}"
        )

        return result

    # ========================================================
    # 4. Raw Quality Gate
    # ========================================================

    @task(
        task_id="validate_raw",
        retries=0,
    )
    def validate_raw(
        ingestion_result: dict,
    ) -> dict:
        """
        Valide explicitement le Raw produit ou réutilisé.
        Une erreur de qualité bloque le pipeline.
        """

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

        compact_result = {
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
                compact_result,
                ensure_ascii=False,
            )
        )
        """

        quality_result = run_project_python(
            python_code=child_code,
            payload={
                "object_key": (
                    ingestion_result[
                        "object_key"
                    ]
                ),
            },
        )

        if quality_result["statut"] != "VALIDE":
            raise RuntimeError(
                "Raw Quality Gate non valide."
            )

        if (
            quality_result["batch_id"]
            != ingestion_result["batch_id"]
        ):
            raise RuntimeError(
                "batch_id incohérent entre "
                "ingestion et Raw Quality Gate."
            )

        if (
            quality_result["nombre_offres"]
            != ingestion_result["nombre_offres"]
        ):
            raise RuntimeError(
                "Nombre d'offres incohérent entre "
                "ingestion et Raw Quality Gate."
            )

        if (
            quality_result["sha256"]
            != ingestion_result["sha256"]
        ):
            raise RuntimeError(
                "SHA-256 incohérent entre "
                "ingestion et Raw Quality Gate."
            )

        if quality_result["nombre_doublons"] != 0:
            raise RuntimeError(
                "Le Raw contient des doublons."
            )

        result = {
            "statut": "VALIDE",
            "batch_id": ingestion_result[
                "batch_id"
            ],
            "ingestion_datetime_utc": (
                ingestion_result[
                    "ingestion_datetime_utc"
                ]
            ),
            "raw_object_key": ingestion_result[
                "object_key"
            ],
            "nombre_offres": quality_result[
                "nombre_offres"
            ],
            "sha256": quality_result[
                "sha256"
            ],
            "storage_status": ingestion_result[
                "storage_status"
            ],
        }

        print("Raw Quality Gate : VALIDE")
        print(
            json.dumps(
                result,
                indent=2,
            )
        )

        return result

    # ========================================================
    # 5. Création du contexte de processing
    # ========================================================

    @task(
        task_id="create_processing_context",
        retries=0,
    )
    def create_processing_context(
        raw_validation_result: dict,
    ) -> dict:
        """
        Crée l'identité du processing Raw -> Bronze -> Silver.

        Le processing_run_id est déterministe pour un même DAG Run :
        un retry conserve donc le même identifiant.
        """

        if raw_validation_result["statut"] != "VALIDE":
            raise RuntimeError(
                "Impossible de créer le contexte de processing : "
                "le Raw n'est pas valide."
            )

        context = get_current_context()

        run_id = context.get("run_id")

        if not run_id:
            task_instance = (
                context.get("task_instance")
                or context.get("ti")
            )

            run_id = getattr(
                task_instance,
                "run_id",
                None,
            )

        if not run_id:
            raise RuntimeError(
                "Impossible de récupérer "
                "le run_id Airflow."
            )

        batch_id = raw_validation_result[
            "batch_id"
        ]

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
    # 6. Transformation Raw -> Bronze
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
        """
        Transforme le Raw validé en Bronze processing_v2.

        Grâce à l'idempotence Bronze :
        - premier passage : CREATED ;
        - retry du même processing_run_id : REUSED.
        """

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

        schema_drift_field_count = result.get(
            "schema_drift_field_count"
        )

        if schema_drift_field_count is None:
            schema_drift = result.get(
                "schema_drift"
            ) or {}

            schema_drift_field_count = len(
                schema_drift
            )

        compact_result = {
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
                schema_drift_field_count
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
                compact_result,
                ensure_ascii=False,
            )
        )
        """

        result = run_project_python(
            python_code=child_code,
            payload=processing_context,
        )

        if (
            result["batch_id"]
            != processing_context["batch_id"]
        ):
            raise RuntimeError(
                "batch_id incohérent après "
                "transformation Bronze."
            )

        if (
            result["processing_run_id"]
            != processing_context[
                "processing_run_id"
            ]
        ):
            raise RuntimeError(
                "processing_run_id incohérent "
                "après transformation Bronze."
            )

        if (
            result["raw_object_key"]
            != processing_context[
                "raw_object_key"
            ]
        ):
            raise RuntimeError(
                "raw_object_key incohérent "
                "après transformation Bronze."
            )

        if (
            result["raw_sha256"]
            != processing_context[
                "raw_sha256"
            ]
        ):
            raise RuntimeError(
                "SHA-256 Raw incohérent "
                "dans le lineage Bronze."
            )

        if (
            result["nombre_lignes_raw"]
            != processing_context[
                "nombre_offres"
            ]
        ):
            raise RuntimeError(
                "Volume Raw incohérent "
                "pendant la transformation Bronze."
            )

        if (
            result["nombre_lignes_bronze"]
            != result["nombre_lignes_raw"]
        ):
            raise RuntimeError(
                "Perte de lignes Raw -> Bronze."
            )

        if result["schema_version"] != "2.0.0":
            raise RuntimeError(
                "Schema version Bronze inattendue : "
                f"{result['schema_version']}"
            )

        if result["path_version"] != "processing_v2":
            raise RuntimeError(
                "Path version Bronze inattendue : "
                f"{result['path_version']}"
            )

        if result["storage_status"] not in {
            "CREATED",
            "REUSED",
        }:
            raise RuntimeError(
                "storage_status Bronze inattendu : "
                f"{result['storage_status']}"
            )

        print(
            "Transformation Raw -> Bronze terminée : "
            f"{result['storage_status']}"
        )
        print(
            "Lignes Bronze : "
            f"{result['nombre_lignes_bronze']}"
        )
        print(
            "Objet Bronze : "
            f"{result['bronze_object_key']}"
        )

        return result

    # ========================================================
    # 7. Bronze Quality Gate
    # ========================================================

    @task(
        task_id="validate_bronze",
        retries=0,
    )
    def validate_bronze(
        bronze_result: dict,
    ) -> dict:
        """
        Exécute le Quality Gate complet Bronze processing_v2.
        """

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

        compact_result = {
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
                compact_result,
                ensure_ascii=False,
            )
        )
        """

        quality_result = run_project_python(
            python_code=child_code,
            payload={
                "bronze_object_key": (
                    bronze_result[
                        "bronze_object_key"
                    ]
                ),
            },
        )

        if quality_result["statut"] != "VALIDE":
            raise RuntimeError(
                "Bronze Quality Gate non valide."
            )

        if (
            quality_result["batch_id"]
            != bronze_result["batch_id"]
        ):
            raise RuntimeError(
                "batch_id incohérent entre "
                "transformation et Quality Gate Bronze."
            )

        if (
            quality_result["processing_run_id"]
            != bronze_result[
                "processing_run_id"
            ]
        ):
            raise RuntimeError(
                "processing_run_id incohérent entre "
                "transformation et Quality Gate Bronze."
            )

        if (
            quality_result["raw_object_key"]
            != bronze_result[
                "raw_object_key"
            ]
        ):
            raise RuntimeError(
                "raw_object_key incohérent dans "
                "le Quality Gate Bronze."
            )

        if (
            quality_result["bronze_object_key"]
            != bronze_result[
                "bronze_object_key"
            ]
        ):
            raise RuntimeError(
                "bronze_object_key incohérent dans "
                "le Quality Gate Bronze."
            )

        if (
            quality_result["raw_sha256"]
            != bronze_result[
                "raw_sha256"
            ]
        ):
            raise RuntimeError(
                "SHA-256 Raw incohérent entre "
                "transformation et Quality Gate Bronze."
            )

        if (
            quality_result["bronze_sha256"]
            != bronze_result["sha256"]
        ):
            raise RuntimeError(
                "SHA-256 Bronze incohérent entre "
                "transformation et Quality Gate Bronze."
            )

        if (
            quality_result["nombre_lignes_raw"]
            != bronze_result[
                "nombre_lignes_raw"
            ]
        ):
            raise RuntimeError(
                "Volume Raw incohérent dans "
                "le Quality Gate Bronze."
            )

        if (
            quality_result["nombre_lignes_bronze"]
            != bronze_result[
                "nombre_lignes_bronze"
            ]
        ):
            raise RuntimeError(
                "Volume Bronze incohérent dans "
                "le Quality Gate Bronze."
            )

        if (
            quality_result["nombre_ids_uniques"]
            != quality_result[
                "nombre_lignes_bronze"
            ]
        ):
            raise RuntimeError(
                "Le Bronze ne possède pas exactement "
                "un ID unique par ligne."
            )

        if (
            quality_result["schema_version"]
            != "2.0.0"
        ):
            raise RuntimeError(
                "Schema version Bronze invalide."
            )

        if (
            quality_result["path_version"]
            != "processing_v2"
        ):
            raise RuntimeError(
                "Path version Bronze invalide."
            )

        result = {
            "statut": "VALIDE",
            "batch_id": quality_result[
                "batch_id"
            ],
            "processing_run_id": (
                quality_result[
                    "processing_run_id"
                ]
            ),
            "raw_object_key": (
                quality_result[
                    "raw_object_key"
                ]
            ),
            "bronze_object_key": (
                quality_result[
                    "bronze_object_key"
                ]
            ),
            "nombre_lignes_bronze": (
                quality_result[
                    "nombre_lignes_bronze"
                ]
            ),
            "nombre_ids_uniques": (
                quality_result[
                    "nombre_ids_uniques"
                ]
            ),
            "nombre_colonnes_bronze": (
                quality_result[
                    "nombre_colonnes_bronze"
                ]
            ),
            "schema_drift_detected": (
                quality_result[
                    "schema_drift_detected"
                ]
            ),
            "schema_drift_field_count": (
                quality_result[
                    "schema_drift_field_count"
                ]
            ),
            "raw_sha256": quality_result[
                "raw_sha256"
            ],
            "bronze_sha256": (
                quality_result[
                    "bronze_sha256"
                ]
            ),
            "storage_status": bronze_result[
                "storage_status"
            ],
        }

        print("Bronze Quality Gate : VALIDE")
        print(
            json.dumps(
                result,
                indent=2,
            )
        )

        return result

    # ========================================================
    # Dépendances
    # ========================================================

    preflight_result = preflight()

    acquisition_context = (
        create_acquisition_context()
    )

    preflight_result >> acquisition_context

    ingestion_result = ingest_raw(
        acquisition_context
    )

    raw_validation_result = validate_raw(
        ingestion_result
    )

    processing_context = (
        create_processing_context(
            raw_validation_result
        )
    )

    bronze_result = raw_to_bronze(
        processing_context
    )

    validate_bronze(
        bronze_result
    )


france_travail_job_market_pipeline()
