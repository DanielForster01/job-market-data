from __future__ import annotations

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
# Configuration générale
# ============================================================

DAG_ID = "france_travail_job_market_pipeline"

PROJECT_ROOT = Path(
    os.getenv(
        "PROJECT_ROOT",
        "/opt/airflow/project",
    )
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
    PROJECT_ROOT
    / "dbt_project"
)

DBT_PROJECT_FILE = (
    DBT_PROJECT_DIR
    / "dbt_project.yml"
)

DBT_PROFILES_DIR = (
    DBT_PROJECT_DIR
)

DBT_PROFILES_FILE = (
    DBT_PROFILES_DIR
    / "profiles.yml"
)

RESULT_PREFIX = (
    "__AIRFLOW_RESULT_JSON__="
)


# ============================================================
# Helpers généraux
# ============================================================

def ensure_equal(
    actual,
    expected,
    message: str,
) -> None:
    """
    Vérifie une égalité stricte.
    """

    if actual != expected:
        raise RuntimeError(
            f"{message} "
            f"Attendu={expected!r}, "
            f"obtenu={actual!r}."
        )


def ensure_in(
    value,
    allowed_values,
    message: str,
) -> None:
    """
    Vérifie qu'une valeur appartient
    à un ensemble de valeurs autorisées.
    """

    if value not in allowed_values:
        raise RuntimeError(
            f"{message} "
            f"Valeur={value!r}, "
            f"valeurs autorisées="
            f"{sorted(allowed_values)!r}."
        )


def run_project_python(
    python_code: str,
    payload: dict,
) -> dict:
    """
    Exécute du code Python dans le virtualenv projet.

    Le payload est transmis en JSON via stdin.

    Le code exécuté doit terminer par :

        __AIRFLOW_RESULT_JSON__=<json>

    Cela permet de :
    - conserver l'environnement Airflow séparé ;
    - exécuter la logique métier dans project_venv ;
    - transmettre uniquement un petit résultat à XCom.
    """

    command = [
        PROJECT_PYTHON,
        "-c",
        textwrap.dedent(
            python_code
        ),
    ]

    print(
        "Commande Python projet : "
        f"{' '.join(command[:2])} <code>"
    )

    process = subprocess.Popen(
        command,
        cwd=str(
            PROJECT_ROOT
        ),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=os.environ.copy(),
    )

    if (
        process.stdin is None
        or process.stdout is None
    ):
        raise RuntimeError(
            "Impossible d'initialiser "
            "les flux du sous-processus Python."
        )

    process.stdin.write(
        json.dumps(
            payload,
            ensure_ascii=False,
            default=str,
        )
    )

    process.stdin.close()

    output_lines: list[str] = []

    for line in process.stdout:

        print(
            line,
            end="",
        )

        output_lines.append(
            line
        )

    return_code = (
        process.wait()
    )

    if return_code != 0:
        raise RuntimeError(
            "Le runtime Python projet "
            "a échoué avec le code "
            f"{return_code}."
        )

    result_line = None

    for line in reversed(
        output_lines
    ):
        stripped_line = (
            line.strip()
        )

        if stripped_line.startswith(
            RESULT_PREFIX
        ):
            result_line = (
                stripped_line
            )
            break

    if result_line is None:
        raise RuntimeError(
            "Aucun résultat structuré "
            f"{RESULT_PREFIX} retourné "
            "par le runtime projet."
        )

    result_json = (
        result_line[
            len(
                RESULT_PREFIX
            ):
        ]
    )

    try:
        result = json.loads(
            result_json
        )

    except json.JSONDecodeError as error:
        raise RuntimeError(
            "Résultat JSON invalide "
            "retourné par le runtime projet."
        ) from error

    if not isinstance(
        result,
        dict,
    ):
        raise RuntimeError(
            "Le runtime projet doit "
            "retourner un dictionnaire JSON."
        )

    return result


def run_project_command(
    command_args: list[str],
) -> str:
    """
    Exécute une commande dans le runtime projet.

    Utilisé notamment pour :
    - loader PostgreSQL ;
    - validation PostgreSQL ;
    - dbt.

    stdout et stderr sont envoyés
    directement dans les logs Airflow.
    """

    command = [
        str(
            argument
        )
        for argument
        in command_args
    ]

    print(
        "Commande projet : "
        + " ".join(
            command
        )
    )

    process = subprocess.Popen(
        command,
        cwd=str(
            PROJECT_ROOT
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=os.environ.copy(),
    )

    if process.stdout is None:
        raise RuntimeError(
            "Impossible de lire "
            "stdout du sous-processus."
        )

    output_lines: list[str] = []

    for line in process.stdout:

        print(
            line,
            end="",
        )

        output_lines.append(
            line
        )

    return_code = (
        process.wait()
    )

    if return_code != 0:
        raise RuntimeError(
            "La commande projet "
            "a échoué avec le code "
            f"{return_code}."
        )

    return "".join(
        output_lines
    )


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id=DAG_ID,
    description=(
        "Pipeline France Travail : "
        "API -> R2 Raw/Bronze/Silver -> "
        "PostgreSQL -> dbt analytics"
    ),
    start_date=datetime(
        2026,
        1,
        1,
        tzinfo=timezone.utc,
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
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
    # 1. PREFLIGHT
    # ========================================================

    @task(
        task_id="preflight",
        retries=0,
    )
    def preflight() -> dict:
        """
        Valide l'environnement avant toute acquisition.

        Aucun appel France Travail n'est réalisé
        tant que cette étape n'est pas verte.
        """

        print(
            "=" * 70
        )
        print(
            "PREFLIGHT PIPELINE"
        )
        print(
            "=" * 70
        )

        required_files = [
            (
                PROJECT_ROOT
                / "src"
                / "ingestion"
                / "france_travail_ingestion.py"
            ),
            (
                PROJECT_ROOT
                / "src"
                / "transformation"
                / "raw_to_bronze.py"
            ),
            (
                PROJECT_ROOT
                / "src"
                / "transformation"
                / "bronze_to_silver.py"
            ),
            (
                PROJECT_ROOT
                / "src"
                / "quality"
                / "raw_r2_quality_check.py"
            ),
            (
                PROJECT_ROOT
                / "src"
                / "quality"
                / "bronze_r2_quality_check.py"
            ),
            (
                PROJECT_ROOT
                / "src"
                / "quality"
                / "silver_r2_quality_check.py"
            ),
            (
                PROJECT_ROOT
                / "src"
                / "loading"
                / "load_silver_to_postgres.py"
            ),
            (
                PROJECT_ROOT
                / "src"
                / "quality"
                / "silver_postgres_validation_report.py"
            ),

            # dbt
            DBT_PROJECT_FILE,
            DBT_PROFILES_FILE,
        ]

        missing_files = [
            str(
                path
            )
            for path
            in required_files
            if not path.exists()
        ]

        if missing_files:
            raise RuntimeError(
                "Fichier(s) projet "
                "introuvable(s) : "
                + ", ".join(
                    missing_files
                )
            )

        # ----------------------------------------------------
        # Runtime Python projet
        # ----------------------------------------------------

        python_binary = Path(
            PROJECT_PYTHON
        )

        if not python_binary.exists():
            raise RuntimeError(
                "Binaire Python projet "
                "introuvable : "
                f"{PROJECT_PYTHON}"
            )

        # ----------------------------------------------------
        # Runtime dbt
        # ----------------------------------------------------

        dbt_binary = Path(
            PROJECT_DBT
        )

        if not dbt_binary.exists():
            raise RuntimeError(
                "Binaire dbt introuvable : "
                f"{PROJECT_DBT}"
            )

        # ----------------------------------------------------
        # Imports critiques
        # ----------------------------------------------------

        import_check = """
        import src.ingestion.france_travail_ingestion
        import src.transformation.raw_to_bronze
        import src.transformation.bronze_to_silver

        import src.quality.raw_r2_quality_check
        import src.quality.bronze_r2_quality_check
        import src.quality.silver_r2_quality_check

        import src.loading.load_silver_to_postgres
        import src.quality.silver_postgres_validation_report

        print("PROJECT_RUNTIME_IMPORTS_OK")
        """

        runtime_output = (
            run_project_command(
                [
                    PROJECT_PYTHON,
                    "-c",
                    textwrap.dedent(
                        import_check
                    ),
                ]
            )
        )

        if (
            "PROJECT_RUNTIME_IMPORTS_OK"
            not in runtime_output
        ):
            raise RuntimeError(
                "Le runtime Python projet "
                "n'est pas opérationnel."
            )

        # ----------------------------------------------------
        # dbt disponible
        # ----------------------------------------------------

        dbt_version_output = (
            run_project_command(
                [
                    PROJECT_DBT,
                    "--version",
                ]
            )
        )

        if not dbt_version_output.strip():
            raise RuntimeError(
                "dbt --version "
                "n'a retourné aucun résultat."
            )

        print()
        print(
            "[OK] Runtime Python projet"
        )
        print(
            "[OK] Modules pipeline"
        )
        print(
            "[OK] Loader PostgreSQL"
        )
        print(
            "[OK] Validator PostgreSQL"
        )
        print(
            "[OK] Projet dbt"
        )
        print(
            "[OK] profiles.yml"
        )
        print(
            "[OK] Binaire dbt"
        )
        print()

        print(
            "PREFLIGHT : VALIDE"
        )

        return {
            "statut": "VALIDE",
            "project_root": str(
                PROJECT_ROOT
            ),
            "project_python": (
                PROJECT_PYTHON
            ),
            "project_dbt": (
                PROJECT_DBT
            ),
            "dbt_project_dir": str(
                DBT_PROJECT_DIR
            ),
        }

    # ========================================================
    # 2. CONTEXTE ACQUISITION
    # ========================================================

    @task(
        task_id="create_acquisition_context",
        retries=0,
    )
    def create_acquisition_context(
        preflight_result: dict,
    ) -> dict:
        """
        Crée un batch_id déterministe par DAG Run.

        Un retry du même DAG Run conserve donc
        exactement le même batch_id.
        """

        ensure_equal(
            preflight_result.get(
                "statut"
            ),
            "VALIDE",
            "Le preflight doit être VALIDE.",
        )

        context = (
            get_current_context()
        )

        dag_run = context.get(
            "dag_run"
        )

        if dag_run is not None:
            dag_run_id = str(
                dag_run.run_id
            )
        else:
            dag_run_id = str(
                context.get(
                    "run_id",
                    "unknown",
                )
            )

        logical_date = (
            context.get(
                "logical_date"
            )
        )

        if logical_date is None:
            ingestion_datetime = (
                datetime.now(
                    timezone.utc
                )
            )
        else:
            ingestion_datetime = (
                logical_date.astimezone(
                    timezone.utc
                )
            )

        batch_id = str(
            uuid5(
                NAMESPACE_URL,
                (
                    f"{DAG_ID}"
                    f"|acquisition|"
                    f"{dag_run_id}"
                ),
            )
        )

        result = {
            "batch_id": batch_id,
            "dag_run_id": (
                dag_run_id
            ),
            "ingestion_datetime_utc": (
                ingestion_datetime
                .isoformat()
            ),
        }

        print(
            json.dumps(
                result,
                indent=2,
                ensure_ascii=False,
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
            minutes=1
        ),
    )
    def ingest_raw(
        acquisition_context: dict,
    ) -> dict:
        """
        API France Travail -> Raw R2 immutable.
        """

        result = run_project_python(
            """
            import json
            import sys
            from datetime import datetime

            from src.ingestion.france_travail_ingestion import (
                run_ingestion,
            )

            payload = json.loads(
                sys.stdin.read()
            )

            ingestion_datetime = (
                datetime.fromisoformat(
                    payload[
                        "ingestion_datetime_utc"
                    ].replace(
                        "Z",
                        "+00:00",
                    )
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

            print(
                "__AIRFLOW_RESULT_JSON__="
                + json.dumps(
                    result,
                    ensure_ascii=False,
                    default=str,
                )
            )
            """,
            acquisition_context,
        )

        ensure_equal(
            result.get(
                "batch_id"
            ),
            acquisition_context.get(
                "batch_id"
            ),
            "batch_id ingestion incohérent.",
        )

        raw_object_key = (
            result.get(
                "raw_object_key"
            )
            or result.get(
                "object_key"
            )
        )

        if not raw_object_key:
            raise RuntimeError(
                "L'ingestion n'a retourné "
                "aucune clé Raw R2."
            )

        raw_sha256 = (
            result.get(
                "raw_sha256"
            )
            or result.get(
                "sha256"
            )
        )

        if not raw_sha256:
            raise RuntimeError(
                "SHA-256 Raw absent "
                "du résultat d'ingestion."
            )

        normalized_result = {
            **result,
            "raw_object_key": (
                raw_object_key
            ),
            "raw_sha256": (
                raw_sha256
            ),
            "storage_status": (
                result.get(
                    "storage_status",
                    "UNKNOWN",
                )
            ),
        }

        print()
        print(
            "Ingestion Raw Airflow : OK"
        )

        print(
            json.dumps(
                normalized_result,
                indent=2,
                ensure_ascii=False,
                default=str,
            )
        )

        return normalized_result

    # ========================================================
    # 4. QUALITY GATE RAW
    # ========================================================

    @task(
        task_id="validate_raw",
        retries=0,
    )
    def validate_raw(
        raw_result: dict,
    ) -> dict:
        """
        Quality Gate Raw R2.
        """

        payload = {
            "raw_object_key": (
                raw_result[
                    "raw_object_key"
                ]
            ),
        }

        quality_result = (
            run_project_python(
                """
                import json
                import sys

                from src.quality.raw_r2_quality_check import (
                    validate_raw_object,
                )

                payload = json.loads(
                    sys.stdin.read()
                )

                result = validate_raw_object(
                    raw_object_key=payload[
                        "raw_object_key"
                    ]
                )

                if result is None:
                    result = {}

                result = dict(
                    result
                )

                result[
                    "statut"
                ] = "VALIDE"

                print(
                    "__AIRFLOW_RESULT_JSON__="
                    + json.dumps(
                        result,
                        ensure_ascii=False,
                        default=str,
                    )
                )
                """,
                payload,
            )
        )

        ensure_equal(
            quality_result.get(
                "statut"
            ),
            "VALIDE",
            "Quality Gate Raw invalide.",
        )

        result = {
            **quality_result,
            "statut": "VALIDE",
            "batch_id": (
                raw_result[
                    "batch_id"
                ]
            ),
            "raw_object_key": (
                raw_result[
                    "raw_object_key"
                ]
            ),
            "raw_sha256": (
                raw_result[
                    "raw_sha256"
                ]
            ),
            "nombre_lignes_raw": (
                quality_result.get(
                    "nombre_lignes_raw",
                    raw_result.get(
                        "nombre_offres"
                    ),
                )
            ),
            "storage_status": (
                raw_result.get(
                    "storage_status"
                )
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
        """
        Crée un processing_run_id déterministe
        par DAG Run et batch_id.

        Raw reste identifié uniquement
        par batch_id.

        Bronze et Silver utilisent
        processing_run_id.
        """

        ensure_equal(
            raw_validation_result.get(
                "statut"
            ),
            "VALIDE",
            "Raw doit être VALIDE.",
        )

        context = (
            get_current_context()
        )

        dag_run = context.get(
            "dag_run"
        )

        if dag_run is not None:
            dag_run_id = str(
                dag_run.run_id
            )
        else:
            dag_run_id = str(
                context.get(
                    "run_id",
                    "unknown",
                )
            )

        batch_id = (
            raw_validation_result[
                "batch_id"
            ]
        )

        processing_run_id = str(
            uuid5(
                NAMESPACE_URL,
                (
                    f"{DAG_ID}"
                    f"|processing|"
                    f"{dag_run_id}|"
                    f"{batch_id}"
                ),
            )
        )

        result = {
            "batch_id": (
                batch_id
            ),
            "processing_run_id": (
                processing_run_id
            ),
            "dag_run_id": (
                dag_run_id
            ),
            "raw_object_key": (
                raw_validation_result[
                    "raw_object_key"
                ]
            ),
            "raw_sha256": (
                raw_validation_result[
                    "raw_sha256"
                ]
            ),
        }

        print(
            json.dumps(
                result,
                indent=2,
                ensure_ascii=False,
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
            minutes=1
        ),
    )
    def raw_to_bronze(
        processing_context: dict,
    ) -> dict:
        """
        Raw R2 -> Bronze Parquet R2.
        """

        result = run_project_python(
            """
            import json
            import sys

            from src.transformation.raw_to_bronze import (
                run_bronze_transformation,
            )

            payload = json.loads(
                sys.stdin.read()
            )

            result = (
                run_bronze_transformation(
                    raw_object_key=payload[
                        "raw_object_key"
                    ],
                    processing_run_id=payload[
                        "processing_run_id"
                    ],
                )
            )

            print(
                "__AIRFLOW_RESULT_JSON__="
                + json.dumps(
                    result,
                    ensure_ascii=False,
                    default=str,
                )
            )
            """,
            processing_context,
        )

        ensure_equal(
            result.get(
                "batch_id"
            ),
            processing_context.get(
                "batch_id"
            ),
            "batch_id Bronze incohérent.",
        )

        ensure_equal(
            result.get(
                "processing_run_id"
            ),
            processing_context.get(
                "processing_run_id"
            ),
            (
                "processing_run_id "
                "Bronze incohérent."
            ),
        )

        if not result.get(
            "bronze_object_key"
        ):
            raise RuntimeError(
                "bronze_object_key absent."
            )

        print()
        print(
            "Raw -> Bronze Airflow : OK"
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
        """
        Quality Gate Bronze processing_v2.
        """

        payload = {
            "bronze_object_key": (
                bronze_result[
                    "bronze_object_key"
                ]
            )
        }

        quality_result = (
            run_project_python(
                """
                import json
                import sys

                from src.quality.bronze_r2_quality_check import (
                    validate_bronze_object,
                )

                payload = json.loads(
                    sys.stdin.read()
                )

                result = (
                    validate_bronze_object(
                        bronze_object_key=payload[
                            "bronze_object_key"
                        ]
                    )
                )

                if result is None:
                    result = {}

                result = dict(
                    result
                )

                result[
                    "statut"
                ] = "VALIDE"

                print(
                    "__AIRFLOW_RESULT_JSON__="
                    + json.dumps(
                        result,
                        ensure_ascii=False,
                        default=str,
                    )
                )
                """,
                payload,
            )
        )

        ensure_equal(
            quality_result.get(
                "statut"
            ),
            "VALIDE",
            (
                "Quality Gate Bronze "
                "invalide."
            ),
        )

        result = {
            **bronze_result,
            **quality_result,
            "statut": "VALIDE",
            "batch_id": (
                bronze_result[
                    "batch_id"
                ]
            ),
            "processing_run_id": (
                bronze_result[
                    "processing_run_id"
                ]
            ),
            "bronze_object_key": (
                bronze_result[
                    "bronze_object_key"
                ]
            ),
        }

        print(
            "Bronze Quality Gate : VALIDE"
        )

        return result

    # ========================================================
    # 8. BRONZE -> SILVER
    # ========================================================

    @task(
        task_id="bronze_to_silver",
        retries=2,
        retry_delay=timedelta(
            minutes=1
        ),
    )
    def bronze_to_silver(
        bronze_validation_result: dict,
    ) -> dict:
        """
        Bronze R2 -> Silver Parquet R2.

        Pipeline normal :
        le même processing_run_id est propagé.
        """

        ensure_equal(
            bronze_validation_result.get(
                "statut"
            ),
            "VALIDE",
            "Bronze doit être VALIDE.",
        )

        payload = {
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
        }

        result = run_project_python(
            """
            import json
            import sys

            from src.transformation.bronze_to_silver import (
                run_silver_transformation,
            )

            payload = json.loads(
                sys.stdin.read()
            )

            result = (
                run_silver_transformation(
                    bronze_object_key=payload[
                        "bronze_object_key"
                    ],
                    processing_run_id=payload[
                        "processing_run_id"
                    ],
                )
            )

            print(
                "__AIRFLOW_RESULT_JSON__="
                + json.dumps(
                    result,
                    ensure_ascii=False,
                    default=str,
                )
            )
            """,
            payload,
        )

        ensure_equal(
            result.get(
                "processing_run_id"
            ),
            bronze_validation_result.get(
                "processing_run_id"
            ),
            (
                "processing_run_id "
                "Silver incohérent."
            ),
        )

        if not result.get(
            "silver_object_key"
        ):
            raise RuntimeError(
                "silver_object_key absent."
            )

        print()
        print(
            "Bronze -> Silver Airflow : OK"
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
        """
        Quality Gate complet Silver R2.
        """

        payload = {
            "silver_object_key": (
                silver_result[
                    "silver_object_key"
                ]
            )
        }

        quality_result = (
            run_project_python(
                """
                import json
                import sys

                from src.quality.silver_r2_quality_check import (
                    validate_silver_object,
                )

                payload = json.loads(
                    sys.stdin.read()
                )

                result = (
                    validate_silver_object(
                        silver_object_key=payload[
                            "silver_object_key"
                        ]
                    )
                )

                if result is None:
                    result = {}

                result = dict(
                    result
                )

                result[
                    "statut"
                ] = "VALIDE"

                print(
                    "__AIRFLOW_RESULT_JSON__="
                    + json.dumps(
                        result,
                        ensure_ascii=False,
                        default=str,
                    )
                )
                """,
                payload,
            )
        )

        ensure_equal(
            quality_result.get(
                "statut"
            ),
            "VALIDE",
            (
                "Quality Gate Silver "
                "invalide."
            ),
        )

        result = {
            **silver_result,
            **quality_result,
            "statut": "VALIDE",
            "silver_object_key": (
                silver_result[
                    "silver_object_key"
                ]
            ),
            "silver_sha256": (
                quality_result.get(
                    "silver_sha256"
                )
                or silver_result.get(
                    "silver_sha256"
                )
                or silver_result.get(
                    "sha256"
                )
            ),
        }

        if not result.get(
            "silver_sha256"
        ):
            raise RuntimeError(
                "silver_sha256 absent "
                "après validation Silver."
            )

        print(
            "Silver Quality Gate : VALIDE"
        )

        print(
            json.dumps(
                result,
                indent=2,
                ensure_ascii=False,
                default=str,
            )
        )

        return result

    # ========================================================
    # 10. SILVER -> POSTGRESQL
    # ========================================================

    @task(
        task_id="load_silver_to_postgres",
        retries=2,
        retry_delay=timedelta(
            minutes=1
        ),
    )
    def load_silver_to_postgres(
        silver_validation_result: dict,
    ) -> dict:
        """
        Publie le snapshot Silver courant
        dans PostgreSQL.

        Le loader est idempotent :
        - LOADED ;
        - ALREADY_CURRENT.
        """

        ensure_equal(
            silver_validation_result.get(
                "statut"
            ),
            "VALIDE",
            (
                "Silver doit être VALIDE "
                "avant PostgreSQL."
            ),
        )

        silver_object_key = (
            silver_validation_result[
                "silver_object_key"
            ]
        )

        output = run_project_command(
            [
                PROJECT_PYTHON,
                "-m",
                (
                    "src.loading."
                    "load_silver_to_postgres"
                ),
                "--silver-object-key",
                silver_object_key,
            ]
        )

        if (
            "STATUT CHARGEMENT : "
            "DÉJÀ À JOUR"
            in output
        ):
            storage_status = (
                "ALREADY_CURRENT"
            )

        elif (
            "STATUT CHARGEMENT : VALIDE"
            in output
        ):
            storage_status = (
                "LOADED"
            )

        else:
            raise RuntimeError(
                "Le loader PostgreSQL "
                "n'a retourné aucun statut "
                "reconnu."
            )

        ensure_in(
            storage_status,
            {
                "LOADED",
                "ALREADY_CURRENT",
            },
            (
                "Statut de chargement "
                "PostgreSQL invalide."
            ),
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
                silver_validation_result.get(
                    "nombre_lignes_silver"
                )
            ),
            "table_postgresql": (
                "silver.france_travail_offres"
            ),
        }

        print(
            "Chargement PostgreSQL "
            "Airflow : "
            f"{storage_status}"
        )

        print(
            json.dumps(
                result,
                indent=2,
                ensure_ascii=False,
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
        Compare strictement le Silver R2
        au snapshot PostgreSQL publié.
        """

        ensure_equal(
            postgres_load_result.get(
                "statut"
            ),
            "VALIDE",
            (
                "Chargement PostgreSQL "
                "non valide."
            ),
        )

        silver_object_key = (
            postgres_load_result[
                "silver_object_key"
            ]
        )

        output = run_project_command(
            [
                PROJECT_PYTHON,
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
            "STATUT SILVER -> "
            "POSTGRESQL : VALIDE"
        )

        if expected_status not in output:
            raise RuntimeError(
                "Le Quality Gate "
                "Silver -> PostgreSQL "
                "n'est pas VALIDE."
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
                postgres_load_result.get(
                    "nombre_lignes_silver"
                )
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
            "Silver -> PostgreSQL : "
            "VALIDE"
        )

        print(
            json.dumps(
                result,
                indent=2,
                ensure_ascii=False,
            )
        )

        return result

    # ========================================================
    # 12. DBT BUILD
    # ========================================================

    @task(
        task_id="dbt_build",
        retries=0,
    )
    def dbt_build(
        postgres_validation_result: dict,
    ) -> dict:
        """
        Construit la couche analytique dbt.

        Exécute :
        - seed ;
        - staging ;
        - intermediate ;
        - marts ;
        - tests dbt.

        Le Quality Gate PostgreSQL doit être vert
        avant tout traitement dbt.
        """

        # ----------------------------------------------------
        # Quality Gate parent
        # ----------------------------------------------------

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

        batch_id = (
            postgres_validation_result.get(
                "batch_id"
            )
        )

        processing_run_id = (
            postgres_validation_result.get(
                "processing_run_id"
            )
        )

        silver_object_key = (
            postgres_validation_result.get(
                "silver_object_key"
            )
        )

        silver_sha256 = (
            postgres_validation_result.get(
                "silver_sha256"
            )
        )

        # ----------------------------------------------------
        # Runtime dbt
        # ----------------------------------------------------

        if not Path(
            PROJECT_DBT
        ).exists():
            raise RuntimeError(
                "Binaire dbt introuvable : "
                f"{PROJECT_DBT}"
            )

        if not DBT_PROJECT_FILE.exists():
            raise RuntimeError(
                "dbt_project.yml "
                "introuvable : "
                f"{DBT_PROJECT_FILE}"
            )

        if not DBT_PROFILES_FILE.exists():
            raise RuntimeError(
                "profiles.yml introuvable : "
                f"{DBT_PROFILES_FILE}"
            )

        # ----------------------------------------------------
        # Traçabilité
        # ----------------------------------------------------

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
            "Batch ID              : "
            f"{batch_id}"
        )

        print(
            "Processing Run ID     : "
            f"{processing_run_id}"
        )

        print(
            "Objet Silver          : "
            f"{silver_object_key}"
        )

        print(
            "SHA-256 Silver        : "
            f"{silver_sha256}"
        )

        print(
            "Projet dbt            : "
            f"{DBT_PROJECT_DIR}"
        )

        print(
            "Profiles dbt          : "
            f"{DBT_PROFILES_DIR}"
        )

        print()

        # ----------------------------------------------------
        # dbt build
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # Artefact officiel dbt
        # ----------------------------------------------------

        run_results_path = (
            DBT_PROJECT_DIR
            / "target"
            / "run_results.json"
        )

        if not run_results_path.exists():
            raise RuntimeError(
                "dbt build terminé mais "
                "target/run_results.json "
                "est introuvable."
            )

        with run_results_path.open(
            "r",
            encoding="utf-8",
        ) as file:
            run_results = (
                json.load(
                    file
                )
            )

        results = run_results.get(
            "results",
            [],
        )

        if not isinstance(
            results,
            list,
        ):
            raise RuntimeError(
                "Le champ results de "
                "run_results.json "
                "est invalide."
            )

        if not results:
            raise RuntimeError(
                "run_results.json "
                "ne contient aucun "
                "résultat dbt."
            )

        # ----------------------------------------------------
        # Statuts
        # ----------------------------------------------------

        status_counts: dict[str, int] = {}

        for result in results:

            status = str(
                result.get(
                    "status",
                    "unknown",
                )
            )

            status_counts[
                status
            ] = (
                status_counts.get(
                    status,
                    0,
                )
                + 1
            )

        failed_statuses = {
            "error",
            "fail",
            "failed",
        }

        dbt_failures = sum(
            count
            for (
                status,
                count,
            )
            in status_counts.items()
            if status.lower()
            in failed_statuses
        )

        if dbt_failures > 0:
            raise RuntimeError(
                "dbt build contient "
                f"{dbt_failures} "
                "résultat(s) en échec."
            )

        total_results = len(
            results
        )

        metadata = (
            run_results.get(
                "metadata",
                {},
            )
        )

        invocation_id = (
            metadata.get(
                "invocation_id"
            )
            if isinstance(
                metadata,
                dict,
            )
            else None
        )

        generated_at = (
            metadata.get(
                "generated_at"
            )
            if isinstance(
                metadata,
                dict,
            )
            else None
        )

        # ----------------------------------------------------
        # Résumé
        # ----------------------------------------------------

        print()
        print(
            "-" * 70
        )
        print(
            "RÉSUMÉ DBT"
        )
        print(
            "-" * 70
        )

        print(
            "Nombre de résultats   : "
            f"{total_results}"
        )

        print(
            "Statuts dbt           : "
            f"{status_counts}"
        )

        print(
            "Invocation ID         : "
            f"{invocation_id}"
        )

        print()
        print(
            "STATUT DBT BUILD : VALIDE"
        )

        result = {
            "statut": "VALIDE",
            "dbt_status": "BUILT",
            "batch_id": (
                batch_id
            ),
            "processing_run_id": (
                processing_run_id
            ),
            "silver_object_key": (
                silver_object_key
            ),
            "silver_sha256": (
                silver_sha256
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
            "dbt_total_results": (
                total_results
            ),
            "dbt_status_counts": (
                status_counts
            ),
            "dbt_invocation_id": (
                invocation_id
            ),
            "dbt_generated_at": (
                generated_at
            ),
        }

        print(
            json.dumps(
                result,
                indent=2,
                ensure_ascii=False,
                default=str,
            )
        )

        return result

    # ========================================================
    # Construction du graphe
    # ========================================================

    preflight_result = (
        preflight()
    )

    acquisition_context = (
        create_acquisition_context(
            preflight_result
        )
    )

    raw_result = (
        ingest_raw(
            acquisition_context
        )
    )

    raw_validation_result = (
        validate_raw(
            raw_result
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