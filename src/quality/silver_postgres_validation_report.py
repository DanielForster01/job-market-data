from __future__ import annotations

import argparse
import hashlib
import json
import os
import re

from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine, URL

from src.storage.r2_paths import parse_data_lake_object_key
from src.storage.r2_storage import R2Storage
from src.utils.logger import logger


ROOT_DIR = Path(__file__).resolve().parents[2]

REPORTS_DIR = ROOT_DIR / "reports" / "quality"
DOCS_DIR = ROOT_DIR / "docs" / "quality"

TARGET_SCHEMA = "silver"
TARGET_TABLE = "france_travail_offres"

LOAD_TABLE = "france_travail_offres__load"

AUDIT_SCHEMA = "audit"
AUDIT_TABLE = "pipeline_runs"

LOAD_PIPELINE_NAME = "load_silver_r2_to_postgres"

EXPECTED_SOURCE = "france_travail"
EXPECTED_PATH_VERSION = "processing_v2"
EXPECTED_SILVER_SCHEMA_VERSION = "2.0.0"

EXPECTED_POSTGRES_EXTRA_COLUMNS = {
    "date_chargement",
    "fichier_source_silver",
}


IDENTIFIER_PATTERN = re.compile(
    r"^[a-z_][a-z0-9_]*$"
)


def validate_identifier(
    identifier: str,
) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(
        identifier
    ):
        raise ValueError(
            f"Identifiant SQL invalide : "
            f"{identifier!r}"
        )

    return identifier


def quote_identifier(
    identifier: str,
) -> str:
    validate_identifier(
        identifier
    )

    return f'"{identifier}"'


def sha256_bytes(
    data: bytes,
) -> str:
    return hashlib.sha256(
        data
    ).hexdigest()


def get_postgres_engine() -> Engine:
    env_path = ROOT_DIR / ".env"

    load_dotenv(
        env_path,
        override=True,
    )

    postgres_user = os.getenv(
        "POSTGRES_USER"
    )

    postgres_password = os.getenv(
        "POSTGRES_PASSWORD"
    )

    postgres_db = os.getenv(
        "POSTGRES_DB"
    )

    postgres_host = os.getenv(
        "POSTGRES_HOST",
        "localhost",
    )

    postgres_port = os.getenv(
        "POSTGRES_PORT",
        "5432",
    )

    variables = {
        "POSTGRES_USER": postgres_user,
        "POSTGRES_PASSWORD": postgres_password,
        "POSTGRES_DB": postgres_db,
        "POSTGRES_HOST": postgres_host,
        "POSTGRES_PORT": postgres_port,
    }

    missing_variables = [
        name
        for name, value in variables.items()
        if not value
    ]

    if missing_variables:
        raise EnvironmentError(
            "Variables PostgreSQL manquantes : "
            f"{missing_variables}. "
            "Vérifie le fichier .env."
        )

    connection_url = URL.create(
        drivername="postgresql+psycopg2",
        username=postgres_user,
        password=postgres_password,
        host=postgres_host,
        port=int(postgres_port),
        database=postgres_db,
    )

    return create_engine(
        connection_url,
        pool_pre_ping=True,
    )


def read_silver_from_r2(
    silver_object_key: str,
) -> tuple[
    pd.DataFrame,
    Dict[str, str],
]:
    path_info = parse_data_lake_object_key(
        silver_object_key
    )

    if path_info.get("layer") != "silver":
        raise ValueError(
            "L'objet fourni n'appartient "
            "pas à la couche Silver."
        )

    if (
        path_info.get("source")
        != EXPECTED_SOURCE
    ):
        raise ValueError(
            "Source inattendue dans "
            "le chemin Silver : "
            f"{path_info.get('source')}"
        )

    if (
        path_info.get("path_version")
        != EXPECTED_PATH_VERSION
    ):
        raise ValueError(
            "Le validateur PostgreSQL "
            "accepte uniquement un objet "
            f"Silver {EXPECTED_PATH_VERSION}."
        )

    batch_id = path_info.get(
        "batch_id"
    )

    processing_run_id = path_info.get(
        "processing_run_id"
    )

    if not batch_id:
        raise ValueError(
            "batch_id absent "
            "du chemin Silver."
        )

    if not processing_run_id:
        raise ValueError(
            "processing_run_id absent "
            "du chemin Silver."
        )

    storage = R2Storage()

    silver_bytes = storage.download_bytes(
        silver_object_key
    )

    silver_sha256 = sha256_bytes(
        silver_bytes
    )

    df = pd.read_parquet(
        BytesIO(
            silver_bytes
        ),
        engine="pyarrow",
    )

    context = {
        "silver_object_key": (
            silver_object_key
        ),
        "silver_sha256": (
            silver_sha256
        ),
        "batch_id": str(
            batch_id
        ),
        "processing_run_id": str(
            processing_run_id
        ),
        "path_version": str(
            path_info.get(
                "path_version"
            )
        ),
    }

    return df, context


def validate_silver_r2(
    df: pd.DataFrame,
    context: Dict[str, str],
) -> None:
    if df.empty:
        raise ValueError(
            "Le Silver R2 est vide."
        )

    required_columns = {
        "id_offre",
        "batch_id",
        "processing_run_id",
        "silver_schema_version",
        "objet_source_bronze",
    }

    missing_columns = sorted(
        required_columns
        - set(df.columns)
    )

    if missing_columns:
        raise ValueError(
            "Colonnes obligatoires "
            "absentes du Silver R2 : "
            f"{missing_columns}"
        )

    if df["id_offre"].isna().any():
        raise ValueError(
            "Le Silver R2 contient "
            "des id_offre manquants."
        )

    if not df["id_offre"].is_unique:
        raise ValueError(
            "Le Silver R2 contient "
            "des id_offre dupliqués."
        )

    batch_values = set(
        df["batch_id"]
        .dropna()
        .astype(str)
        .unique()
    )

    if batch_values != {
        context["batch_id"]
    }:
        raise ValueError(
            "batch_id incohérent "
            "dans le Silver R2 : "
            f"{sorted(batch_values)}"
        )

    run_values = set(
        df["processing_run_id"]
        .dropna()
        .astype(str)
        .unique()
    )

    if run_values != {
        context["processing_run_id"]
    }:
        raise ValueError(
            "processing_run_id "
            "incohérent dans le "
            "Silver R2 : "
            f"{sorted(run_values)}"
        )

    schema_values = set(
        df["silver_schema_version"]
        .dropna()
        .astype(str)
        .unique()
    )

    if schema_values != {
        EXPECTED_SILVER_SCHEMA_VERSION
    }:
        raise ValueError(
            "silver_schema_version "
            "inattendue : "
            f"{sorted(schema_values)}"
        )


def fetch_postgres_columns(
    engine: Engine,
) -> List[Dict[str, Any]]:
    with engine.begin() as connection:
        rows = connection.execute(
            text(
                """
                select
                    ordinal_position,
                    column_name,
                    data_type,
                    is_nullable

                from
                    information_schema.columns

                where
                    table_schema =
                        :schema_name
                    and table_name =
                        :table_name

                order by
                    ordinal_position;
                """
            ),
            {
                "schema_name": (
                    TARGET_SCHEMA
                ),
                "table_name": (
                    TARGET_TABLE
                ),
            },
        ).mappings().all()

    return [
        dict(row)
        for row in rows
    ]


def compare_columns(
    silver_columns: List[str],
    postgres_columns: List[str],
) -> Dict[str, Any]:
    silver_set = set(
        silver_columns
    )

    postgres_set = set(
        postgres_columns
    )

    expected_postgres_set = (
        silver_set
        | EXPECTED_POSTGRES_EXTRA_COLUMNS
    )

    missing_in_postgres = sorted(
        silver_set
        - postgres_set
    )

    unexpected_in_postgres = sorted(
        postgres_set
        - expected_postgres_set
    )

    missing_extra_columns = sorted(
        EXPECTED_POSTGRES_EXTRA_COLUMNS
        - postgres_set
    )

    return {
        "colonnes_silver": (
            len(silver_columns)
        ),
        "colonnes_postgresql": (
            len(postgres_columns)
        ),
        "colonnes_postgresql_attendues": (
            len(expected_postgres_set)
        ),
        "colonnes_absentes_postgresql": (
            missing_in_postgres
        ),
        "colonnes_techniques_absentes": (
            missing_extra_columns
        ),
        "colonnes_postgresql_inattendues": (
            unexpected_in_postgres
        ),
        "schema_valide": (
            not missing_in_postgres
            and not missing_extra_columns
            and not unexpected_in_postgres
            and postgres_set
            == expected_postgres_set
        ),
    }


def ensure_postgres_contract(
    column_comparison: Dict[str, Any],
) -> None:
    if not column_comparison[
        "schema_valide"
    ]:
        raise ValueError(
            "Le schéma PostgreSQL ne "
            "correspond pas exactement "
            "au Silver R2 + colonnes "
            "techniques. "
            f"Détail : "
            f"{column_comparison}"
        )


def fetch_postgres_metrics(
    engine: Engine,
) -> Dict[str, Any]:
    with engine.begin() as connection:
        metrics = connection.execute(
            text(
                f"""
                select
                    count(*)
                        as nombre_lignes,

                    count(
                        distinct id_offre
                    )
                        as ids_distincts,

                    count(*) filter (
                        where id_offre
                        is null
                    )
                        as ids_manquants,

                    count(
                        distinct batch_id
                    )
                        as nombre_batchs,

                    min(batch_id)
                        as batch_id,

                    count(
                        distinct
                        processing_run_id
                    )
                        as nombre_processing_runs,

                    min(
                        processing_run_id
                    )
                        as processing_run_id,

                    count(
                        distinct
                        silver_schema_version
                    )
                        as nombre_schema_versions,

                    min(
                        silver_schema_version
                    )
                        as silver_schema_version,

                    count(
                        distinct
                        fichier_source_silver
                    )
                        as nombre_sources_silver,

                    min(
                        fichier_source_silver
                    )
                        as fichier_source_silver,

                    count(*) filter (
                        where
                            date_chargement
                            is null
                    )
                        as dates_chargement_manquantes,

                    count(
                        distinct date_chargement
                    )
                        as nombre_dates_chargement,

                    min(date_chargement)
                        as date_chargement_min,

                    max(date_chargement)
                        as date_chargement_max

                from
                    {TARGET_SCHEMA}.
                    {TARGET_TABLE};
                """
            )
        ).mappings().one()

        duplicates = connection.execute(
            text(
                f"""
                select
                    count(*)

                from (
                    select
                        id_offre

                    from
                        {TARGET_SCHEMA}.
                        {TARGET_TABLE}

                    group by
                        id_offre

                    having
                        count(*) > 1
                ) duplicated;
                """
            )
        ).scalar_one()

    result = dict(
        metrics
    )

    result[
        "ids_dupliques"
    ] = int(
        duplicates
    )

    return result


def fetch_postgres_dataframe(
    engine: Engine,
    silver_columns: List[str],
) -> pd.DataFrame:
    quoted_columns = ", ".join(
        quote_identifier(
            column
        )
        for column in silver_columns
    )

    sql = text(
        f"""
        select
            {quoted_columns}

        from
            {TARGET_SCHEMA}.
            {TARGET_TABLE}

        order by
            id_offre;
        """
    )

    with engine.begin() as connection:
        return pd.read_sql_query(
            sql=sql,
            con=connection,
        )


def fetch_postgres_ids(
    engine: Engine,
) -> pd.Series:
    with engine.begin() as connection:
        df = pd.read_sql_query(
            sql=text(
                f"""
                select
                    id_offre

                from
                    {TARGET_SCHEMA}.
                    {TARGET_TABLE};
                """
            ),
            con=connection,
        )

    return df[
        "id_offre"
    ]


def compare_id_sets(
    silver_ids: pd.Series,
    postgres_ids: pd.Series,
) -> Dict[str, Any]:
    silver_set = set(
        silver_ids
        .dropna()
        .astype(str)
    )

    postgres_set = set(
        postgres_ids
        .dropna()
        .astype(str)
    )

    missing_in_postgres = sorted(
        silver_set
        - postgres_set
    )

    extra_in_postgres = sorted(
        postgres_set
        - silver_set
    )

    return {
        "ids_identiques": (
            not missing_in_postgres
            and not extra_in_postgres
        ),
        "ids_absents_postgresql": (
            len(
                missing_in_postgres
            )
        ),
        "ids_en_trop_postgresql": (
            len(
                extra_in_postgres
            )
        ),
        "exemples_ids_absents": (
            missing_in_postgres[:10]
        ),
        "exemples_ids_en_trop": (
            extra_in_postgres[:10]
        ),
    }


def normalize_transfer_value(
    value: Any,
) -> Any:
    """
    Reproduit la représentation
    relationnelle utilisée par le loader
    pour permettre une comparaison exacte
    Silver R2 -> PostgreSQL.
    """

    if value is None:
        return None

    if isinstance(
        value,
        (
            pd.Timestamp,
            datetime,
        ),
    ):
        if pd.isna(
            value
        ):
            return None

        timestamp = pd.Timestamp(
            value
        )

        if timestamp.tzinfo is None:
            timestamp = (
                timestamp.tz_localize(
                    "UTC"
                )
            )
        else:
            timestamp = (
                timestamp.tz_convert(
                    "UTC"
                )
            )

        return timestamp.isoformat()

    if isinstance(
        value,
        np.ndarray,
    ):
        value = value.tolist()

    if isinstance(
        value,
        set,
    ):
        value = sorted(
            value,
            key=str,
        )

    if isinstance(
        value,
        (
            dict,
            list,
            tuple,
        ),
    ):
        return json.dumps(
            value,
            ensure_ascii=False,
            default=str,
        )

    if isinstance(
        value,
        np.generic,
    ):
        value = value.item()

    try:
        if pd.isna(
            value
        ):
            return None
    except (
        TypeError,
        ValueError,
    ):
        pass

    return value


def row_digest(
    row: pd.Series,
    columns: List[str],
) -> str:
    payload = {
        column: (
            normalize_transfer_value(
                row[column]
            )
        )
        for column in columns
    }

    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(
            ",",
            ":",
        ),
        default=str,
    )

    return hashlib.sha256(
        serialized.encode(
            "utf-8"
        )
    ).hexdigest()


def compute_row_hashes(
    df: pd.DataFrame,
    columns: List[str],
) -> Dict[str, str]:
    if "id_offre" not in columns:
        raise ValueError(
            "id_offre absent de "
            "la liste de colonnes."
        )

    hashes: Dict[
        str,
        str,
    ] = {}

    for _, row in df.iterrows():
        offer_id = str(
            row[
                "id_offre"
            ]
        )

        hashes[
            offer_id
        ] = row_digest(
            row=row,
            columns=columns,
        )

    return hashes


def compare_row_hashes(
    silver_df: pd.DataFrame,
    postgres_df: pd.DataFrame,
    columns: List[str],
) -> Dict[str, Any]:
    silver_hashes = (
        compute_row_hashes(
            df=silver_df,
            columns=columns,
        )
    )

    postgres_hashes = (
        compute_row_hashes(
            df=postgres_df,
            columns=columns,
        )
    )

    common_ids = (
        set(
            silver_hashes
        )
        & set(
            postgres_hashes
        )
    )

    mismatched_ids = sorted(
        offer_id
        for offer_id in common_ids
        if (
            silver_hashes[
                offer_id
            ]
            != postgres_hashes[
                offer_id
            ]
        )
    )

    missing_in_postgres = sorted(
        set(
            silver_hashes
        )
        - set(
            postgres_hashes
        )
    )

    extra_in_postgres = sorted(
        set(
            postgres_hashes
        )
        - set(
            silver_hashes
        )
    )

    return {
        "contenu_identique": (
            not mismatched_ids
            and not missing_in_postgres
            and not extra_in_postgres
        ),
        "nombre_lignes_comparees": (
            len(
                common_ids
            )
        ),
        "nombre_lignes_differentes": (
            len(
                mismatched_ids
            )
        ),
        "exemples_ids_differents": (
            mismatched_ids[:10]
        ),
        "nombre_ids_absents_postgresql": (
            len(
                missing_in_postgres
            )
        ),
        "nombre_ids_en_trop_postgresql": (
            len(
                extra_in_postgres
            )
        ),
    }


def fetch_latest_matching_audit_run(
    engine: Engine,
    context: Dict[str, str],
) -> Dict[str, Any] | None:
    with engine.begin() as connection:
        row = connection.execute(
            text(
                f"""
                select
                    run_id,
                    pipeline_name,
                    status,
                    started_at,
                    finished_at,
                    message,
                    batch_id,
                    processing_run_id,
                    source_object_key,
                    source_sha256,
                    source_row_count,
                    loaded_row_count,
                    silver_schema_version,
                    git_commit_sha,
                    git_branch,
                    git_worktree_dirty

                from
                    {AUDIT_SCHEMA}.
                    {AUDIT_TABLE}

                where
                    pipeline_name =
                        :pipeline_name
                    and batch_id =
                        :batch_id
                    and processing_run_id =
                        :processing_run_id
                    and source_sha256 =
                        :source_sha256

                order by
                    run_id desc

                limit 1;
                """
            ),
            {
                "pipeline_name": (
                    LOAD_PIPELINE_NAME
                ),
                "batch_id": (
                    context[
                        "batch_id"
                    ]
                ),
                "processing_run_id": (
                    context[
                        "processing_run_id"
                    ]
                ),
                "source_sha256": (
                    context[
                        "silver_sha256"
                    ]
                ),
            },
        ).mappings().first()

    if row is None:
        return None

    return dict(
        row
    )


def load_table_exists(
    engine: Engine,
) -> bool:
    return inspect(
        engine
    ).has_table(
        table_name=(
            LOAD_TABLE
        ),
        schema=(
            TARGET_SCHEMA
        ),
    )


def make_json_serializable(
    value: Any,
) -> Any:
    if value is None:
        return None

    if isinstance(
        value,
        pd.Timestamp,
    ):
        if pd.isna(
            value
        ):
            return None

        return value.isoformat()

    if isinstance(
        value,
        datetime,
    ):
        return value.isoformat()

    if isinstance(
        value,
        np.generic,
    ):
        return value.item()

    if isinstance(
        value,
        dict,
    ):
        return {
            key: (
                make_json_serializable(
                    item
                )
            )
            for key, item
            in value.items()
        }

    if isinstance(
        value,
        list,
    ):
        return [
            make_json_serializable(
                item
            )
            for item in value
        ]

    try:
        if pd.isna(
            value
        ):
            return None
    except (
        TypeError,
        ValueError,
    ):
        pass

    return value


def build_checks(
    silver_df: pd.DataFrame,
    context: Dict[str, str],
    postgres_metrics: Dict[str, Any],
    column_comparison: Dict[str, Any],
    id_comparison: Dict[str, Any],
    content_comparison: Dict[str, Any],
    audit_run: Dict[str, Any] | None,
    load_table_present: bool,
) -> Dict[str, bool]:
    expected_rows = len(
        silver_df
    )

    audit_exists = (
        audit_run is not None
    )

    return {
        "silver_non_vide": (
            expected_rows > 0
        ),

        "nombre_lignes_identique": (
            int(
                postgres_metrics[
                    "nombre_lignes"
                ]
            )
            == expected_rows
        ),

        "ids_distincts_identiques": (
            int(
                postgres_metrics[
                    "ids_distincts"
                ]
            )
            == expected_rows
        ),

        "ids_postgresql_non_nuls": (
            int(
                postgres_metrics[
                    "ids_manquants"
                ]
            )
            == 0
        ),

        "ids_postgresql_non_dupliques": (
            int(
                postgres_metrics[
                    "ids_dupliques"
                ]
            )
            == 0
        ),

        "ensembles_ids_identiques": (
            bool(
                id_comparison[
                    "ids_identiques"
                ]
            )
        ),

        "schema_postgresql_valide": (
            bool(
                column_comparison[
                    "schema_valide"
                ]
            )
        ),

        "batch_unique_postgresql": (
            int(
                postgres_metrics[
                    "nombre_batchs"
                ]
            )
            == 1
        ),

        "batch_id_identique": (
            postgres_metrics[
                "batch_id"
            ]
            == context[
                "batch_id"
            ]
        ),

        "processing_run_unique_postgresql": (
            int(
                postgres_metrics[
                    "nombre_processing_runs"
                ]
            )
            == 1
        ),

        "processing_run_id_identique": (
            postgres_metrics[
                "processing_run_id"
            ]
            == context[
                "processing_run_id"
            ]
        ),

        "silver_schema_version_unique": (
            int(
                postgres_metrics[
                    "nombre_schema_versions"
                ]
            )
            == 1
        ),

        "silver_schema_version_identique": (
            postgres_metrics[
                "silver_schema_version"
            ]
            == EXPECTED_SILVER_SCHEMA_VERSION
        ),

        "source_silver_unique_postgresql": (
            int(
                postgres_metrics[
                    "nombre_sources_silver"
                ]
            )
            == 1
        ),

        "source_silver_identique": (
            postgres_metrics[
                "fichier_source_silver"
            ]
            == context[
                "silver_object_key"
            ]
        ),

        "date_chargement_non_nulle": (
            int(
                postgres_metrics[
                    "dates_chargement_manquantes"
                ]
            )
            == 0
        ),

        "date_chargement_unique": (
            int(
                postgres_metrics[
                    "nombre_dates_chargement"
                ]
            )
            == 1
        ),

        "contenu_silver_postgresql_identique": (
            bool(
                content_comparison[
                    "contenu_identique"
                ]
            )
        ),

        "audit_present": (
            audit_exists
        ),

        "audit_success": (
            audit_exists
            and audit_run.get(
                "status"
            )
            == "SUCCESS"
        ),

        "audit_batch_id_identique": (
            audit_exists
            and audit_run.get(
                "batch_id"
            )
            == context[
                "batch_id"
            ]
        ),

        "audit_processing_run_identique": (
            audit_exists
            and audit_run.get(
                "processing_run_id"
            )
            == context[
                "processing_run_id"
            ]
        ),

        "audit_source_object_identique": (
            audit_exists
            and audit_run.get(
                "source_object_key"
            )
            == context[
                "silver_object_key"
            ]
        ),

        "audit_sha256_identique": (
            audit_exists
            and audit_run.get(
                "source_sha256"
            )
            == context[
                "silver_sha256"
            ]
        ),

        "audit_volume_source_identique": (
            audit_exists
            and int(
                audit_run.get(
                    "source_row_count"
                )
                or -1
            )
            == expected_rows
        ),

        "audit_volume_charge_identique": (
            audit_exists
            and int(
                audit_run.get(
                    "loaded_row_count"
                )
                or -1
            )
            == expected_rows
        ),

        "audit_schema_version_identique": (
            audit_exists
            and audit_run.get(
                "silver_schema_version"
            )
            == EXPECTED_SILVER_SCHEMA_VERSION
        ),

        "audit_git_commit_present": (
            audit_exists
            and bool(
                audit_run.get(
                    "git_commit_sha"
                )
            )
        ),

        "audit_git_branch_presente": (
            audit_exists
            and bool(
                audit_run.get(
                    "git_branch"
                )
            )
        ),

        "audit_git_worktree_clean": (
            audit_exists
            and audit_run.get(
                "git_worktree_dirty"
            )
            is False
        ),

        "table_intermediaire_absente": (
            not load_table_present
        ),
    }


def get_global_status(
    checks: Dict[str, bool],
) -> str:
    return (
        "VALIDE"
        if all(
            checks.values()
        )
        else "NON_VALIDE"
    )


def write_json_report(
    report: Dict[str, Any],
) -> Path:
    REPORTS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    timestamp = (
        datetime.now(
            timezone.utc
        ).strftime(
            "%Y-%m-%d_%H-%M-%S"
        )
    )

    output_path = (
        REPORTS_DIR
        / (
            "silver_postgres_"
            "validation_report_"
            f"{timestamp}.json"
        )
    )

    serializable_report = (
        make_json_serializable(
            report
        )
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            serializable_report,
            file,
            ensure_ascii=False,
            indent=4,
            default=str,
        )

    return output_path


def write_markdown_summary(
    report: Dict[str, Any],
) -> Path:
    DOCS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        DOCS_DIR
        / (
            "silver_postgres_"
            "validation_summary.md"
        )
    )

    checks = report[
        "checks"
    ]

    context = report[
        "source_r2"
    ]

    postgres = report[
        "postgresql"
    ]

    column_comparison = report[
        "comparaison_colonnes"
    ]

    content_comparison = report[
        "comparaison_contenu"
    ]

    audit = report[
        "audit_run"
    ]

    def icon(
        value: bool,
    ) -> str:
        return (
            "✅"
            if value
            else "❌"
        )

    lines = [
        "# Validation Silver R2 → PostgreSQL",
        "",
        "## Statut global",
        "",
        f"**Statut : {report['statut_global']}**",
        "",
        "## Source Silver R2",
        "",
        (
            "- Objet : "
            f"`{context['silver_object_key']}`"
        ),
        (
            "- Batch ID : "
            f"`{context['batch_id']}`"
        ),
        (
            "- Processing Run ID : "
            f"`{context['processing_run_id']}`"
        ),
        (
            "- SHA-256 : "
            f"`{context['silver_sha256']}`"
        ),
        (
            "- Schema version : "
            f"`{EXPECTED_SILVER_SCHEMA_VERSION}`"
        ),
        (
            "- Lignes : "
            f"`{context['nombre_lignes']}`"
        ),
        (
            "- Colonnes : "
            f"`{context['nombre_colonnes']}`"
        ),
        "",
        "## PostgreSQL",
        "",
        (
            "- Table : "
            f"`{TARGET_SCHEMA}.{TARGET_TABLE}`"
        ),
        (
            "- Lignes : "
            f"`{postgres['nombre_lignes']}`"
        ),
        (
            "- IDs distincts : "
            f"`{postgres['ids_distincts']}`"
        ),
        (
            "- Batch ID : "
            f"`{postgres['batch_id']}`"
        ),
        (
            "- Processing Run ID : "
            f"`{postgres['processing_run_id']}`"
        ),
        "",
        "## Comparaison du schéma",
        "",
        (
            "- Colonnes Silver R2 : "
            f"`{column_comparison['colonnes_silver']}`"
        ),
        (
            "- Colonnes PostgreSQL : "
            f"`{column_comparison['colonnes_postgresql']}`"
        ),
        (
            "- Colonnes attendues PostgreSQL : "
            f"`{column_comparison['colonnes_postgresql_attendues']}`"
        ),
        (
            "- Colonnes absentes : "
            f"`{column_comparison['colonnes_absentes_postgresql']}`"
        ),
        (
            "- Colonnes inattendues : "
            f"`{column_comparison['colonnes_postgresql_inattendues']}`"
        ),
        "",
        "## Comparaison du contenu",
        "",
        (
            "- Contenu identique : "
            f"`{content_comparison['contenu_identique']}`"
        ),
        (
            "- Lignes comparées : "
            f"`{content_comparison['nombre_lignes_comparees']}`"
        ),
        (
            "- Lignes différentes : "
            f"`{content_comparison['nombre_lignes_differentes']}`"
        ),
        "",
        "## Contrôles",
        "",
        "| Contrôle | Résultat |",
        "|---|---|",
    ]

    for (
        check_name,
        check_value,
    ) in checks.items():
        lines.append(
            f"| {check_name} | "
            f"{icon(check_value)} |"
        )

    lines.extend(
        [
            "",
            "## Audit du chargement",
            "",
        ]
    )

    if audit:
        lines.extend(
            [
                (
                    "- Run ID : "
                    f"`{audit.get('run_id')}`"
                ),
                (
                    "- Statut : "
                    f"`{audit.get('status')}`"
                ),
                (
                    "- Git commit : "
                    f"`{audit.get('git_commit_sha')}`"
                ),
                (
                    "- Git branch : "
                    f"`{audit.get('git_branch')}`"
                ),
                (
                    "- Git worktree dirty : "
                    f"`{audit.get('git_worktree_dirty')}`"
                ),
                (
                    "- Source rows : "
                    f"`{audit.get('source_row_count')}`"
                ),
                (
                    "- Loaded rows : "
                    f"`{audit.get('loaded_row_count')}`"
                ),
            ]
        )
    else:
        lines.append(
            "- Aucun run d'audit correspondant."
        )

    lines.append("")

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        file.write(
            "\n".join(
                lines
            )
        )

    return output_path


def run_validation_report(
    silver_object_key: str,
) -> Dict[str, Any]:
    print()
    print("=" * 70)
    print(
        "VALIDATION SILVER R2 "
        "-> POSTGRESQL"
    )
    print("=" * 70)
    print()

    silver_df, context = (
        read_silver_from_r2(
            silver_object_key
        )
    )

    validate_silver_r2(
        df=silver_df,
        context=context,
    )

    print(
        "[OK] Objet Silver R2 lisible."
    )

    print(
        "[OK] Convention processing_v2 "
        "validée."
    )

    print(
        "[OK] Contrat Silver R2 validé."
    )

    print(
        "[OK] SHA-256 Silver calculé."
    )

    engine = get_postgres_engine()

    postgres_columns_data = (
        fetch_postgres_columns(
            engine
        )
    )

    if not postgres_columns_data:
        raise ValueError(
            "La table "
            f"{TARGET_SCHEMA}."
            f"{TARGET_TABLE} "
            "n'existe pas ou ne contient "
            "aucune colonne."
        )

    postgres_columns = [
        column[
            "column_name"
        ]
        for column
        in postgres_columns_data
    ]

    column_comparison = (
        compare_columns(
            silver_columns=list(
                silver_df.columns
            ),
            postgres_columns=(
                postgres_columns
            ),
        )
    )

    ensure_postgres_contract(
        column_comparison
    )

    print(
        "[OK] Schéma PostgreSQL "
        "strictement conforme."
    )

    postgres_metrics = (
        fetch_postgres_metrics(
            engine
        )
    )

    postgres_ids = (
        fetch_postgres_ids(
            engine
        )
    )

    id_comparison = (
        compare_id_sets(
            silver_ids=(
                silver_df[
                    "id_offre"
                ]
            ),
            postgres_ids=(
                postgres_ids
            ),
        )
    )

    print(
        "[OK] Ensembles d'IDs "
        "comparés."
    )

    postgres_df = (
        fetch_postgres_dataframe(
            engine=engine,
            silver_columns=list(
                silver_df.columns
            ),
        )
    )

    content_comparison = (
        compare_row_hashes(
            silver_df=silver_df,
            postgres_df=postgres_df,
            columns=list(
                silver_df.columns
            ),
        )
    )

    if content_comparison[
        "contenu_identique"
    ]:
        print(
            "[OK] Contenu Silver R2 "
            "et PostgreSQL identique."
        )
    else:
        print(
            "[ERREUR] Différences de "
            "contenu détectées entre "
            "Silver R2 et PostgreSQL."
        )

    audit_run = (
        fetch_latest_matching_audit_run(
            engine=engine,
            context=context,
        )
    )

    load_table_present = (
        load_table_exists(
            engine
        )
    )

    checks = build_checks(
        silver_df=silver_df,
        context=context,
        postgres_metrics=(
            postgres_metrics
        ),
        column_comparison=(
            column_comparison
        ),
        id_comparison=(
            id_comparison
        ),
        content_comparison=(
            content_comparison
        ),
        audit_run=(
            audit_run
        ),
        load_table_present=(
            load_table_present
        ),
    )

    global_status = (
        get_global_status(
            checks
        )
    )

    source_r2 = {
        **context,
        "nombre_lignes": int(
            len(
                silver_df
            )
        ),
        "nombre_colonnes": int(
            len(
                silver_df.columns
            )
        ),
        "ids_distincts": int(
            silver_df[
                "id_offre"
            ].nunique(
                dropna=True
            )
        ),
    }

    report = {
        "date_rapport": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
        "statut_global": (
            global_status
        ),
        "source_r2": (
            source_r2
        ),
        "table_postgresql": (
            f"{TARGET_SCHEMA}."
            f"{TARGET_TABLE}"
        ),
        "postgresql": (
            postgres_metrics
        ),
        "colonnes_postgresql_detail": (
            postgres_columns_data
        ),
        "comparaison_colonnes": (
            column_comparison
        ),
        "comparaison_ids": (
            id_comparison
        ),
        "comparaison_contenu": (
            content_comparison
        ),
        "audit_run": (
            audit_run
        ),
        "checks": (
            checks
        ),
    }

    json_report_path = (
        write_json_report(
            report
        )
    )

    markdown_summary_path = (
        write_markdown_summary(
            report
        )
    )

    print()
    print("-" * 70)
    print("RÉSUMÉ")
    print("-" * 70)

    print(
        f"Objet Silver             : "
        f"{context['silver_object_key']}"
    )

    print(
        f"Batch ID                 : "
        f"{context['batch_id']}"
    )

    print(
        f"Processing Run ID        : "
        f"{context['processing_run_id']}"
    )

    print(
        f"SHA-256 Silver           : "
        f"{context['silver_sha256']}"
    )

    print(
        f"Lignes Silver R2         : "
        f"{len(silver_df)}"
    )

    print(
        f"Lignes PostgreSQL        : "
        f"{postgres_metrics['nombre_lignes']}"
    )

    print(
        f"Colonnes Silver R2       : "
        f"{len(silver_df.columns)}"
    )

    print(
        f"Colonnes PostgreSQL      : "
        f"{len(postgres_columns)}"
    )

    print(
        f"IDs identiques           : "
        f"{id_comparison['ids_identiques']}"
    )

    print(
        f"Contenu identique        : "
        f"{content_comparison['contenu_identique']}"
    )

    print(
        f"Audit run                : "
        f"{audit_run.get('run_id') if audit_run else None}"
    )

    print(
        f"Audit status             : "
        f"{audit_run.get('status') if audit_run else None}"
    )

    print(
        f"Table __load présente    : "
        f"{load_table_present}"
    )

    print(
        f"Rapport JSON             : "
        f"{json_report_path}"
    )

    print(
        f"Synthèse Markdown        : "
        f"{markdown_summary_path}"
    )

    print()
    print(
        "STATUT SILVER -> POSTGRESQL : "
        f"{global_status}"
    )

    if global_status != "VALIDE":
        failed_checks = [
            name
            for name, value
            in checks.items()
            if not value
        ]

        print()
        print(
            "Contrôles en échec :"
        )

        for check in failed_checks:
            print(
                f"  - {check}"
            )

    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Valide le chargement d'un "
            "objet Silver processing_v2 "
            "Cloudflare R2 vers PostgreSQL."
        )
    )

    parser.add_argument(
        "--silver-object-key",
        required=True,
        help=(
            "Clé exacte de l'objet "
            "Silver R2 à comparer à "
            "PostgreSQL."
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    report = run_validation_report(
        silver_object_key=(
            args.silver_object_key
        )
    )

    if (
        report[
            "statut_global"
        ]
        != "VALIDE"
    ):
        raise SystemExit(
            1
        )


if __name__ == "__main__":
    main()