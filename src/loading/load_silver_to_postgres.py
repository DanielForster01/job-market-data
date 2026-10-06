from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys

from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Dict

import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Text,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.engine import Engine, URL
from sqlalchemy.types import TypeEngine

from src.storage.r2_paths import parse_data_lake_object_key
from src.storage.r2_storage import R2Storage
from src.utils.logger import logger
from src.utils.run_context import (
    get_git_branch,
    get_git_commit_sha,
    is_git_worktree_dirty,
)


ROOT_DIR = Path(__file__).resolve().parents[2]

TARGET_SCHEMA = "silver"
TARGET_TABLE = "france_travail_offres"
LOAD_TABLE = "france_travail_offres__load"

AUDIT_SCHEMA = "audit"
AUDIT_TABLE = "pipeline_runs"

PIPELINE_NAME = "load_silver_r2_to_postgres"

EXPECTED_SOURCE = "france_travail"
EXPECTED_PATH_VERSION = "processing_v2"
EXPECTED_SILVER_SCHEMA_VERSION = "2.0.0"


REQUIRED_COLUMNS = [
    "id_offre",
    "batch_id",
    "processing_run_id",
    "silver_schema_version",
    "intitule_offre",
    "date_creation",
    "code_type_contrat",
    "mot_cle_recherche",
    "salaire_renseigne",
    "information_entreprise_disponible",
    "coordonnees_renseignees",
    "localisation_renseignee",
    "objet_source_bronze",
]


DATETIME_COLUMNS = {
    "date_creation",
    "date_actualisation",
    "date_ingestion",
    "date_traitement_silver",
    "date_chargement",
}


BOOLEAN_COLUMNS = {
    "date_actualisation_coherente",
    "coordonnees_renseignees",
    "localisation_renseignee",
    "entreprise_renseignee",
    "description_entreprise_renseignee",
    "information_entreprise_disponible",
    "est_entreprise_adaptee",
    "est_employeur_handi_engage",
    "salaire_renseigne",
    "est_alternance",
    "offre_difficile_a_pourvoir",
    "accessible_travailleur_handicape",
}


INTEGER_COLUMNS = {
    "nombre_postes",
}


FLOAT_COLUMNS = {
    "latitude",
    "longitude",
}


INDEX_COLUMNS = [
    "date_creation",
    "code_type_contrat",
    "code_rome",
    "mot_cle_recherche",
    "batch_id",
    "processing_run_id",
]


IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def validate_identifier(identifier: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(
            f"Identifiant SQL non autorisé : {identifier!r}"
        )

    return identifier


def quote_identifier(identifier: str) -> str:
    validate_identifier(identifier)
    return f'"{identifier}"'


def get_postgres_engine() -> Engine:
    env_path = ROOT_DIR / ".env"

    load_dotenv(
        env_path,
        override=True,
    )

    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_db = os.getenv("POSTGRES_DB")
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
        variable_name
        for variable_name, variable_value in variables.items()
        if not variable_value
    ]

    if missing_variables:
        raise EnvironmentError(
            "Variables PostgreSQL manquantes : "
            f"{missing_variables}. Vérifie le fichier .env."
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
) -> tuple[pd.DataFrame, Dict[str, str]]:
    path_info = parse_data_lake_object_key(
        silver_object_key
    )

    if path_info.get("layer") != "silver":
        raise ValueError(
            "L'objet R2 fourni n'appartient pas à la couche Silver."
        )

    if path_info.get("source") != EXPECTED_SOURCE:
        raise ValueError(
            "Source R2 inattendue : "
            f"{path_info.get('source')}"
        )

    if (
        path_info.get("path_version")
        != EXPECTED_PATH_VERSION
    ):
        raise ValueError(
            "Le loader PostgreSQL accepte uniquement "
            f"les chemins {EXPECTED_PATH_VERSION}."
        )

    batch_id = path_info.get("batch_id")
    processing_run_id = path_info.get(
        "processing_run_id"
    )

    if not batch_id:
        raise ValueError(
            "batch_id absent du chemin Silver."
        )

    if not processing_run_id:
        raise ValueError(
            "processing_run_id absent du chemin Silver."
        )

    storage = R2Storage()

    silver_bytes = storage.download_bytes(
        silver_object_key
    )

    silver_sha256 = sha256_bytes(
        silver_bytes
    )

    df = pd.read_parquet(
        BytesIO(silver_bytes),
        engine="pyarrow",
    )

    context = {
        "silver_object_key": silver_object_key,
        "silver_sha256": silver_sha256,
        "batch_id": str(batch_id),
        "processing_run_id": str(processing_run_id),
        "path_version": str(
            path_info.get("path_version")
        ),
    }

    return df, context


def validate_silver_dataframe(
    df: pd.DataFrame,
    context: Dict[str, str],
) -> None:
    if df.empty:
        raise ValueError(
            "Le DataFrame Silver est vide."
        )

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Colonnes Silver obligatoires absentes : "
            f"{missing_columns}"
        )

    if df["id_offre"].isna().any():
        raise ValueError(
            "id_offre contient des valeurs nulles."
        )

    if not df["id_offre"].is_unique:
        duplicated_ids = (
            df.loc[
                df["id_offre"].duplicated(
                    keep=False
                ),
                "id_offre",
            ]
            .astype(str)
            .head(10)
            .tolist()
        )

        raise ValueError(
            "Des id_offre sont dupliqués dans Silver. "
            f"Exemples : {duplicated_ids}"
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
            "batch_id incohérent entre le chemin R2 "
            "et les lignes Silver : "
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
            "processing_run_id incohérent entre "
            "le chemin R2 et les lignes Silver : "
            f"{sorted(run_values)}"
        )

    schema_versions = set(
        df["silver_schema_version"]
        .dropna()
        .astype(str)
        .unique()
    )

    if schema_versions != {
        EXPECTED_SILVER_SCHEMA_VERSION
    }:
        raise ValueError(
            "silver_schema_version inattendue : "
            f"{sorted(schema_versions)}"
        )


def normalize_dataframe_for_postgres(
    df: pd.DataFrame,
    silver_object_key: str,
) -> pd.DataFrame:
    df = df.copy()

    df["date_chargement"] = datetime.now(
        timezone.utc
    )

    # Compatibilité avec les modèles dbt existants.
    # La valeur contient désormais la clé R2 complète,
    # ce qui améliore le lineage.
    df["fichier_source_silver"] = (
        silver_object_key
    )

    for column in DATETIME_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column],
                errors="coerce",
                utc=True,
            )

    for column in BOOLEAN_COLUMNS:
        if column in df.columns:
            df[column] = df[column].astype(
                "boolean"
            )

    for column in INTEGER_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            ).astype("Int64")

    for column in FLOAT_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            )

    df = df.astype(object).where(
        pd.notna(df),
        None,
    )

    return df


def sqlalchemy_type_for_column(
    column: str,
) -> TypeEngine:
    if column in DATETIME_COLUMNS:
        return DateTime(timezone=True)

    if column in BOOLEAN_COLUMNS:
        return Boolean()

    if column in INTEGER_COLUMNS:
        return BigInteger()

    if column in FLOAT_COLUMNS:
        return Float()

    return Text()


def postgres_type_for_column(
    column: str,
) -> str:
    if column in DATETIME_COLUMNS:
        return "timestamptz"

    if column in BOOLEAN_COLUMNS:
        return "boolean"

    if column in INTEGER_COLUMNS:
        return "bigint"

    if column in FLOAT_COLUMNS:
        return "double precision"

    return "text"


def build_dtype_mapping(
    df: pd.DataFrame,
) -> Dict[str, TypeEngine]:
    return {
        column: sqlalchemy_type_for_column(
            column
        )
        for column in df.columns
    }


def ensure_audit_objects(
    engine: Engine,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                create schema if not exists
                    {AUDIT_SCHEMA};
                """
            )
        )

        connection.execute(
            text(
                f"""
                create table if not exists
                    {AUDIT_SCHEMA}.{AUDIT_TABLE}
                (
                    run_id bigserial primary key,
                    pipeline_name text not null,
                    status text not null,
                    started_at timestamptz default now(),
                    finished_at timestamptz,
                    message text
                );
                """
            )
        )

        additional_columns = {
            "batch_id": "text",
            "processing_run_id": "text",
            "source_object_key": "text",
            "source_sha256": "text",
            "source_row_count": "bigint",
            "loaded_row_count": "bigint",
            "silver_schema_version": "text",
            "git_commit_sha": "text",
            "git_branch": "text",
            "git_worktree_dirty": "boolean",
        }

        for column, sql_type in (
            additional_columns.items()
        ):
            validate_identifier(column)

            connection.execute(
                text(
                    f"""
                    alter table
                        {AUDIT_SCHEMA}.{AUDIT_TABLE}
                    add column if not exists
                        {quote_identifier(column)}
                        {sql_type};
                    """
                )
            )


def start_audit_run(
    engine: Engine,
    context: Dict[str, str],
    source_row_count: int,
    git_commit_sha: str,
    git_branch: str,
    git_worktree_dirty: bool,
) -> int:
    with engine.begin() as connection:
        result = connection.execute(
            text(
                f"""
                insert into
                    {AUDIT_SCHEMA}.{AUDIT_TABLE}
                (
                    pipeline_name,
                    status,
                    started_at,
                    batch_id,
                    processing_run_id,
                    source_object_key,
                    source_sha256,
                    source_row_count,
                    silver_schema_version,
                    git_commit_sha,
                    git_branch,
                    git_worktree_dirty,
                    message
                )
                values
                (
                    :pipeline_name,
                    'STARTED',
                    now(),
                    :batch_id,
                    :processing_run_id,
                    :source_object_key,
                    :source_sha256,
                    :source_row_count,
                    :silver_schema_version,
                    :git_commit_sha,
                    :git_branch,
                    :git_worktree_dirty,
                    :message
                )
                returning run_id;
                """
            ),
            {
                "pipeline_name": PIPELINE_NAME,
                "batch_id": context["batch_id"],
                "processing_run_id": (
                    context[
                        "processing_run_id"
                    ]
                ),
                "source_object_key": (
                    context[
                        "silver_object_key"
                    ]
                ),
                "source_sha256": (
                    context[
                        "silver_sha256"
                    ]
                ),
                "source_row_count": (
                    source_row_count
                ),
                "silver_schema_version": (
                    EXPECTED_SILVER_SCHEMA_VERSION
                ),
                "git_commit_sha": git_commit_sha,
                "git_branch": git_branch,
                "git_worktree_dirty": (
                    git_worktree_dirty
                ),
                "message": (
                    "Début du chargement "
                    "Silver R2 vers PostgreSQL."
                ),
            },
        )

        return int(
            result.scalar_one()
        )


def finish_audit_run(
    engine: Engine,
    run_id: int,
    status: str,
    loaded_row_count: int | None,
    message: str,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                update
                    {AUDIT_SCHEMA}.{AUDIT_TABLE}
                set
                    status = :status,
                    finished_at = now(),
                    loaded_row_count =
                        :loaded_row_count,
                    message = :message
                where run_id = :run_id;
                """
            ),
            {
                "run_id": run_id,
                "status": status,
                "loaded_row_count": (
                    loaded_row_count
                ),
                "message": message,
            },
        )


def table_exists(
    engine: Engine,
    schema: str,
    table: str,
) -> bool:
    return inspect(engine).has_table(
        table_name=table,
        schema=schema,
    )


def get_table_columns(
    engine: Engine,
    schema: str,
    table: str,
) -> set[str]:
    inspector = inspect(engine)

    if not inspector.has_table(
        table_name=table,
        schema=schema,
    ):
        return set()

    return {
        column["name"]
        for column in inspector.get_columns(
            table_name=table,
            schema=schema,
        )
    }


def previous_success_exists(
    engine: Engine,
    context: Dict[str, str],
) -> bool:
    with engine.begin() as connection:
        result = connection.execute(
            text(
                f"""
                select exists (
                    select 1
                    from
                        {AUDIT_SCHEMA}.{AUDIT_TABLE}
                    where
                        pipeline_name =
                            :pipeline_name
                        and status = 'SUCCESS'
                        and batch_id =
                            :batch_id
                        and processing_run_id =
                            :processing_run_id
                        and source_sha256 =
                            :source_sha256
                );
                """
            ),
            {
                "pipeline_name": PIPELINE_NAME,
                "batch_id": context["batch_id"],
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
        )

        return bool(
            result.scalar_one()
        )


def current_snapshot_matches(
    engine: Engine,
    context: Dict[str, str],
    expected_rows: int,
) -> bool:
    if not table_exists(
        engine,
        TARGET_SCHEMA,
        TARGET_TABLE,
    ):
        return False

    columns = get_table_columns(
        engine,
        TARGET_SCHEMA,
        TARGET_TABLE,
    )

    required = {
        "batch_id",
        "processing_run_id",
        "id_offre",
    }

    if not required.issubset(columns):
        return False

    with engine.begin() as connection:
        result = connection.execute(
            text(
                f"""
                select
                    count(*) as row_count,
                    count(distinct batch_id)
                        as batch_count,
                    min(batch_id)
                        as batch_id,
                    count(
                        distinct processing_run_id
                    ) as run_count,
                    min(processing_run_id)
                        as processing_run_id
                from
                    {TARGET_SCHEMA}.{TARGET_TABLE};
                """
            )
        ).mappings().one()

    return (
        int(result["row_count"])
        == expected_rows
        and int(result["batch_count"]) == 1
        and result["batch_id"]
        == context["batch_id"]
        and int(result["run_count"]) == 1
        and result["processing_run_id"]
        == context["processing_run_id"]
    )


def is_already_loaded(
    engine: Engine,
    context: Dict[str, str],
    expected_rows: int,
) -> bool:
    return (
        previous_success_exists(
            engine,
            context,
        )
        and current_snapshot_matches(
            engine,
            context,
            expected_rows,
        )
    )


def load_dataframe_to_load_table(
    engine: Engine,
    df: pd.DataFrame,
) -> None:
    dtype_mapping = build_dtype_mapping(
        df
    )

    with engine.begin() as connection:
        df.to_sql(
            name=LOAD_TABLE,
            con=connection,
            schema=TARGET_SCHEMA,
            if_exists="replace",
            index=False,
            dtype=dtype_mapping,
            method="multi",
            chunksize=500,
        )


def ensure_target_structure(
    connection,
    df: pd.DataFrame,
) -> None:
    connection.execute(
        text(
            f"""
            create schema if not exists
                {TARGET_SCHEMA};
            """
        )
    )

    target_exists = connection.execute(
        text(
            """
            select exists (
                select 1
                from information_schema.tables
                where table_schema = :schema_name
                  and table_name = :table_name
            );
            """
        ),
        {
            "schema_name": TARGET_SCHEMA,
            "table_name": TARGET_TABLE,
        },
    ).scalar_one()

    if not target_exists:
        connection.execute(
            text(
                f"""
                create table
                    {TARGET_SCHEMA}.{TARGET_TABLE}
                (
                    like
                    {TARGET_SCHEMA}.{LOAD_TABLE}
                    including defaults
                );
                """
            )
        )

    current_columns = {
        row[0]
        for row in connection.execute(
            text(
                """
                select column_name
                from information_schema.columns
                where table_schema = :schema_name
                  and table_name = :table_name;
                """
            ),
            {
                "schema_name": TARGET_SCHEMA,
                "table_name": TARGET_TABLE,
            },
        )
    }

    for column in df.columns:
        validate_identifier(column)

        if column in current_columns:
            continue

        sql_type = postgres_type_for_column(
            column
        )

        connection.execute(
            text(
                f"""
                alter table
                    {TARGET_SCHEMA}.{TARGET_TABLE}
                add column
                    {quote_identifier(column)}
                    {sql_type};
                """
            )
        )


def ensure_primary_key_and_indexes(
    connection,
) -> None:
    connection.execute(
        text(
            f"""
            alter table
                {TARGET_SCHEMA}.{TARGET_TABLE}
            alter column id_offre
            set not null;
            """
        )
    )

    primary_key_exists = connection.execute(
        text(
            """
            select exists (
                select 1
                from pg_constraint c
                join pg_class t
                  on t.oid = c.conrelid
                join pg_namespace n
                  on n.oid = t.relnamespace
                where
                    n.nspname = :schema_name
                    and t.relname = :table_name
                    and c.contype = 'p'
            );
            """
        ),
        {
            "schema_name": TARGET_SCHEMA,
            "table_name": TARGET_TABLE,
        },
    ).scalar_one()

    if not primary_key_exists:
        connection.execute(
            text(
                f"""
                alter table
                    {TARGET_SCHEMA}.{TARGET_TABLE}
                add constraint
                    pk_{TARGET_TABLE}
                primary key (id_offre);
                """
            )
        )

    for column in INDEX_COLUMNS:
        validate_identifier(column)

        connection.execute(
            text(
                f"""
                create index if not exists
                    idx_{TARGET_TABLE}_{column}
                on
                    {TARGET_SCHEMA}.{TARGET_TABLE}
                    ({quote_identifier(column)});
                """
            )
        )


def replace_current_snapshot(
    engine: Engine,
    df: pd.DataFrame,
    context: Dict[str, str],
) -> None:
    quoted_columns = ", ".join(
        quote_identifier(column)
        for column in df.columns
    )

    with engine.begin() as connection:
        ensure_target_structure(
            connection,
            df,
        )

        connection.execute(
            text(
                f"""
                truncate table
                    {TARGET_SCHEMA}.{TARGET_TABLE};
                """
            )
        )

        connection.execute(
            text(
                f"""
                insert into
                    {TARGET_SCHEMA}.{TARGET_TABLE}
                    ({quoted_columns})
                select
                    {quoted_columns}
                from
                    {TARGET_SCHEMA}.{LOAD_TABLE};
                """
            )
        )

        ensure_primary_key_and_indexes(
            connection
        )

        metrics = connection.execute(
            text(
                f"""
                select
                    count(*) as row_count,
                    count(distinct id_offre)
                        as distinct_ids,
                    count(*) filter (
                        where id_offre is null
                    ) as missing_ids,
                    count(
                        distinct batch_id
                    ) as batch_count,
                    min(batch_id)
                        as batch_id,
                    count(
                        distinct processing_run_id
                    ) as run_count,
                    min(processing_run_id)
                        as processing_run_id
                from
                    {TARGET_SCHEMA}.{TARGET_TABLE};
                """
            )
        ).mappings().one()

        if int(metrics["row_count"]) != len(df):
            raise ValueError(
                "Nombre de lignes PostgreSQL "
                "incorrect après chargement : "
                f"{metrics['row_count']} "
                f"au lieu de {len(df)}."
            )

        if int(
            metrics["distinct_ids"]
        ) != len(df):
            raise ValueError(
                "Le nombre d'IDs distincts "
                "PostgreSQL est incorrect."
            )

        if int(metrics["missing_ids"]) != 0:
            raise ValueError(
                "PostgreSQL contient des "
                "id_offre manquants."
            )

        if (
            int(metrics["batch_count"]) != 1
            or metrics["batch_id"]
            != context["batch_id"]
        ):
            raise ValueError(
                "batch_id PostgreSQL "
                "incohérent."
            )

        if (
            int(metrics["run_count"]) != 1
            or metrics[
                "processing_run_id"
            ]
            != context[
                "processing_run_id"
            ]
        ):
            raise ValueError(
                "processing_run_id PostgreSQL "
                "incohérent."
            )


def drop_load_table(
    engine: Engine,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                drop table if exists
                    {TARGET_SCHEMA}.{LOAD_TABLE};
                """
            )
        )


def load_silver_to_postgres(
    silver_object_key: str,
) -> int:
    audit_run_id: int | None = None

    git_commit_sha = get_git_commit_sha()
    git_branch = get_git_branch()
    git_worktree_dirty = (
        is_git_worktree_dirty()
    )

    df, context = read_silver_from_r2(
        silver_object_key
    )

    validate_silver_dataframe(
        df,
        context,
    )

    print()
    print("=" * 70)
    print("CHARGEMENT SILVER R2 -> POSTGRESQL")
    print("=" * 70)
    print()
    print(
        f"Objet Silver          : "
        f"{context['silver_object_key']}"
    )
    print(
        f"Batch ID              : "
        f"{context['batch_id']}"
    )
    print(
        f"Processing Run ID     : "
        f"{context['processing_run_id']}"
    )
    print(
        f"Silver schema version : "
        f"{EXPECTED_SILVER_SCHEMA_VERSION}"
    )
    print(
        f"Lignes Silver         : "
        f"{len(df)}"
    )
    print(
        f"Colonnes Silver       : "
        f"{len(df.columns)}"
    )
    print(
        f"SHA-256 Silver        : "
        f"{context['silver_sha256']}"
    )
    print(
        f"Git branch            : "
        f"{git_branch}"
    )
    print(
        f"Git commit            : "
        f"{git_commit_sha}"
    )
    print(
        f"Git worktree dirty    : "
        f"{git_worktree_dirty}"
    )
    print()

    engine = get_postgres_engine()

    ensure_audit_objects(
        engine
    )

    try:
        audit_run_id = start_audit_run(
            engine=engine,
            context=context,
            source_row_count=len(df),
            git_commit_sha=git_commit_sha,
            git_branch=git_branch,
            git_worktree_dirty=(
                git_worktree_dirty
            ),
        )

        if is_already_loaded(
            engine=engine,
            context=context,
            expected_rows=len(df),
        ):
            message = (
                "Snapshot Silver déjà chargé : "
                "aucune modification PostgreSQL "
                "nécessaire. "
                f"batch_id={context['batch_id']}, "
                "processing_run_id="
                f"{context['processing_run_id']}, "
                f"lignes={len(df)}"
            )

            finish_audit_run(
                engine=engine,
                run_id=audit_run_id,
                status="SUCCESS",
                loaded_row_count=len(df),
                message=message,
            )

            print(
                "[OK] Chargement idempotent : "
                "snapshot déjà présent."
            )
            print(
                "[OK] Aucune donnée "
                "PostgreSQL modifiée."
            )
            print()
            print(
                "STATUT CHARGEMENT : "
                "DÉJÀ À JOUR"
            )

            return len(df)

        df_postgres = (
            normalize_dataframe_for_postgres(
                df=df,
                silver_object_key=(
                    silver_object_key
                ),
            )
        )

        load_dataframe_to_load_table(
            engine=engine,
            df=df_postgres,
        )

        replace_current_snapshot(
            engine=engine,
            df=df_postgres,
            context=context,
        )

        success_message = (
            "Chargement Silver R2 vers "
            "PostgreSQL terminé avec succès. "
            f"table={TARGET_SCHEMA}."
            f"{TARGET_TABLE}, "
            f"batch_id={context['batch_id']}, "
            "processing_run_id="
            f"{context['processing_run_id']}, "
            f"lignes={len(df_postgres)}, "
            "source_sha256="
            f"{context['silver_sha256']}"
        )

        finish_audit_run(
            engine=engine,
            run_id=audit_run_id,
            status="SUCCESS",
            loaded_row_count=len(
                df_postgres
            ),
            message=success_message,
        )

        print(
            "[OK] Silver R2 téléchargé."
        )
        print(
            "[OK] Contrat Silver validé."
        )
        print(
            "[OK] Table de chargement "
            "préparée."
        )
        print(
            "[OK] Snapshot PostgreSQL "
            "remplacé atomiquement."
        )
        print(
            "[OK] IDs PostgreSQL validés."
        )
        print(
            "[OK] batch_id PostgreSQL "
            "validé."
        )
        print(
            "[OK] processing_run_id "
            "PostgreSQL validé."
        )
        print()
        print(
            f"Lignes PostgreSQL      : "
            f"{len(df_postgres)}"
        )
        print(
            f"Table PostgreSQL       : "
            f"{TARGET_SCHEMA}.{TARGET_TABLE}"
        )
        print()
        print(
            "STATUT CHARGEMENT : VALIDE"
        )

        return len(df_postgres)

    except Exception as error:
        error_message = (
            "Échec du chargement "
            "Silver R2 vers PostgreSQL. "
            f"objet={silver_object_key}, "
            f"erreur={error}"
        )

        logger.exception(
            error_message
        )

        if audit_run_id is not None:
            finish_audit_run(
                engine=engine,
                run_id=audit_run_id,
                status="FAILED",
                loaded_row_count=None,
                message=error_message,
            )

        print(
            error_message,
            file=sys.stderr,
        )

        raise

    finally:
        try:
            drop_load_table(
                engine
            )
        except Exception:
            logger.exception(
                "Impossible de supprimer "
                "la table temporaire "
                f"{TARGET_SCHEMA}."
                f"{LOAD_TABLE}."
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Charge explicitement un objet "
            "Silver processing_v2 depuis "
            "Cloudflare R2 vers PostgreSQL."
        )
    )

    parser.add_argument(
        "--silver-object-key",
        required=True,
        help=(
            "Clé complète de l'objet "
            "Silver R2 à charger."
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    load_silver_to_postgres(
        silver_object_key=(
            args.silver_object_key
        )
    )


if __name__ == "__main__":
    main()