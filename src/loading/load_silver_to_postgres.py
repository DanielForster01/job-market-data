from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict
from uuid import uuid4

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Text,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.types import TypeEngine

from src.utils.logger import logger


ROOT_DIR = Path(__file__).resolve().parents[2]
SILVER_DIR = ROOT_DIR / "data" / "silver"

TARGET_SCHEMA = "silver"
TARGET_TABLE = "france_travail_offres"

AUDIT_SCHEMA = "audit"
AUDIT_TABLE = "pipeline_runs"

PIPELINE_NAME = "load_silver_to_postgres"


REQUIRED_COLUMNS = [
    "id_offre",
    "intitule_offre",
    "date_creation",
    "code_type_contrat",
    "mot_cle_recherche",
    "salaire_renseigne",
    "information_entreprise_disponible",
    "coordonnees_renseignees",
]


DATETIME_COLUMNS = [
    "date_creation",
    "date_actualisation",
    "date_ingestion",
    "date_traitement_silver",
    "date_chargement",
]


BOOLEAN_COLUMNS = [
    "date_actualisation_coherente",
    "coordonnees_renseignees",
    "entreprise_renseignee",
    "description_entreprise_renseignee",
    "information_entreprise_disponible",
    "est_entreprise_adaptee",
    "est_employeur_handi_engage",
    "salaire_renseigne",
    "est_alternance",
    "offre_difficile_a_pourvoir",
    "accessible_travailleur_handicape",
]


INTEGER_COLUMNS = [
    "nombre_postes",
]


FLOAT_COLUMNS = [
    "latitude",
    "longitude",
]


INDEX_COLUMNS = [
    "date_creation",
    "code_type_contrat",
    "code_rome",
    "mot_cle_recherche",
]


BASE_DTYPE_MAPPING: Dict[str, TypeEngine] = {
    # Métadonnées de chargement
    "batch_id": Text(),
    "date_chargement": DateTime(timezone=True),
    "fichier_source_silver": Text(),

    # Identité de l'offre
    "id_offre": Text(),
    "intitule_offre": Text(),
    "description_offre": Text(),

    # Dates
    "date_creation": DateTime(timezone=True),
    "date_actualisation": DateTime(timezone=True),
    "date_actualisation_coherente": Boolean(),

    # Localisation
    "libelle_lieu_travail": Text(),
    "latitude": Float(),
    "longitude": Float(),
    "code_postal": Text(),
    "code_commune": Text(),
    "coordonnees_renseignees": Boolean(),

    # Entreprise
    "nom_entreprise": Text(),
    "description_entreprise": Text(),
    "entreprise_renseignee": Boolean(),
    "description_entreprise_renseignee": Boolean(),
    "information_entreprise_disponible": Boolean(),
    "est_entreprise_adaptee": Boolean(),
    "est_employeur_handi_engage": Boolean(),

    # Contrat
    "code_type_contrat": Text(),
    "libelle_type_contrat": Text(),
    "nature_contrat": Text(),

    # Expérience
    "experience_exigee": Text(),
    "libelle_experience": Text(),
    "commentaire_experience": Text(),

    # Métier ROME
    "code_rome": Text(),
    "libelle_rome": Text(),
    "libelle_appellation": Text(),

    # Salaire
    "salaire_libelle": Text(),
    "salaire_renseigne": Boolean(),
    "periode_salaire": Text(),

    # Alternance et postes
    "est_alternance": Boolean(),
    "nombre_postes": BigInteger(),

    # Temps de travail
    "libelle_duree_travail": Text(),
    "libelle_duree_travail_converti": Text(),

    # Qualification / secteur
    "code_qualification": Text(),
    "libelle_qualification": Text(),
    "code_naf": Text(),
    "code_secteur_activite": Text(),
    "libelle_secteur_activite": Text(),
    "tranche_effectif_etablissement": Text(),

    # Accessibilité / difficulté / déplacement
    "offre_difficile_a_pourvoir": Boolean(),
    "accessible_travailleur_handicape": Boolean(),
    "code_deplacement": Text(),
    "libelle_deplacement": Text(),

    # Compléments
    "complement_exercice": Text(),
    "origine_offre": Text(),
    "url_origine_offre": Text(),
    "horaires_travail": Text(),

    # Ingestion
    "mot_cle_recherche": Text(),
    "date_ingestion": DateTime(timezone=True),
    "fichier_source": Text(),

    # Traçabilité / données semi-structurées
    "enregistrement_brut": Text(),
    "competences_brutes": Text(),
    "formations_brutes": Text(),
    "langues_brutes": Text(),
    "qualites_professionnelles_brutes": Text(),
    "contact_brut": Text(),
    "agence_brute": Text(),
    "permis_bruts": Text(),

    # Traitement Silver
    "date_traitement_silver": DateTime(timezone=True),
}


def get_latest_silver_file() -> Path:
    silver_files = sorted(
        SILVER_DIR.glob("france_travail_jobs_silver_*.parquet"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not silver_files:
        raise FileNotFoundError(
            f"Aucun fichier Silver trouvé dans {SILVER_DIR}. "
            "Lance d'abord la transformation Bronze vers Silver."
        )

    return silver_files[0]


def get_postgres_engine() -> Engine:
    env_path = ROOT_DIR / ".env"
    load_dotenv(env_path, override=True)

    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_db = os.getenv("POSTGRES_DB")
    postgres_host = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")

    missing_variables = [
        variable_name
        for variable_name, variable_value in {
            "POSTGRES_USER": postgres_user,
            "POSTGRES_PASSWORD": postgres_password,
            "POSTGRES_DB": postgres_db,
            "POSTGRES_HOST": postgres_host,
            "POSTGRES_PORT": postgres_port,
        }.items()
        if not variable_value
    ]

    if missing_variables:
        raise EnvironmentError(
            "Variables d'environnement PostgreSQL manquantes : "
            f"{missing_variables}. Vérifie ton fichier .env."
        )

    connection_url = (
        f"postgresql+psycopg2://{postgres_user}:{postgres_password}"
        f"@{postgres_host}:{postgres_port}/{postgres_db}"
    )

    return create_engine(connection_url, pool_pre_ping=True)


def ensure_database_objects(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(f"create schema if not exists {TARGET_SCHEMA};")
        )

        connection.execute(
            text(f"create schema if not exists {AUDIT_SCHEMA};")
        )

        connection.execute(
            text(
                f"""
                create table if not exists {AUDIT_SCHEMA}.{AUDIT_TABLE} (
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


def start_audit_run(
    engine: Engine,
    silver_file_name: str,
    batch_id: str,
) -> int:
    message = (
        f"Début du chargement Silver vers PostgreSQL. "
        f"Fichier={silver_file_name}, batch_id={batch_id}"
    )

    with engine.begin() as connection:
        result = connection.execute(
            text(
                f"""
                insert into {AUDIT_SCHEMA}.{AUDIT_TABLE}
                    (pipeline_name, status, started_at, message)
                values
                    (:pipeline_name, 'STARTED', now(), :message)
                returning run_id;
                """
            ),
            {
                "pipeline_name": PIPELINE_NAME,
                "message": message,
            },
        )

        return int(result.scalar_one())


def finish_audit_run(
    engine: Engine,
    run_id: int,
    status: str,
    message: str,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                update {AUDIT_SCHEMA}.{AUDIT_TABLE}
                set
                    status = :status,
                    finished_at = now(),
                    message = :message
                where run_id = :run_id;
                """
            ),
            {
                "run_id": run_id,
                "status": status,
                "message": message,
            },
        )


def validate_silver_dataframe(df: pd.DataFrame) -> None:
    if df.empty:
        raise ValueError("Le DataFrame Silver est vide.")

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Colonnes obligatoires absentes du fichier Silver : "
            f"{missing_columns}"
        )

    if df["id_offre"].isna().any():
        raise ValueError("La colonne id_offre contient des valeurs nulles.")

    if not df["id_offre"].is_unique:
        duplicated_ids = (
            df.loc[df["id_offre"].duplicated(), "id_offre"]
            .head(10)
            .tolist()
        )

        raise ValueError(
            "La colonne id_offre contient des doublons. "
            f"Exemples : {duplicated_ids}"
        )


def normalize_dataframe_for_postgres(
    df: pd.DataFrame,
    silver_file_name: str,
    batch_id: str,
) -> pd.DataFrame:
    df = df.copy()

    load_timestamp = datetime.now(timezone.utc)

    df["batch_id"] = batch_id
    df["date_chargement"] = load_timestamp
    df["fichier_source_silver"] = silver_file_name

    for column in DATETIME_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column],
                errors="coerce",
                utc=True,
            )

    for column in BOOLEAN_COLUMNS:
        if column in df.columns:
            df[column] = df[column].astype("boolean")

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

    # Conversion finale pour éviter les problèmes psycopg2 avec pd.NA.
    df = df.astype(object).where(pd.notna(df), None)

    return df


def build_dtype_mapping(df: pd.DataFrame) -> Dict[str, TypeEngine]:
    dtype_mapping: Dict[str, TypeEngine] = {}

    for column in df.columns:
        dtype_mapping[column] = BASE_DTYPE_MAPPING.get(column, Text())

    return dtype_mapping


def load_dataframe_to_postgres(
    engine: Engine,
    df: pd.DataFrame,
) -> None:
    dtype_mapping = build_dtype_mapping(df)

    logger.info(
        f"Chargement full refresh vers "
        f"{TARGET_SCHEMA}.{TARGET_TABLE}"
    )

    with engine.begin() as connection:
        df.to_sql(
            name=TARGET_TABLE,
            con=connection,
            schema=TARGET_SCHEMA,
            if_exists="replace",
            index=False,
            dtype=dtype_mapping,
            method="multi",
            chunksize=500,
        )


def add_constraints_and_indexes(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                alter table {TARGET_SCHEMA}.{TARGET_TABLE}
                alter column id_offre set not null;
                """
            )
        )

        connection.execute(
            text(
                f"""
                alter table {TARGET_SCHEMA}.{TARGET_TABLE}
                add constraint pk_{TARGET_TABLE}
                primary key (id_offre);
                """
            )
        )

        for column in INDEX_COLUMNS:
            connection.execute(
                text(
                    f"""
                    create index if not exists
                    idx_{TARGET_TABLE}_{column}
                    on {TARGET_SCHEMA}.{TARGET_TABLE} ({column});
                    """
                )
            )


def validate_loaded_table(
    engine: Engine,
    expected_rows: int,
) -> None:
    with engine.begin() as connection:
        row_count = connection.execute(
            text(
                f"""
                select count(*)
                from {TARGET_SCHEMA}.{TARGET_TABLE};
                """
            )
        ).scalar_one()

        missing_ids = connection.execute(
            text(
                f"""
                select count(*)
                from {TARGET_SCHEMA}.{TARGET_TABLE}
                where id_offre is null;
                """
            )
        ).scalar_one()

        duplicated_ids = connection.execute(
            text(
                f"""
                select count(*)
                from (
                    select id_offre
                    from {TARGET_SCHEMA}.{TARGET_TABLE}
                    group by id_offre
                    having count(*) > 1
                ) duplicated;
                """
            )
        ).scalar_one()

    if row_count != expected_rows:
        raise ValueError(
            "Nombre de lignes incorrect dans PostgreSQL : "
            f"{row_count} au lieu de {expected_rows}."
        )

    if missing_ids != 0:
        raise ValueError(
            f"id_offre manquants dans PostgreSQL : {missing_ids}."
        )

    if duplicated_ids != 0:
        raise ValueError(
            f"id_offre dupliqués dans PostgreSQL : {duplicated_ids}."
        )


def load_silver_to_postgres() -> int:
    batch_id = str(uuid4())
    audit_run_id: int | None = None

    silver_file = get_latest_silver_file()

    logger.info(f"Fichier Silver sélectionné : {silver_file}")
    print(f"Fichier Silver sélectionné : {silver_file.name}")

    engine = get_postgres_engine()
    ensure_database_objects(engine)

    try:
        audit_run_id = start_audit_run(
            engine=engine,
            silver_file_name=silver_file.name,
            batch_id=batch_id,
        )

        df = pd.read_parquet(silver_file)

        logger.info(
            f"Lecture Silver terminée : "
            f"{len(df)} lignes, {len(df.columns)} colonnes"
        )

        print(f"Lignes lues depuis Silver : {len(df)}")
        print(f"Colonnes lues depuis Silver : {len(df.columns)}")

        validate_silver_dataframe(df)

        df = normalize_dataframe_for_postgres(
            df=df,
            silver_file_name=silver_file.name,
            batch_id=batch_id,
        )

        load_dataframe_to_postgres(
            engine=engine,
            df=df,
        )

        add_constraints_and_indexes(engine)

        validate_loaded_table(
            engine=engine,
            expected_rows=len(df),
        )

        success_message = (
            f"Chargement Silver terminé avec succès. "
            f"Table={TARGET_SCHEMA}.{TARGET_TABLE}, "
            f"fichier={silver_file.name}, "
            f"lignes={len(df)}, "
            f"colonnes={len(df.columns)}, "
            f"batch_id={batch_id}"
        )

        finish_audit_run(
            engine=engine,
            run_id=audit_run_id,
            status="SUCCESS",
            message=success_message,
        )

        logger.info(success_message)

        print(success_message)

        return len(df)

    except Exception as error:
        error_message = (
            f"Échec du chargement Silver vers PostgreSQL. "
            f"Fichier={silver_file.name}, "
            f"batch_id={batch_id}, "
            f"erreur={error}"
        )

        logger.exception(error_message)

        if audit_run_id is not None:
            finish_audit_run(
                engine=engine,
                run_id=audit_run_id,
                status="FAILED",
                message=error_message,
            )

        print(error_message, file=sys.stderr)

        raise


if __name__ == "__main__":
    load_silver_to_postgres()