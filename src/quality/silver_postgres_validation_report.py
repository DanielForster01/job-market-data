from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.utils.logger import logger


ROOT_DIR = Path(__file__).resolve().parents[2]

SILVER_DIR = ROOT_DIR / "data" / "silver"
REPORTS_DIR = ROOT_DIR / "reports" / "quality"
DOCS_DIR = ROOT_DIR / "docs" / "quality"

TARGET_SCHEMA = "silver"
TARGET_TABLE = "france_travail_offres"

AUDIT_SCHEMA = "audit"
AUDIT_TABLE = "pipeline_runs"

LOAD_PIPELINE_NAME = "load_silver_to_postgres"

EXPECTED_POSTGRES_EXTRA_COLUMNS = [
    "batch_id",
    "date_chargement",
    "fichier_source_silver",
]


def get_latest_silver_file() -> Path:
    silver_files = sorted(
        SILVER_DIR.glob("france_travail_jobs_silver_*.parquet"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not silver_files:
        raise FileNotFoundError(
            f"Aucun fichier Silver trouvé dans {SILVER_DIR}"
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
        f"postgresql+psycopg2://{postgres_user}:"
        f"{quote_plus(postgres_password)}"
        f"@{postgres_host}:{postgres_port}/{postgres_db}"
    )

    return create_engine(connection_url, pool_pre_ping=True)


def make_json_serializable(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.isoformat()

    if isinstance(value, datetime):
        return value.isoformat()

    if pd.isna(value) if not isinstance(value, (list, dict, tuple, set)) else False:
        return None

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)

    return value


def clean_dict_for_json(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: make_json_serializable(value)
        for key, value in data.items()
    }


def read_silver_parquet(silver_file: Path) -> pd.DataFrame:
    logger.info(f"Lecture du fichier Silver Parquet : {silver_file}")
    return pd.read_parquet(silver_file)


def compute_parquet_metrics(df: pd.DataFrame, silver_file: Path) -> Dict[str, Any]:
    duplicated_ids = int(df["id_offre"].duplicated().sum()) if "id_offre" in df.columns else None
    missing_ids = int(df["id_offre"].isna().sum()) if "id_offre" in df.columns else None
    distinct_ids = int(df["id_offre"].nunique(dropna=True)) if "id_offre" in df.columns else None

    metrics = {
        "fichier_silver": silver_file.name,
        "chemin_fichier_silver": str(silver_file),
        "nombre_lignes": int(len(df)),
        "nombre_colonnes": int(len(df.columns)),
        "colonnes": list(df.columns),
        "id_offre_distincts": distinct_ids,
        "id_offre_manquants": missing_ids,
        "id_offre_doublons": duplicated_ids,
    }

    if "date_creation" in df.columns:
        metrics["date_creation_min"] = pd.to_datetime(
            df["date_creation"],
            errors="coerce",
            utc=True,
        ).min()

        metrics["date_creation_max"] = pd.to_datetime(
            df["date_creation"],
            errors="coerce",
            utc=True,
        ).max()

    if "date_actualisation" in df.columns:
        metrics["date_actualisation_min"] = pd.to_datetime(
            df["date_actualisation"],
            errors="coerce",
            utc=True,
        ).min()

        metrics["date_actualisation_max"] = pd.to_datetime(
            df["date_actualisation"],
            errors="coerce",
            utc=True,
        ).max()

    return clean_dict_for_json(metrics)


def fetch_postgres_table_metrics(engine: Engine) -> Dict[str, Any]:
    with engine.begin() as connection:
        metrics = connection.execute(
            text(
                f"""
                select
                    count(*) as nombre_lignes,
                    count(distinct id_offre) as id_offre_distincts,
                    count(*) filter (where id_offre is null) as id_offre_manquants,
                    min(date_creation) as date_creation_min,
                    max(date_creation) as date_creation_max,
                    min(date_actualisation) as date_actualisation_min,
                    max(date_actualisation) as date_actualisation_max
                from {TARGET_SCHEMA}.{TARGET_TABLE};
                """
            )
        ).mappings().one()

        duplicates = connection.execute(
            text(
                f"""
                select count(*) as id_offre_doublons
                from (
                    select id_offre
                    from {TARGET_SCHEMA}.{TARGET_TABLE}
                    group by id_offre
                    having count(*) > 1
                ) duplicated;
                """
            )
        ).scalar_one()

    postgres_metrics = dict(metrics)
    postgres_metrics["id_offre_doublons"] = int(duplicates)

    return clean_dict_for_json(postgres_metrics)


def fetch_postgres_columns(engine: Engine) -> List[Dict[str, Any]]:
    with engine.begin() as connection:
        rows = connection.execute(
            text(
                """
                select
                    column_name,
                    data_type,
                    is_nullable
                from information_schema.columns
                where table_schema = :schema_name
                  and table_name = :table_name
                order by ordinal_position;
                """
            ),
            {
                "schema_name": TARGET_SCHEMA,
                "table_name": TARGET_TABLE,
            },
        ).mappings().all()

    return [dict(row) for row in rows]


def fetch_postgres_batches(engine: Engine) -> List[Dict[str, Any]]:
    with engine.begin() as connection:
        rows = connection.execute(
            text(
                f"""
                select
                    fichier_source_silver,
                    batch_id,
                    count(*) as nombre_lignes,
                    min(date_chargement) as premiere_date_chargement,
                    max(date_chargement) as derniere_date_chargement
                from {TARGET_SCHEMA}.{TARGET_TABLE}
                group by fichier_source_silver, batch_id
                order by derniere_date_chargement desc;
                """
            )
        ).mappings().all()

    return [
        clean_dict_for_json(dict(row))
        for row in rows
    ]


def fetch_latest_audit_run(engine: Engine) -> Dict[str, Any] | None:
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
                    message
                from {AUDIT_SCHEMA}.{AUDIT_TABLE}
                where pipeline_name = :pipeline_name
                order by run_id desc
                limit 1;
                """
            ),
            {
                "pipeline_name": LOAD_PIPELINE_NAME,
            },
        ).mappings().first()

    if row is None:
        return None

    return clean_dict_for_json(dict(row))


def fetch_postgres_ids(engine: Engine) -> pd.Series:
    with engine.begin() as connection:
        df = pd.read_sql_query(
            sql=text(
                f"""
                select id_offre
                from {TARGET_SCHEMA}.{TARGET_TABLE};
                """
            ),
            con=connection,
        )

    return df["id_offre"].astype(str)


def compare_id_sets(
    parquet_ids: pd.Series,
    postgres_ids: pd.Series,
) -> Dict[str, Any]:
    parquet_set = set(parquet_ids.dropna().astype(str))
    postgres_set = set(postgres_ids.dropna().astype(str))

    missing_in_postgres = sorted(list(parquet_set - postgres_set))
    extra_in_postgres = sorted(list(postgres_set - parquet_set))

    return {
        "ids_identiques": len(missing_in_postgres) == 0 and len(extra_in_postgres) == 0,
        "nombre_ids_absents_dans_postgres": len(missing_in_postgres),
        "nombre_ids_en_trop_dans_postgres": len(extra_in_postgres),
        "exemples_ids_absents_dans_postgres": missing_in_postgres[:10],
        "exemples_ids_en_trop_dans_postgres": extra_in_postgres[:10],
    }


def compare_columns(
    parquet_columns: List[str],
    postgres_columns: List[str],
) -> Dict[str, Any]:
    parquet_set = set(parquet_columns)
    postgres_set = set(postgres_columns)
    expected_extra_set = set(EXPECTED_POSTGRES_EXTRA_COLUMNS)

    missing_in_postgres = sorted(list(parquet_set - postgres_set))
    extra_in_postgres = sorted(list(postgres_set - parquet_set))

    unexpected_extra_columns = sorted(
        list(set(extra_in_postgres) - expected_extra_set)
    )

    return {
        "colonnes_parquet": len(parquet_columns),
        "colonnes_postgresql": len(postgres_columns),
        "colonnes_attendues_en_plus_postgresql": EXPECTED_POSTGRES_EXTRA_COLUMNS,
        "colonnes_absentes_dans_postgresql": missing_in_postgres,
        "colonnes_en_plus_dans_postgresql": extra_in_postgres,
        "colonnes_en_plus_non_attendues": unexpected_extra_columns,
        "schema_colonnes_valide": (
            len(missing_in_postgres) == 0
            and len(unexpected_extra_columns) == 0
            and expected_extra_set.issubset(postgres_set)
        ),
    }


def build_validation_checks(
    parquet_metrics: Dict[str, Any],
    postgres_metrics: Dict[str, Any],
    id_comparison: Dict[str, Any],
    column_comparison: Dict[str, Any],
    batches: List[Dict[str, Any]],
    latest_audit_run: Dict[str, Any] | None,
) -> Dict[str, bool]:
    checks = {
        "nombre_lignes_identique": (
            parquet_metrics["nombre_lignes"]
            == postgres_metrics["nombre_lignes"]
        ),
        "id_offre_manquants_parquet_zero": (
            parquet_metrics["id_offre_manquants"] == 0
        ),
        "id_offre_manquants_postgresql_zero": (
            postgres_metrics["id_offre_manquants"] == 0
        ),
        "doublons_id_offre_parquet_zero": (
            parquet_metrics["id_offre_doublons"] == 0
        ),
        "doublons_id_offre_postgresql_zero": (
            postgres_metrics["id_offre_doublons"] == 0
        ),
        "ids_identiques": id_comparison["ids_identiques"],
        "schema_colonnes_valide": column_comparison["schema_colonnes_valide"],
        "batch_unique": len(batches) == 1,
        "audit_latest_success": (
            latest_audit_run is not None
            and latest_audit_run.get("status") == "SUCCESS"
        ),
    }

    return checks


def get_global_status(checks: Dict[str, bool]) -> str:
    return "VALIDE" if all(checks.values()) else "NON_VALIDE"


def write_json_report(report: Dict[str, Any]) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    output_path = REPORTS_DIR / f"silver_postgres_validation_report_{timestamp}.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(
            report,
            file,
            ensure_ascii=False,
            indent=4,
            default=str,
        )

    return output_path


def write_markdown_summary(report: Dict[str, Any]) -> Path:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    output_path = DOCS_DIR / "silver_postgres_validation_summary.md"

    checks = report["checks"]
    parquet_metrics = report["parquet"]
    postgres_metrics = report["postgresql"]
    column_comparison = report["comparaison_colonnes"]
    id_comparison = report["comparaison_ids"]
    batches = report["batches"]
    audit = report["audit_latest_run"]

    def check_icon(value: bool) -> str:
        return "✅" if value else "❌"

    lines = [
        "# Validation Silver PostgreSQL",
        "",
        "## Statut global",
        "",
        f"**Statut : {report['statut_global']}**",
        "",
        "## Source contrôlée",
        "",
        f"- Fichier Silver Parquet : `{parquet_metrics['fichier_silver']}`",
        f"- Table PostgreSQL : `{TARGET_SCHEMA}.{TARGET_TABLE}`",
        f"- Date du rapport : `{report['date_rapport']}`",
        "",
        "## Résumé des volumes",
        "",
        "| Indicateur | Parquet | PostgreSQL |",
        "|---|---:|---:|",
        f"| Nombre de lignes | {parquet_metrics['nombre_lignes']} | {postgres_metrics['nombre_lignes']} |",
        f"| Nombre de colonnes | {parquet_metrics['nombre_colonnes']} | {column_comparison['colonnes_postgresql']} |",
        f"| ID distincts | {parquet_metrics['id_offre_distincts']} | {postgres_metrics['id_offre_distincts']} |",
        f"| ID manquants | {parquet_metrics['id_offre_manquants']} | {postgres_metrics['id_offre_manquants']} |",
        f"| Doublons ID | {parquet_metrics['id_offre_doublons']} | {postgres_metrics['id_offre_doublons']} |",
        "",
        "## Contrôles de validation",
        "",
        "| Contrôle | Résultat |",
        "|---|---|",
    ]

    for check_name, check_value in checks.items():
        lines.append(f"| {check_name} | {check_icon(check_value)} |")

    lines.extend(
        [
            "",
            "## Comparaison des identifiants",
            "",
            f"- IDs identiques : `{id_comparison['ids_identiques']}`",
            f"- IDs absents dans PostgreSQL : `{id_comparison['nombre_ids_absents_dans_postgres']}`",
            f"- IDs en trop dans PostgreSQL : `{id_comparison['nombre_ids_en_trop_dans_postgres']}`",
            "",
            "## Comparaison des colonnes",
            "",
            f"- Colonnes Parquet : `{column_comparison['colonnes_parquet']}`",
            f"- Colonnes PostgreSQL : `{column_comparison['colonnes_postgresql']}`",
            f"- Colonnes absentes dans PostgreSQL : `{column_comparison['colonnes_absentes_dans_postgresql']}`",
            f"- Colonnes en plus dans PostgreSQL : `{column_comparison['colonnes_en_plus_dans_postgresql']}`",
            f"- Colonnes en plus non attendues : `{column_comparison['colonnes_en_plus_non_attendues']}`",
            "",
            "## Batch chargé",
            "",
        ]
    )

    if batches:
        for batch in batches:
            lines.extend(
                [
                    f"- Fichier source Silver : `{batch['fichier_source_silver']}`",
                    f"- Batch ID : `{batch['batch_id']}`",
                    f"- Nombre de lignes : `{batch['nombre_lignes']}`",
                    f"- Première date de chargement : `{batch['premiere_date_chargement']}`",
                    f"- Dernière date de chargement : `{batch['derniere_date_chargement']}`",
                    "",
                ]
            )
    else:
        lines.append("- Aucun batch trouvé.")

    lines.extend(
        [
            "## Dernier run d'audit",
            "",
        ]
    )

    if audit:
        lines.extend(
            [
                f"- Run ID : `{audit['run_id']}`",
                f"- Pipeline : `{audit['pipeline_name']}`",
                f"- Statut : `{audit['status']}`",
                f"- Début : `{audit['started_at']}`",
                f"- Fin : `{audit['finished_at']}`",
                f"- Message : `{audit['message']}`",
            ]
        )
    else:
        lines.append("- Aucun run d'audit trouvé.")

    lines.append("")

    with output_path.open("w", encoding="utf-8") as file:
        file.write("\n".join(lines))

    return output_path


def run_validation_report() -> Dict[str, Any]:
    silver_file = get_latest_silver_file()
    engine = get_postgres_engine()

    df = read_silver_parquet(silver_file)

    if "id_offre" not in df.columns:
        raise ValueError("La colonne id_offre est absente du fichier Silver.")

    parquet_metrics = compute_parquet_metrics(df, silver_file)
    postgres_metrics = fetch_postgres_table_metrics(engine)

    postgres_columns_data = fetch_postgres_columns(engine)
    postgres_columns = [
        column["column_name"]
        for column in postgres_columns_data
    ]

    postgres_ids = fetch_postgres_ids(engine)

    id_comparison = compare_id_sets(
        parquet_ids=df["id_offre"],
        postgres_ids=postgres_ids,
    )

    column_comparison = compare_columns(
        parquet_columns=list(df.columns),
        postgres_columns=postgres_columns,
    )

    batches = fetch_postgres_batches(engine)
    latest_audit_run = fetch_latest_audit_run(engine)

    checks = build_validation_checks(
        parquet_metrics=parquet_metrics,
        postgres_metrics=postgres_metrics,
        id_comparison=id_comparison,
        column_comparison=column_comparison,
        batches=batches,
        latest_audit_run=latest_audit_run,
    )

    report = {
        "date_rapport": datetime.now(timezone.utc).isoformat(),
        "statut_global": get_global_status(checks),
        "table_postgresql": f"{TARGET_SCHEMA}.{TARGET_TABLE}",
        "parquet": parquet_metrics,
        "postgresql": postgres_metrics,
        "colonnes_postgresql_detail": postgres_columns_data,
        "comparaison_ids": id_comparison,
        "comparaison_colonnes": column_comparison,
        "batches": batches,
        "audit_latest_run": latest_audit_run,
        "checks": checks,
    }

    json_report_path = write_json_report(report)
    markdown_summary_path = write_markdown_summary(report)

    logger.info(f"Rapport JSON généré : {json_report_path}")
    logger.info(f"Synthèse Markdown générée : {markdown_summary_path}")

    print("Rapport de validation Silver PostgreSQL généré.")
    print(f"Statut global : {report['statut_global']}")
    print(f"Fichier Silver : {silver_file.name}")
    print(f"Table PostgreSQL : {TARGET_SCHEMA}.{TARGET_TABLE}")
    print(f"Lignes Parquet : {parquet_metrics['nombre_lignes']}")
    print(f"Lignes PostgreSQL : {postgres_metrics['nombre_lignes']}")
    print(f"IDs identiques : {id_comparison['ids_identiques']}")
    print(f"Colonnes Parquet : {column_comparison['colonnes_parquet']}")
    print(f"Colonnes PostgreSQL : {column_comparison['colonnes_postgresql']}")
    print(f"Rapport JSON : {json_report_path}")
    print(f"Synthèse Markdown : {markdown_summary_path}")

    return report


if __name__ == "__main__":
    run_validation_report()