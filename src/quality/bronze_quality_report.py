import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.logger import logger


ROOT_DIR = Path(__file__).resolve().parents[2]

BRONZE_DIR = ROOT_DIR / "data" / "bronze"
REPORT_DIR = ROOT_DIR / "reports" / "quality"
DOCS_QUALITY_DIR = ROOT_DIR / "docs" / "quality"

REPORT_DIR.mkdir(parents=True, exist_ok=True)
DOCS_QUALITY_DIR.mkdir(parents=True, exist_ok=True)


CRITICAL_COLUMNS = [
    "id",
    "intitule",
    "description",
    "dateCreation",
    "dateActualisation",
    "lieuTravail_libelle",
    "entreprise_nom",
    "typeContrat",
    "typeContratLibelle",
    "salaire_libelle",
    "search_keyword",
    "ingestion_timestamp",
    "raw_source_file",
    "raw_record",
]


JSON_COLUMNS = [
    "competences",
    "formations",
    "langues",
    "qualitesProfessionnelles",
    "contact",
    "agence",
    "permis",
    "raw_record",
]


DATE_COLUMNS = [
    "dateCreation",
    "dateActualisation",
    "ingestion_timestamp",
]


def get_latest_bronze_file() -> Path:
    """
    Récupère le fichier Bronze Parquet le plus récent.
    """

    bronze_files = sorted(
        BRONZE_DIR.glob("france_travail_jobs_bronze_*.parquet"),
        key=lambda file: file.stat().st_mtime,
        reverse=True,
    )

    if not bronze_files:
        raise FileNotFoundError(
            "Aucun fichier Bronze trouvé dans data/bronze/. "
            "Lance d'abord la transformation Raw vers Bronze."
        )

    return bronze_files[0]


def is_missing(value: Any) -> bool:
    """
    Détermine si une valeur doit être considérée comme manquante.

    Gère aussi les valeurs array-like (ex: colonnes ayant été lues
    depuis Parquet avec un type liste/array) pour éviter l'erreur
    "ambiguous truth value" de pandas sur les types non scalaires.
    """

    # Valeurs de type liste/tuple/array : considérées manquantes si vides
    if isinstance(value, (list, tuple)):
        return len(value) == 0

    if hasattr(value, "shape"):  # numpy array
        return value.size == 0

    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        # pd.isna a échoué car value n'est pas un scalaire reconnu
        return False

    if isinstance(value, str) and value.strip() == "":
        return True

    return False


def count_missing(series: pd.Series) -> int:
    """
    Compte les valeurs manquantes dans une colonne.
    """

    return int(series.apply(is_missing).sum())


def compute_completeness(df: pd.DataFrame) -> dict:
    """
    Calcule le taux de complétude de chaque colonne.
    """

    results = {}

    total_rows = len(df)

    for column in df.columns:
        missing_count = count_missing(df[column])
        available_count = total_rows - missing_count

        results[column] = {
            "available_count": int(available_count),
            "missing_count": int(missing_count),
            "completeness_rate": round((available_count / total_rows) * 100, 2),
        }

    return results


def validate_json_column(series: pd.Series) -> dict:
    """
    Vérifie si les valeurs non nulles d'une colonne sont des JSON valides.
    """

    valid_count = 0
    invalid_count = 0
    missing_count = 0

    for value in series:
        if is_missing(value):
            missing_count += 1
            continue

        try:
            json.loads(value)
            valid_count += 1
        except Exception:
            invalid_count += 1

    total_non_missing = valid_count + invalid_count

    if total_non_missing == 0:
        validity_rate = None
    else:
        validity_rate = round((valid_count / total_non_missing) * 100, 2)

    return {
        "valid_json_count": int(valid_count),
        "invalid_json_count": int(invalid_count),
        "missing_count": int(missing_count),
        "json_validity_rate_non_missing": validity_rate,
    }


def validate_date_column(series: pd.Series) -> dict:
    """
    Vérifie si les valeurs non nulles d'une colonne sont convertibles en dates.
    """

    missing_count = count_missing(series)

    parsed_dates = pd.to_datetime(series, errors="coerce", utc=True)

    invalid_count = int(parsed_dates.isna().sum() - missing_count)
    valid_count = int(len(series) - missing_count - invalid_count)

    non_missing_count = valid_count + invalid_count

    if non_missing_count == 0:
        validity_rate = None
    else:
        validity_rate = round((valid_count / non_missing_count) * 100, 2)

    return {
        "valid_date_count": valid_count,
        "invalid_date_count": invalid_count,
        "missing_count": missing_count,
        "date_validity_rate_non_missing": validity_rate,
    }


def value_counts_top(series: pd.Series, top_n: int = 15) -> dict:
    """
    Retourne les valeurs les plus fréquentes d'une colonne.
    """

    return (
        series.fillna("Non renseigné")
        .astype(str)
        .replace("", "Non renseigné")
        .value_counts()
        .head(top_n)
        .to_dict()
    )


def generate_markdown_summary(report: dict, output_file: Path) -> None:
    """
    Génère une synthèse Markdown lisible pour documentation.
    """

    completeness = report["completeness"]

    markdown = f"""# Rapport qualité Bronze

## Fichier analysé

`{report["bronze_file"]}`

## Résumé

| Indicateur | Valeur |
|---|---:|
| Nombre de lignes | {report["row_count"]} |
| Nombre de colonnes | {report["column_count"]} |
| Identifiants uniques | {report["unique_job_ids"]} |
| Doublons sur `id` | {report["duplicate_job_ids"]} |
| Offres avec salaire renseigné | {report["salary_available_count"]} |
| Offres sans salaire renseigné | {report["salary_missing_count"]} |
| Taux de salaire renseigné | {report["salary_available_rate"]} % |
| Entreprises renseignées | {report["company_available_count"]} |
| Entreprises non renseignées | {report["company_missing_count"]} |
| Taux entreprise renseignée | {report["company_available_rate"]} % |

## Complétude des colonnes critiques

| Colonne | Disponible | Manquant | Complétude |
|---|---:|---:|---:|
"""

    for column in report["critical_columns"]:
        if column in completeness:
            info = completeness[column]
            markdown += (
                f"| `{column}` | "
                f"{info['available_count']} | "
                f"{info['missing_count']} | "
                f"{info['completeness_rate']} % |\n"
            )
        else:
            markdown += f"| `{column}` | 0 | {report['row_count']} | 0 % |\n"

    markdown += """

## Validité des dates

| Colonne | Dates valides | Dates invalides | Valeurs manquantes | Taux de validité |
|---|---:|---:|---:|---:|
"""

    for column, info in report["date_validation"].items():
        markdown += (
            f"| `{column}` | "
            f"{info['valid_date_count']} | "
            f"{info['invalid_date_count']} | "
            f"{info['missing_count']} | "
            f"{info['date_validity_rate_non_missing']} % |\n"
        )

    markdown += """

## Validité des champs JSON

| Colonne | JSON valides | JSON invalides | Valeurs manquantes | Taux de validité |
|---|---:|---:|---:|---:|
"""

    for column, info in report["json_validation"].items():
        markdown += (
            f"| `{column}` | "
            f"{info['valid_json_count']} | "
            f"{info['invalid_json_count']} | "
            f"{info['missing_count']} | "
            f"{info['json_validity_rate_non_missing']} % |\n"
        )

    markdown += """

## Observations importantes

- La couche Bronze conserve le même niveau de granularité que la source : une ligne correspond à une offre récupérée.
- Les champs `entreprise_nom` et `salaire_libelle` présentent une forte proportion de valeurs manquantes.
- Les salaires manquants ne sont pas une erreur de pipeline : ils reflètent une limite de complétude de la source.
- Les champs complexes comme `competences`, `formations`, `langues` et `permis` sont conservés au format JSON string.
- Les règles de nettoyage seront appliquées uniquement dans la couche Silver.

## Décisions pour la couche Silver

- Conserver une ligne par offre unique.
- Typer les dates.
- Standardiser les localisations.
- Créer un champ `company_name_clean`.
- Créer un indicateur `has_salary`.
- Extraire les bornes de salaire lorsque le format le permet.
- Normaliser les types de contrat.
- Préparer des tables analytiques pour la couche Gold.
"""

    with open(output_file, "w", encoding="utf-8") as file:
        file.write(markdown)


def generate_bronze_quality_report() -> None:
    """
    Génère un rapport qualité complet sur la couche Bronze.
    """

    logger.info("Début du rapport qualité Bronze")

    bronze_file = get_latest_bronze_file()

    logger.info(f"Fichier Bronze analysé : {bronze_file}")

    df = pd.read_parquet(bronze_file)

    row_count = len(df)
    column_count = len(df.columns)

    missing_columns = [
        column for column in CRITICAL_COLUMNS if column not in df.columns
    ]

    completeness = compute_completeness(df)

    duplicate_job_ids = None
    unique_job_ids = None

    if "id" in df.columns:
        unique_job_ids = int(df["id"].nunique(dropna=True))
        duplicate_job_ids = int(df.duplicated(subset=["id"]).sum())

    salary_missing_count = None
    salary_available_count = None
    salary_available_rate = None

    if "salaire_libelle" in df.columns:
        salary_missing_count = count_missing(df["salaire_libelle"])
        salary_available_count = row_count - salary_missing_count
        salary_available_rate = round((salary_available_count / row_count) * 100, 2)

    company_missing_count = None
    company_available_count = None
    company_available_rate = None

    if "entreprise_nom" in df.columns:
        company_missing_count = count_missing(df["entreprise_nom"])
        company_available_count = row_count - company_missing_count
        company_available_rate = round((company_available_count / row_count) * 100, 2)

    date_validation = {}

    for column in DATE_COLUMNS:
        if column in df.columns:
            date_validation[column] = validate_date_column(df[column])

    json_validation = {}

    for column in JSON_COLUMNS:
        if column in df.columns:
            json_validation[column] = validate_json_column(df[column])

    distributions = {}

    distribution_columns = [
        "typeContratLibelle",
        "natureContrat",
        "lieuTravail_libelle",
        "romeLibelle",
        "search_keyword",
        "secteurActiviteLibelle",
    ]

    for column in distribution_columns:
        if column in df.columns:
            distributions[column] = value_counts_top(df[column])

    report = {
        "report_generated_at": datetime.now().isoformat(),
        "bronze_file": bronze_file.name,
        "row_count": int(row_count),
        "column_count": int(column_count),
        "columns": list(df.columns),
        "critical_columns": CRITICAL_COLUMNS,
        "missing_critical_columns": missing_columns,
        "unique_job_ids": unique_job_ids,
        "duplicate_job_ids": duplicate_job_ids,
        "salary_available_count": salary_available_count,
        "salary_missing_count": salary_missing_count,
        "salary_available_rate": salary_available_rate,
        "company_available_count": company_available_count,
        "company_missing_count": company_missing_count,
        "company_available_rate": company_available_rate,
        "completeness": completeness,
        "date_validation": date_validation,
        "json_validation": json_validation,
        "distributions": distributions,
    }

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    json_output_file = REPORT_DIR / f"bronze_quality_report_{timestamp}.json"
    markdown_output_file = DOCS_QUALITY_DIR / "bronze_quality_summary.md"

    with open(json_output_file, "w", encoding="utf-8") as file:
        json.dump(report, file, ensure_ascii=False, indent=2)

    generate_markdown_summary(report, markdown_output_file)

    logger.info(f"Rapport JSON créé : {json_output_file}")
    logger.info(f"Synthèse Markdown créée : {markdown_output_file}")
    logger.info("Fin du rapport qualité Bronze")

    print("Rapport qualité Bronze généré.")
    print(f"Fichier Bronze analysé : {bronze_file.name}")
    print(f"Nombre de lignes : {row_count}")
    print(f"Nombre de colonnes : {column_count}")
    print(f"Doublons sur id : {duplicate_job_ids}")
    print(f"Taux de salaire renseigné : {salary_available_rate} %")
    print(f"Taux entreprise renseignée : {company_available_rate} %")
    print(f"Rapport JSON : {json_output_file}")
    print(f"Synthèse Markdown : {markdown_output_file}")


if __name__ == "__main__":
    generate_bronze_quality_report()