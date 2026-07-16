from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.logger import logger


ROOT_DIR = Path(__file__).resolve().parents[2]

BRONZE_DIR = ROOT_DIR / "data" / "bronze"
SILVER_DIR = ROOT_DIR / "data" / "silver"

REPORTS_DIR = ROOT_DIR / "reports" / "quality"
DOCS_DIR = ROOT_DIR / "docs" / "quality"

REPORTS_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)


BRONZE_PATTERN = "france_travail_jobs_bronze_*.parquet"
SILVER_PATTERN = "france_travail_jobs_silver_*.parquet"


CRITICAL_COLUMNS = [
    "id_offre",
    "intitule_offre",
    "description_offre",
    "date_creation",
    "date_actualisation",
    "libelle_lieu_travail",
    "code_type_contrat",
    "libelle_type_contrat",
    "mot_cle_recherche",
    "fichier_source",
    "enregistrement_brut",
]


CALCULATED_COLUMNS = [
    "date_actualisation_coherente",
    "entreprise_renseignee",
    "description_entreprise_renseignee",
    "information_entreprise_disponible",
    "salaire_renseigne",
    "periode_salaire",
    "coordonnees_renseignees",
    "date_traitement_silver",
]


DATE_COLUMNS = [
    "date_creation",
    "date_actualisation",
    "date_ingestion",
    "date_traitement_silver",
]


# Colonnes garanties à 100% en Bronze : toute valeur manquante en Silver
# signale un échec de parsing du pipeline, pas une limite de la source.
DATE_COLUMNS_EXPECTED_COMPLETE = ["date_creation", "date_actualisation"]


JSON_COLUMNS = [
    "competences_brutes",
    "formations_brutes",
    "langues_brutes",
    "qualites_professionnelles_brutes",
    "contact_brut",
    "agence_brute",
    "permis_bruts",
    "enregistrement_brut",
]


def get_latest_file(directory: Path, pattern: str) -> Path:
    files = sorted(
        directory.glob(pattern),
        key=lambda file: file.stat().st_mtime,
        reverse=True,
    )

    if not files:
        raise FileNotFoundError(
            f"Aucun fichier trouvé dans {directory} avec le pattern {pattern}"
        )

    return files[0]


def percentage(count: int, total: int) -> float:
    if total == 0:
        return 0.0

    return round((count / total) * 100, 2)


def is_missing(value: Any) -> bool:
    if value is None:
        return True

    if value is pd.NA:
        return True

    if isinstance(value, str):
        return value.strip() == ""

    try:
        result = pd.isna(value)

        if isinstance(result, bool):
            return result

        return False

    except Exception:
        return False


def missing_count(series: pd.Series) -> int:
    return int(series.apply(is_missing).sum())


def present_count(series: pd.Series) -> int:
    return int(len(series) - missing_count(series))


def to_display_value(value: Any) -> str:
    if is_missing(value):
        return "NULL"

    return str(value)


def distribution(df: pd.DataFrame, column: str) -> dict[str, int]:
    if column not in df.columns:
        return {}

    counts = df[column].value_counts(dropna=False)

    return {
        to_display_value(key): int(value)
        for key, value in counts.items()
    }


def count_true(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns:
        return 0

    return int((df[column] == True).sum())  # noqa: E712


def is_valid_json(value: Any) -> bool:
    if is_missing(value):
        return True

    if not isinstance(value, str):
        return False

    try:
        json.loads(value)
        return True

    except json.JSONDecodeError:
        return False


def compute_volume_report(bronze_df: pd.DataFrame, silver_df: pd.DataFrame) -> dict[str, Any]:
    return {
        "lignes_bronze": int(len(bronze_df)),
        "lignes_silver": int(len(silver_df)),
        "ecart_lignes": int(len(bronze_df) - len(silver_df)),
        "colonnes_bronze": int(bronze_df.shape[1]),
        "colonnes_silver": int(silver_df.shape[1]),
    }


def compute_id_report(silver_df: pd.DataFrame) -> dict[str, Any]:
    if "id_offre" not in silver_df.columns:
        return {
            "colonne_presente": False,
            "id_offre_manquants": None,
            "id_offre_uniques": None,
            "doublons_id_offre": None,
        }

    return {
        "colonne_presente": True,
        "id_offre_manquants": missing_count(silver_df["id_offre"]),
        "id_offre_uniques": int(silver_df["id_offre"].nunique(dropna=True)),
        "doublons_id_offre": int(silver_df.duplicated(subset=["id_offre"]).sum()),
    }


def compute_columns_report(silver_df: pd.DataFrame) -> dict[str, Any]:
    critical_missing_columns = [
        column
        for column in CRITICAL_COLUMNS
        if column not in silver_df.columns
    ]

    calculated_missing_columns = [
        column
        for column in CALCULATED_COLUMNS
        if column not in silver_df.columns
    ]

    critical_missing_values = {}

    for column in CRITICAL_COLUMNS:
        if column in silver_df.columns:
            critical_missing_values[column] = missing_count(silver_df[column])
        else:
            critical_missing_values[column] = None

    return {
        "colonnes_critiques_absentes": critical_missing_columns,
        "colonnes_calculees_absentes": calculated_missing_columns,
        "valeurs_manquantes_colonnes_critiques": critical_missing_values,
    }


def compute_dates_report(silver_df: pd.DataFrame) -> dict[str, Any]:
    report: dict[str, Any] = {}

    for column in DATE_COLUMNS:
        if column not in silver_df.columns:
            report[column] = {
                "presente": False,
                "type": None,
                "valeurs_manquantes": None,
                "date_min": None,
                "date_max": None,
            }
            continue

        report[column] = {
            "presente": True,
            "type": str(silver_df[column].dtype),
            "valeurs_manquantes": int(silver_df[column].isna().sum()),
            "date_min": str(silver_df[column].min()) if silver_df[column].notna().any() else None,
            "date_max": str(silver_df[column].max()) if silver_df[column].notna().any() else None,
        }

    report["date_actualisation_coherente"] = distribution(
        silver_df,
        "date_actualisation_coherente",
    )

    return report


def compute_company_report(silver_df: pd.DataFrame) -> dict[str, Any]:
    total_rows = len(silver_df)

    entreprise_count = count_true(silver_df, "entreprise_renseignee")
    description_count = count_true(silver_df, "description_entreprise_renseignee")
    information_count = count_true(silver_df, "information_entreprise_disponible")

    return {
        "entreprise_renseignee": {
            "nombre": entreprise_count,
            "taux_%": percentage(entreprise_count, total_rows),
        },
        "description_entreprise_renseignee": {
            "nombre": description_count,
            "taux_%": percentage(description_count, total_rows),
        },
        "information_entreprise_disponible": {
            "nombre": information_count,
            "taux_%": percentage(information_count, total_rows),
        },
        "est_entreprise_adaptee": distribution(
            silver_df,
            "est_entreprise_adaptee",
        ),
        "est_employeur_handi_engage": distribution(
            silver_df,
            "est_employeur_handi_engage",
        ),
    }


def compute_salary_report(silver_df: pd.DataFrame) -> dict[str, Any]:
    total_rows = len(silver_df)
    salary_count = count_true(silver_df, "salaire_renseigne")

    return {
        "salaire_renseigne": {
            "nombre": salary_count,
            "taux_%": percentage(salary_count, total_rows),
        },
        "periode_salaire": distribution(
            silver_df,
            "periode_salaire",
        ),
    }


def compute_location_report(silver_df: pd.DataFrame) -> dict[str, Any]:
    total_rows = len(silver_df)

    coordinates_count = count_true(silver_df, "coordonnees_renseignees")

    code_postal_count = (
        present_count(silver_df["code_postal"])
        if "code_postal" in silver_df.columns
        else 0
    )

    code_commune_count = (
        present_count(silver_df["code_commune"])
        if "code_commune" in silver_df.columns
        else 0
    )

    return {
        "coordonnees_renseignees": {
            "nombre": coordinates_count,
            "taux_%": percentage(coordinates_count, total_rows),
        },
        "code_postal_renseigne": {
            "nombre": code_postal_count,
            "taux_%": percentage(code_postal_count, total_rows),
        },
        "code_commune_renseigne": {
            "nombre": code_commune_count,
            "taux_%": percentage(code_commune_count, total_rows),
        },
    }


def compute_contract_report(silver_df: pd.DataFrame) -> dict[str, Any]:
    return {
        "code_type_contrat": distribution(
            silver_df,
            "code_type_contrat",
        ),
        "libelle_type_contrat": distribution(
            silver_df,
            "libelle_type_contrat",
        ),
        "nature_contrat": distribution(
            silver_df,
            "nature_contrat",
        ),
    }


def compute_json_report(silver_df: pd.DataFrame) -> dict[str, Any]:
    report: dict[str, Any] = {}

    for column in JSON_COLUMNS:
        if column not in silver_df.columns:
            report[column] = {
                "presente": False,
                "valeurs_non_vides": None,
                "json_valides": None,
                "json_invalides": None,
                "exemples_invalides": [],
            }
            continue

        non_empty_mask = ~silver_df[column].apply(is_missing)
        non_empty_values = silver_df.loc[non_empty_mask, column]

        validity = non_empty_values.apply(is_valid_json)
        invalid_values = non_empty_values.loc[~validity]

        report[column] = {
            "presente": True,
            "valeurs_non_vides": int(non_empty_mask.sum()),
            "json_valides": int(validity.sum()),
            "json_invalides": int((~validity).sum()),
            "exemples_invalides": invalid_values.head(3).tolist(),
        }

    return report


def compute_missing_values_report(silver_df: pd.DataFrame) -> list[dict[str, Any]]:
    total_rows = len(silver_df)
    rows = []

    for column in silver_df.columns:
        missing = missing_count(silver_df[column])
        present = total_rows - missing

        rows.append(
            {
                "colonne": column,
                "manquant": missing,
                "present": present,
                "taux_manquant_%": percentage(missing, total_rows),
                "taux_present_%": percentage(present, total_rows),
            }
        )

    return sorted(
        rows,
        key=lambda row: row["taux_manquant_%"],
        reverse=True,
    )


def build_validation_status(report: dict[str, Any]) -> dict[str, Any]:
    problems = []

    id_report = report["identifiant"]
    columns_report = report["colonnes"]
    json_report = report["json"]
    dates_report = report["dates"]

    if id_report["colonne_presente"] is False:
        problems.append("La colonne id_offre est absente.")

    if id_report["id_offre_manquants"] not in (0, None):
        problems.append("Certaines lignes ont un id_offre manquant.")

    if id_report["doublons_id_offre"] not in (0, None):
        problems.append("Des doublons id_offre sont présents.")

    if columns_report["colonnes_critiques_absentes"]:
        problems.append("Certaines colonnes critiques sont absentes.")

    if columns_report["colonnes_calculees_absentes"]:
        problems.append("Certaines colonnes calculées Silver sont absentes.")

    # date_creation et date_actualisation sont à 100% renseignées en
    # Bronze : toute valeur manquante en Silver signale un échec de
    # parsing du pipeline, pas une limite de la source.
    for column in DATE_COLUMNS_EXPECTED_COMPLETE:
        column_report = dates_report.get(column, {})
        if column_report.get("presente") and (column_report.get("valeurs_manquantes") or 0) > 0:
            problems.append(
                f"La colonne {column} contient "
                f"{column_report['valeurs_manquantes']} date(s) non parsée(s) "
                f"alors qu'elle est complète en Bronze."
            )

    for column, result in json_report.items():
        if result["presente"] and result["json_invalides"] > 0:
            problems.append(f"La colonne {column} contient du JSON invalide.")

    return {
        "statut": "VALIDE" if not problems else "A_CONTROLER",
        "problemes": problems,
    }


def build_quality_report(
    bronze_file: Path,
    silver_file: Path,
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
) -> dict[str, Any]:
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bronze_file": str(bronze_file),
        "silver_file": str(silver_file),
        "volume": compute_volume_report(bronze_df, silver_df),
        "identifiant": compute_id_report(silver_df),
        "colonnes": compute_columns_report(silver_df),
        "dates": compute_dates_report(silver_df),
        "entreprise": compute_company_report(silver_df),
        "salaire": compute_salary_report(silver_df),
        "localisation": compute_location_report(silver_df),
        "contrats": compute_contract_report(silver_df),
        "json": compute_json_report(silver_df),
        "valeurs_manquantes": compute_missing_values_report(silver_df),
    }

    report["validation"] = build_validation_status(report)

    return report


def save_json_report(report: dict[str, Any]) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    output_file = REPORTS_DIR / f"silver_quality_report_{timestamp}.json"

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(
            report,
            file,
            indent=2,
            ensure_ascii=False,
            default=str,
        )

    return output_file


def format_distribution_table(data: dict[str, int]) -> str:
    if not data:
        return "_Aucune donnée disponible._"

    lines = [
        "| Valeur | Nombre |",
        "|---|---:|",
    ]

    for value, count in data.items():
        lines.append(f"| `{value}` | {count} |")

    return "\n".join(lines)


def format_missing_values_table(rows: list[dict[str, Any]], limit: int = 25) -> str:
    lines = [
        "| Colonne | Manquant | Présent | Taux manquant |",
        "|---|---:|---:|---:|",
    ]

    for row in rows[:limit]:
        lines.append(
            f"| `{row['colonne']}` | "
            f"{row['manquant']} | "
            f"{row['present']} | "
            f"{row['taux_manquant_%']} % |"
        )

    return "\n".join(lines)


def save_markdown_summary(report: dict[str, Any]) -> Path:
    output_file = DOCS_DIR / "silver_quality_summary.md"

    markdown = f"""# Rapport qualité — Couche Silver

## 1. Informations générales

- Généré le : `{report["generated_at"]}`
- Statut : **{report["validation"]["statut"]}**
- Fichier Bronze analysé : `{Path(report["bronze_file"]).name}`
- Fichier Silver analysé : `{Path(report["silver_file"]).name}`

---

## 2. Volumétrie

| Indicateur | Valeur |
|---|---:|
| Lignes Bronze | {report["volume"]["lignes_bronze"]} |
| Lignes Silver | {report["volume"]["lignes_silver"]} |
| Écart Bronze - Silver | {report["volume"]["ecart_lignes"]} |
| Colonnes Bronze | {report["volume"]["colonnes_bronze"]} |
| Colonnes Silver | {report["volume"]["colonnes_silver"]} |

---

## 3. Identifiant métier

| Indicateur | Valeur |
|---|---:|
| Colonne `id_offre` présente | {report["identifiant"]["colonne_presente"]} |
| `id_offre` manquants | {report["identifiant"]["id_offre_manquants"]} |
| `id_offre` uniques | {report["identifiant"]["id_offre_uniques"]} |
| Doublons `id_offre` | {report["identifiant"]["doublons_id_offre"]} |

---

## 4. Colonnes

### Colonnes critiques absentes

```text
{report["colonnes"]["colonnes_critiques_absentes"]}
```

### Colonnes calculées absentes

```text
{report["colonnes"]["colonnes_calculees_absentes"]}
```

### Valeurs manquantes sur les colonnes critiques

```json
{json.dumps(report["colonnes"]["valeurs_manquantes_colonnes_critiques"], indent=2, ensure_ascii=False)}
```

---

## 5. Dates

```json
{json.dumps(report["dates"], indent=2, ensure_ascii=False)}
```

---

## 6. Informations entreprise

| Indicateur | Nombre | Taux |
|---|---:|---:|
| Nom entreprise renseigné | {report["entreprise"]["entreprise_renseignee"]["nombre"]} | {report["entreprise"]["entreprise_renseignee"]["taux_%"]} % |
| Description entreprise renseignée | {report["entreprise"]["description_entreprise_renseignee"]["nombre"]} | {report["entreprise"]["description_entreprise_renseignee"]["taux_%"]} % |
| Nom ou description disponible | {report["entreprise"]["information_entreprise_disponible"]["nombre"]} | {report["entreprise"]["information_entreprise_disponible"]["taux_%"]} % |

---

## 7. Salaire

| Indicateur | Nombre | Taux |
|---|---:|---:|
| Salaire renseigné | {report["salaire"]["salaire_renseigne"]["nombre"]} | {report["salaire"]["salaire_renseigne"]["taux_%"]} % |

### Répartition de periode_salaire

{format_distribution_table(report["salaire"]["periode_salaire"])}

---

## 8. Localisation

| Indicateur | Nombre | Taux |
|---|---:|---:|
| Coordonnées renseignées | {report["localisation"]["coordonnees_renseignees"]["nombre"]} | {report["localisation"]["coordonnees_renseignees"]["taux_%"]} % |
| Code postal renseigné | {report["localisation"]["code_postal_renseigne"]["nombre"]} | {report["localisation"]["code_postal_renseigne"]["taux_%"]} % |
| Code commune renseigné | {report["localisation"]["code_commune_renseigne"]["nombre"]} | {report["localisation"]["code_commune_renseigne"]["taux_%"]} % |

---

## 9. Contrats

### code_type_contrat

{format_distribution_table(report["contrats"]["code_type_contrat"])}

### nature_contrat

{format_distribution_table(report["contrats"]["nature_contrat"])}

---

## 10. Validité JSON

```json
{json.dumps(report["json"], indent=2, ensure_ascii=False)}
```

---

## 11. Top valeurs manquantes

{format_missing_values_table(report["valeurs_manquantes"], limit=25)}

---

## 12. Points à contrôler

{report["validation"]["problemes"]}

---

## 13. Conclusion

La couche Silver est considérée comme techniquement valide si :

- `id_offre` est présent, unique et non nul ;
- les colonnes critiques sont présentes ;
- les colonnes calculées Silver sont présentes ;
- les dates sont correctement typées ;
- les champs JSON conservés sont valides ;
- les valeurs manquantes correspondent aux limites de la source et non à une erreur de transformation.
"""

    output_file.write_text(markdown, encoding="utf-8")

    return output_file


def run_silver_quality_report() -> tuple[Path, Path]:
    try:
        bronze_file = get_latest_file(BRONZE_DIR, BRONZE_PATTERN)
        silver_file = get_latest_file(SILVER_DIR, SILVER_PATTERN)

        logger.info(f"Lecture du fichier Bronze : {bronze_file}")
        logger.info(f"Lecture du fichier Silver : {silver_file}")

        bronze_df = pd.read_parquet(bronze_file)
        silver_df = pd.read_parquet(silver_file)

        report = build_quality_report(
            bronze_file=bronze_file,
            silver_file=silver_file,
            bronze_df=bronze_df,
            silver_df=silver_df,
        )

        json_report_file = save_json_report(report)
        markdown_summary_file = save_markdown_summary(report)

        print("Rapport qualité Silver généré.")
        print(f"Statut : {report['validation']['statut']}")
        print(f"Fichier Bronze analysé : {bronze_file.name}")
        print(f"Fichier Silver analysé : {silver_file.name}")
        print(f"Lignes Bronze : {report['volume']['lignes_bronze']}")
        print(f"Lignes Silver : {report['volume']['lignes_silver']}")
        print(f"Doublons id_offre : {report['identifiant']['doublons_id_offre']}")
        print(f"id_offre manquants : {report['identifiant']['id_offre_manquants']}")
        print(
            "Information entreprise disponible : "
            f"{report['entreprise']['information_entreprise_disponible']['taux_%']} %"
        )
        print(
            "Salaire renseigné : "
            f"{report['salaire']['salaire_renseigne']['taux_%']} %"
        )
        print(
            "Coordonnées renseignées : "
            f"{report['localisation']['coordonnees_renseignees']['taux_%']} %"
        )
        print(f"Rapport JSON : {json_report_file}")
        print(f"Synthèse Markdown : {markdown_summary_file}")

        logger.info(f"Rapport JSON Silver généré : {json_report_file}")
        logger.info(f"Synthèse Markdown Silver générée : {markdown_summary_file}")

        return json_report_file, markdown_summary_file

    except Exception as error:
        logger.exception(f"Erreur lors du rapport qualité Silver : {error}")
        raise


if __name__ == "__main__":
    run_silver_quality_report()