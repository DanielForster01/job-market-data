import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from src.utils.logger import logger


ROOT_DIR = Path(__file__).resolve().parents[2]
BRONZE_DIR = ROOT_DIR / "data" / "bronze"
SILVER_DIR = ROOT_DIR / "data" / "silver"
SILVER_DIR.mkdir(parents=True, exist_ok=True)


# ----------------------------------------------------------------------
# Renommage des colonnes (section 8 de la documentation)
# ----------------------------------------------------------------------

RENAME_MAP = {
    "id": "id_offre",
    "intitule": "intitule_offre",
    "description": "description_offre",
    "dateCreation": "date_creation",
    "dateActualisation": "date_actualisation",
    "lieuTravail_libelle": "libelle_lieu_travail",
    "lieuTravail_latitude": "latitude",
    "lieuTravail_longitude": "longitude",
    "lieuTravail_codePostal": "code_postal",
    "lieuTravail_commune": "code_commune",
    "entreprise_nom": "nom_entreprise",
    "entreprise_description": "description_entreprise",
    "entrepriseAdaptee": "est_entreprise_adaptee",
    "employeurHandiEngage": "est_employeur_handi_engage",
    "typeContrat": "code_type_contrat",
    "typeContratLibelle": "libelle_type_contrat",
    "natureContrat": "nature_contrat",
    "experienceExige": "experience_exigee",
    "experienceLibelle": "libelle_experience",
    "romeCode": "code_rome",
    "romeLibelle": "libelle_rome",
    "appellationlibelle": "libelle_appellation",
    "salaire_libelle": "salaire_libelle",
    "alternance": "est_alternance",
    "nombrePostes": "nombre_postes",
    "dureeTravailLibelle": "libelle_duree_travail",
    "dureeTravailLibelleConverti": "libelle_duree_travail_converti",
    "qualificationCode": "code_qualification",
    "qualificationLibelle": "libelle_qualification",
    "codeNAF": "code_naf",
    "secteurActivite": "code_secteur_activite",
    "secteurActiviteLibelle": "libelle_secteur_activite",
    "trancheEffectifEtab": "tranche_effectif_etablissement",
    "offresManqueCandidats": "offre_difficile_a_pourvoir",
    "accessibleTH": "accessible_travailleur_handicape",
    "deplacementCode": "code_deplacement",
    "deplacementLibelle": "libelle_deplacement",
    "experienceCommentaire": "commentaire_experience",
    "complementExercice": "complement_exercice",
    "origineOffre_origine": "origine_offre",
    "origineOffre_urlOrigine": "url_origine_offre",
    "contexteTravail_horaires": "horaires_travail",
    "search_keyword": "mot_cle_recherche",
    "ingestion_timestamp": "date_ingestion",
    "raw_source_file": "fichier_source",
    "raw_record": "enregistrement_brut",
    "competences": "competences_brutes",
    "formations": "formations_brutes",
    "langues": "langues_brutes",
    "qualitesProfessionnelles": "qualites_professionnelles_brutes",
    "contact": "contact_brut",
    "agence": "agence_brute",
    "permis": "permis_bruts",
}


# Colonne redondante avec est_entreprise_adaptee.
# Investigation validée :
# - 0 différence sur les lignes où les deux colonnes sont renseignées
# - entrepriseAdaptee est complète à 100 %
# - entreprise_entrepriseAdaptee est moins complète
EXCLUDED_COLUMNS = ["entreprise_entrepriseAdaptee"]


# Colonnes booléennes 100 % renseignées dans Bronze
BOOLEAN_COLUMNS_FULL = [
    "est_entreprise_adaptee",
    "est_employeur_handi_engage",
    "est_alternance",
]


# Colonnes booléennes potentiellement incomplètes
BOOLEAN_COLUMNS_NULLABLE = [
    "offre_difficile_a_pourvoir",
    "accessible_travailleur_handicape",
]


# ----------------------------------------------------------------------
# Fichier Bronze le plus récent
# ----------------------------------------------------------------------

def get_latest_bronze_file() -> Path:
    bronze_files = sorted(
        BRONZE_DIR.glob("france_travail_jobs_bronze_*.parquet"),
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )

    if not bronze_files:
        raise FileNotFoundError(
            "Aucun fichier Bronze trouvé dans data/bronze/. "
            "Lance d'abord la transformation Raw vers Bronze."
        )

    return bronze_files[0]


# ----------------------------------------------------------------------
# Validation des colonnes attendues
# ----------------------------------------------------------------------

def validate_required_columns(df: pd.DataFrame) -> None:
    """
    Vérifie que les colonnes Bronze nécessaires à la transformation Silver
    sont bien présentes avant de lancer le traitement.
    """

    required_columns = set(RENAME_MAP.keys())

    # On ne rend pas obligatoire une colonne explicitement exclue.
    required_columns = required_columns - set(EXCLUDED_COLUMNS)

    missing_columns = sorted(required_columns - set(df.columns))

    if missing_columns:
        raise ValueError(
            "Colonnes Bronze manquantes pour la transformation Silver : "
            + ", ".join(missing_columns)
        )


# ----------------------------------------------------------------------
# Nettoyage des chaînes de caractères
# ----------------------------------------------------------------------

def clean_string_value(value):
    """
    Nettoie une valeur texte :
    - suppression des espaces en début et fin ;
    - chaîne vide transformée en valeur manquante.
    """

    if not isinstance(value, str):
        return value

    stripped = value.strip()

    return stripped if stripped != "" else None


def clean_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie toutes les colonnes texte.

    Règles :
    - suppression des espaces en début/fin ;
    - chaînes vides -> valeur manquante ;
    - pas de modification de casse ;
    - pas de correction métier automatique.
    """

    object_columns = df.select_dtypes(include="object").columns

    for column in object_columns:
        df[column] = df[column].apply(clean_string_value)

    return df


# ----------------------------------------------------------------------
# Traitement des dates
# ----------------------------------------------------------------------

def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convertit les dates en datetime UTC.

    date_creation et date_actualisation sont déjà au format ISO UTC.
    date_ingestion vient du nom/timestamp d'ingestion au format :
    YYYY-MM-DD_HH-MM-SS
    """

    df["date_creation"] = pd.to_datetime(
        df["date_creation"],
        utc=True,
        errors="coerce",
    )

    df["date_actualisation"] = pd.to_datetime(
        df["date_actualisation"],
        utc=True,
        errors="coerce",
    )

    df["date_ingestion"] = pd.to_datetime(
        df["date_ingestion"],
        format="%Y-%m-%d_%H-%M-%S",
        errors="coerce",
        utc=True,
    )

    return df


def compute_date_coherence(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée date_actualisation_coherente.

    Règle :
    - True si date_actualisation >= date_creation ;
    - False si date_actualisation < date_creation ;
    - NA si une des deux dates est absente ou invalide.
    """

    coherence = pd.array([pd.NA] * len(df), dtype="boolean")

    both_valid = df["date_creation"].notna() & df["date_actualisation"].notna()

    coherence[both_valid.to_numpy()] = (
        df.loc[both_valid, "date_actualisation"]
        >= df.loc[both_valid, "date_creation"]
    ).to_numpy()

    df["date_actualisation_coherente"] = coherence

    return df


# ----------------------------------------------------------------------
# Gestion des doublons stricts sur id_offre
# ----------------------------------------------------------------------

def deduplicate_on_id(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Supprime uniquement les doublons stricts sur id_offre.

    Règle :
    - pour un même id_offre, conserver la ligne la plus récemment actualisée.
    - les quasi-doublons ne sont pas supprimés en Silver.
    """

    initial_count = len(df)

    df = df.sort_values(
        "date_actualisation",
        ascending=False,
        na_position="last",
    )

    df = df.drop_duplicates(
        subset="id_offre",
        keep="first",
    )

    df = df.reset_index(drop=True)

    duplicates_removed = initial_count - len(df)

    return df, duplicates_removed


# ----------------------------------------------------------------------
# Informations entreprise
# ----------------------------------------------------------------------

def apply_entreprise_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée les indicateurs de disponibilité des informations entreprise.
    """

    df["entreprise_renseignee"] = df["nom_entreprise"].notna()

    df["description_entreprise_renseignee"] = (
        df["description_entreprise"].notna()
    )

    df["information_entreprise_disponible"] = (
        df["entreprise_renseignee"]
        | df["description_entreprise_renseignee"]
    )

    return df


# ----------------------------------------------------------------------
# Salaire
# ----------------------------------------------------------------------

PERIOD_PATTERN = re.compile(r"^\s*(Annuel|Mensuel|Horaire)", re.IGNORECASE)


def extract_salary_period(text: Optional[str]) -> Optional[str]:
    """
    Extrait uniquement la période du salaire :
    - Annuel
    - Mensuel
    - Horaire

    Ne calcule pas salaire_min, salaire_max ou salaire_moyen.
    """

    if not isinstance(text, str) or text.strip() == "":
        return None

    match = PERIOD_PATTERN.match(text)

    if not match:
        return None

    return match.group(1).capitalize()


def apply_salary_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée les indicateurs simples liés au salaire.
    """

    df["salaire_renseigne"] = df["salaire_libelle"].notna()
    df["periode_salaire"] = df["salaire_libelle"].apply(extract_salary_period)

    return df


# ----------------------------------------------------------------------
# Localisation
# ----------------------------------------------------------------------

def apply_location_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée l'indicateur de disponibilité des coordonnées géographiques.
    """

    df["coordonnees_renseignees"] = (
        df["latitude"].notna()
        & df["longitude"].notna()
    )

    return df


# ----------------------------------------------------------------------
# Typage booléen robuste
# ----------------------------------------------------------------------

def normalize_boolean_value(value):
    """
    Normalise une valeur en booléen nullable.

    Cette fonction sécurise les cas où une colonne booléenne serait lue
    comme object avec True/False, None, ou éventuellement des chaînes.
    """

    if pd.isna(value):
        return pd.NA

    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        normalized = value.strip().lower()

        if normalized in {"true", "1", "yes", "oui"}:
            return True

        if normalized in {"false", "0", "no", "non"}:
            return False

    return value


# ----------------------------------------------------------------------
# Typage final
# ----------------------------------------------------------------------

def apply_final_typing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique les types finaux attendus en Silver.
    """

    df["id_offre"] = df["id_offre"].astype("string")
    df["nombre_postes"] = df["nombre_postes"].astype("Int64")

    for column in BOOLEAN_COLUMNS_FULL:
        df[column] = df[column].apply(normalize_boolean_value).astype("boolean")

    for column in BOOLEAN_COLUMNS_NULLABLE:
        df[column] = df[column].apply(normalize_boolean_value).astype("boolean")

    calculated_boolean_columns = [
        "date_actualisation_coherente",
        "entreprise_renseignee",
        "description_entreprise_renseignee",
        "information_entreprise_disponible",
        "salaire_renseigne",
        "coordonnees_renseignees",
    ]

    for column in calculated_boolean_columns:
        df[column] = df[column].astype("boolean")

    return df


# ----------------------------------------------------------------------
# Transformation principale Bronze -> Silver
# ----------------------------------------------------------------------

def run_silver_transformation(bronze_file: Path) -> Path:
    try:
        logger.info(f"Lecture du fichier Bronze : {bronze_file}")

        df = pd.read_parquet(bronze_file)
        bronze_row_count = len(df)

        logger.info(f"{bronze_row_count} lignes Bronze à transformer")

        validate_required_columns(df)

        # Suppression des colonnes explicitement exclues de Silver.
        df = df.drop(columns=EXCLUDED_COLUMNS, errors="ignore")

        # Renommage des colonnes Bronze vers Silver.
        df = df.rename(columns=RENAME_MAP)

        # Nettoyage léger des chaînes de caractères.
        df = clean_string_columns(df)

        # Normalisation du mot-clé de recherche.
        if "mot_cle_recherche" in df.columns:
            df["mot_cle_recherche"] = df["mot_cle_recherche"].str.lower()

        # Conversion des dates.
        df = parse_dates(df)

        # Déduplication stricte uniquement sur id_offre.
        df, duplicates_removed = deduplicate_on_id(df)

        # Colonnes calculées.
        df = compute_date_coherence(df)
        df = apply_entreprise_indicators(df)
        df = apply_salary_indicators(df)
        df = apply_location_indicators(df)

        # Date de traitement Silver.
        processing_time = datetime.now(timezone.utc)
        df["date_traitement_silver"] = pd.Timestamp.now(tz="UTC")

        # Typage final.
        df = apply_final_typing(df)

        silver_row_count = len(df)

        unique_ids = df["id_offre"].nunique(dropna=True)
        missing_ids = df["id_offre"].isna().sum()

        logger.info(f"Doublons id_offre supprimés : {duplicates_removed}")
        logger.info(
            f"Lignes Silver : {silver_row_count} "
            f"(Bronze : {bronze_row_count})"
        )
        logger.info(
            f"id_offre uniques : {unique_ids} | "
            f"id_offre manquants : {missing_ids}"
        )

         # Nom de fichier timestampé au moment du traitement, cohérent avec
        # la convention déjà utilisée pour Raw et Bronze (traçabilité
        # complète de chaque exécution, pas d'écrasement).

        timestamp_str = processing_time.strftime("%Y-%m-%d_%H-%M-%S")
        output_file = SILVER_DIR / f"france_travail_jobs_silver_{timestamp_str}.parquet"
 

        df.to_parquet(output_file, index=False)

        logger.info(f"Fichier Silver sauvegardé : {output_file}")

        print(
            f"Transformation Silver terminée : "
            f"{silver_row_count} lignes, {df.shape[1]} colonnes"
        )
        print(f"Doublons id_offre supprimés : {duplicates_removed}")
        print(f"id_offre uniques : {unique_ids} / {silver_row_count}")
        print(f"id_offre manquants : {missing_ids}")
        print(f"Fichier créé : {output_file}")

        return output_file

    except Exception as error:
        logger.exception(f"Erreur lors de la transformation Silver : {error}")
        raise


if __name__ == "__main__":
    latest_bronze = get_latest_bronze_file()
    run_silver_transformation(latest_bronze)