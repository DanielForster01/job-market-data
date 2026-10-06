import argparse
import hashlib
import re
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from src.storage.r2_paths import (
    build_silver_object_key,
    parse_data_lake_object_key,
)
from src.storage.r2_storage import R2Storage
from src.utils.logger import logger
from src.utils.run_context import (
    ProcessingRunContext,
    build_processing_run_context,
    get_git_branch,
    get_git_commit_sha,
    is_git_worktree_dirty,
)


# ============================================================
# Configuration
# ============================================================

ROOT_DIR = (
    Path(__file__)
    .resolve()
    .parents[2]
)

SOURCE_NAME = "france_travail"

EXPECTED_BRONZE_SCHEMA_VERSION = "2.0.0"
EXPECTED_BRONZE_PATH_VERSION = "processing_v2"

SILVER_SCHEMA_VERSION = "2.0.0"
SILVER_PATH_VERSION = "processing_v2"


# ============================================================
# Renommage Bronze -> Silver
# ============================================================

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

    "employeurHandiEngage": (
        "est_employeur_handi_engage"
    ),

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

    "dureeTravailLibelle": (
        "libelle_duree_travail"
    ),

    "dureeTravailLibelleConverti": (
        "libelle_duree_travail_converti"
    ),

    "qualificationCode": "code_qualification",

    "qualificationLibelle": (
        "libelle_qualification"
    ),

    "codeNAF": "code_naf",

    "secteurActivite": (
        "code_secteur_activite"
    ),

    "secteurActiviteLibelle": (
        "libelle_secteur_activite"
    ),

    "trancheEffectifEtab": (
        "tranche_effectif_etablissement"
    ),

    "offresManqueCandidats": (
        "offre_difficile_a_pourvoir"
    ),

    "accessibleTH": (
        "accessible_travailleur_handicape"
    ),

    "deplacementCode": (
        "code_deplacement"
    ),

    "deplacementLibelle": (
        "libelle_deplacement"
    ),

    "experienceCommentaire": (
        "commentaire_experience"
    ),

    "complementExercice": (
        "complement_exercice"
    ),

    "origineOffre_origine": (
        "origine_offre"
    ),

    "origineOffre_urlOrigine": (
        "url_origine_offre"
    ),

    "contexteTravail_horaires": (
        "horaires_travail"
    ),

    # --------------------------------------------------------
    # Acquisition
    # --------------------------------------------------------

    "search_keyword": (
        "mot_cle_recherche"
    ),

    "search_keywords": (
        "mots_cles_recherche"
    ),

    # --------------------------------------------------------
    # Ingestion
    # --------------------------------------------------------

    "ingestion_timestamp": (
        "date_ingestion"
    ),

    # --------------------------------------------------------
    # Dataset / lineage
    # --------------------------------------------------------

    "batch_id": "batch_id",

    # Le processing_run_id présent dans Bronze appartient
    # à l'artefact Bronze parent.
    "processing_run_id": (
        "bronze_processing_run_id"
    ),

    "bronze_schema_version": (
        "bronze_schema_version"
    ),

    "raw_source_file": (
        "fichier_source"
    ),

    "raw_source_object_key": (
        "objet_source_raw"
    ),

    "raw_record": (
        "enregistrement_brut"
    ),

    # --------------------------------------------------------
    # Structures complexes
    # --------------------------------------------------------

    "competences": (
        "competences_brutes"
    ),

    "formations": (
        "formations_brutes"
    ),

    "langues": (
        "langues_brutes"
    ),

    "qualitesProfessionnelles": (
        "qualites_professionnelles_brutes"
    ),

    "contact": (
        "contact_brut"
    ),

    "agence": (
        "agence_brute"
    ),

    "permis": (
        "permis_bruts"
    ),
}


# ============================================================
# Colonnes exclues
# ============================================================

EXCLUDED_COLUMNS = [
    "entreprise_entrepriseAdaptee",
]


# ============================================================
# Colonnes booléennes
# ============================================================

BOOLEAN_COLUMNS_FULL = [
    "est_entreprise_adaptee",
    "est_employeur_handi_engage",
    "est_alternance",
]


BOOLEAN_COLUMNS_NULLABLE = [
    "offre_difficile_a_pourvoir",
    "accessible_travailleur_handicape",
]


# ============================================================
# Schéma Bronze
# ============================================================

def validate_required_columns(
    df: pd.DataFrame,
) -> None:
    """
    Vérifie que le Bronze possède toutes les colonnes
    nécessaires à la construction Silver.
    """

    required_columns = (
        set(
            RENAME_MAP.keys()
        )
        - set(
            EXCLUDED_COLUMNS
        )
    )

    missing_columns = sorted(
        required_columns
        - set(
            df.columns
        )
    )

    if missing_columns:
        raise RuntimeError(
            "Colonnes Bronze manquantes : "
            + ", ".join(
                missing_columns
            )
        )


# ============================================================
# Contexte d'exécution Silver
# ============================================================

def resolve_silver_processing_context(
    bronze_metadata: dict[str, str],
    bronze_processing_run_id: str,
    requested_processing_run_id: str | None,
) -> ProcessingRunContext:
    """
    Détermine le contexte d'exécution Silver.

    Cas 1 :
        aucun nouvel ID n'est fourni.

        Silver appartient alors à la même exécution
        logique que Bronze.

        La provenance Git actuelle doit être STRICTEMENT
        identique à celle ayant produit Bronze.

    Cas 2 :
        un autre processing_run_id est fourni.

        Il s'agit d'un replay/retraitement Silver.
        Un nouveau contexte Git est créé.

    Cela permet de distinguer :

        pipeline normal
            Bronze R1 -> Silver R1

        replay partiel
            Bronze R1 -> Silver R2
    """

    target_processing_run_id = (
        requested_processing_run_id
        or bronze_processing_run_id
    )

    # --------------------------------------------------------
    # Même run logique que Bronze
    # --------------------------------------------------------

    if (
        target_processing_run_id
        == bronze_processing_run_id
    ):

        expected_commit = (
            bronze_metadata.get(
                "git-commit-sha"
            )
        )

        expected_branch = (
            bronze_metadata.get(
                "git-branch"
            )
        )

        expected_dirty_raw = (
            bronze_metadata.get(
                "git-worktree-dirty"
            )
        )

        expected_started_at = (
            bronze_metadata.get(
                "processing-started-at-utc"
            )
        )

        if not expected_commit:
            raise RuntimeError(
                "git-commit-sha absent "
                "du Bronze parent."
            )

        if not expected_branch:
            raise RuntimeError(
                "git-branch absent "
                "du Bronze parent."
            )

        if expected_dirty_raw not in {
            "true",
            "false",
        }:
            raise RuntimeError(
                "git-worktree-dirty invalide "
                "dans le Bronze parent."
            )

        if not expected_started_at:
            raise RuntimeError(
                "processing-started-at-utc absent "
                "du Bronze parent."
            )

        current_commit = (
            get_git_commit_sha(
                repo_dir=ROOT_DIR
            )
        )

        current_branch = (
            get_git_branch(
                repo_dir=ROOT_DIR
            )
        )

        current_dirty = (
            is_git_worktree_dirty(
                repo_dir=ROOT_DIR
            )
        )

        expected_dirty = (
            expected_dirty_raw
            == "true"
        )

        if (
            current_commit
            != expected_commit
        ):
            raise RuntimeError(
                "Le code Git actuel diffère de celui "
                "ayant produit Bronze. "
                "Pour effectuer un replay Silver, "
                "fournis un nouveau "
                "--processing-run-id."
            )

        if (
            current_branch
            != expected_branch
        ):
            raise RuntimeError(
                "La branche Git actuelle diffère "
                "de celle du Bronze parent. "
                "Utilise un nouveau "
                "--processing-run-id pour un replay."
            )

        if (
            current_dirty
            != expected_dirty
        ):
            raise RuntimeError(
                "L'état du worktree Git actuel diffère "
                "de celui ayant produit Bronze. "
                "Utilise un nouveau "
                "--processing-run-id pour un replay."
            )

        return ProcessingRunContext(
            processing_run_id=(
                bronze_processing_run_id
            ),
            processing_started_at_utc=(
                expected_started_at
            ),
            git_commit_sha=(
                expected_commit
            ),
            git_worktree_dirty=(
                expected_dirty
            ),
            git_branch=(
                expected_branch
            ),
        )

    # --------------------------------------------------------
    # Replay / nouveau run Silver
    # --------------------------------------------------------

    return build_processing_run_context(
        processing_run_id=(
            target_processing_run_id
        ),
        repo_dir=ROOT_DIR,
    )


# ============================================================
# Nettoyage
# ============================================================

def clean_string_value(
    value: Any,
) -> Any:
    """
    Trim des chaînes.
    Les chaînes vides deviennent None.
    """

    if not isinstance(
        value,
        str,
    ):
        return value

    stripped = (
        value.strip()
    )

    if not stripped:
        return None

    return stripped


def clean_string_columns(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Nettoyage léger de toutes les colonnes object.
    """

    object_columns = (
        df.select_dtypes(
            include="object"
        )
        .columns
    )

    for column in object_columns:

        df[column] = (
            df[column]
            .apply(
                clean_string_value
            )
        )

    return df


# ============================================================
# Dates
# ============================================================

def parse_dates(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Convertit les dates fonctionnelles et techniques
    vers datetime UTC.
    """

    df[
        "date_creation"
    ] = pd.to_datetime(
        df[
            "date_creation"
        ],
        utc=True,
        errors="coerce",
    )

    df[
        "date_actualisation"
    ] = pd.to_datetime(
        df[
            "date_actualisation"
        ],
        utc=True,
        errors="coerce",
    )

    df[
        "date_ingestion"
    ] = pd.to_datetime(
        df[
            "date_ingestion"
        ],
        format="%Y-%m-%d_%H-%M-%S",
        utc=True,
        errors="coerce",
    )

    return df


def compute_date_coherence(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    date_actualisation_coherente :

    True
        date_actualisation >= date_creation

    False
        date_actualisation < date_creation

    NA
        une des deux dates est absente
    """

    coherence = pd.array(
        [
            pd.NA
        ]
        * len(
            df
        ),
        dtype="boolean",
    )

    both_valid = (
        df[
            "date_creation"
        ]
        .notna()
        & df[
            "date_actualisation"
        ]
        .notna()
    )

    coherence[
        both_valid.to_numpy()
    ] = (
        df.loc[
            both_valid,
            "date_actualisation",
        ]
        >= df.loc[
            both_valid,
            "date_creation",
        ]
    ).to_numpy()

    df[
        "date_actualisation_coherente"
    ] = coherence

    return df


# ============================================================
# IDs
# ============================================================

def validate_unique_ids(
    df: pd.DataFrame,
) -> None:
    """
    Silver ne corrige pas silencieusement les IDs.

    Bronze étant déjà certifié :
    - aucun ID manquant ;
    - aucun doublon accepté.
    """

    missing_mask = (
        df[
            "id_offre"
        ]
        .isna()
        |
        df[
            "id_offre"
        ]
        .astype(str)
        .str.strip()
        .eq("")
    )

    missing_ids = int(
        missing_mask.sum()
    )

    if missing_ids > 0:
        raise RuntimeError(
            f"{missing_ids} id_offre "
            "manquant(s)."
        )

    duplicated_ids = int(
        df[
            "id_offre"
        ]
        .astype(str)
        .duplicated()
        .sum()
    )

    if duplicated_ids > 0:
        raise RuntimeError(
            f"{duplicated_ids} doublon(s) "
            "id_offre détecté(s)."
        )


# ============================================================
# Entreprise
# ============================================================

def apply_entreprise_indicators(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Indicateurs de disponibilité entreprise.
    """

    df[
        "entreprise_renseignee"
    ] = (
        df[
            "nom_entreprise"
        ]
        .notna()
    )

    df[
        "description_entreprise_renseignee"
    ] = (
        df[
            "description_entreprise"
        ]
        .notna()
    )

    df[
        "information_entreprise_disponible"
    ] = (
        df[
            "entreprise_renseignee"
        ]
        | df[
            "description_entreprise_renseignee"
        ]
    )

    return df


# ============================================================
# Salaire
# ============================================================

PERIOD_PATTERN = re.compile(
    r"^\s*(Annuel|Mensuel|Horaire)",
    re.IGNORECASE,
)


def extract_salary_period(
    text: Optional[str],
) -> Optional[str]:
    """
    Extrait la période du salaire :
    Annuel, Mensuel ou Horaire.
    """

    if (
        not isinstance(
            text,
            str,
        )
        or not text.strip()
    ):
        return None

    match = (
        PERIOD_PATTERN.match(
            text
        )
    )

    if not match:
        return None

    return (
        match
        .group(1)
        .capitalize()
    )


def apply_salary_indicators(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Indicateurs salaire Silver.
    """

    df[
        "salaire_renseigne"
    ] = (
        df[
            "salaire_libelle"
        ]
        .notna()
    )

    df[
        "periode_salaire"
    ] = (
        df[
            "salaire_libelle"
        ]
        .apply(
            extract_salary_period
        )
    )

    return df


# ============================================================
# Localisation
# ============================================================

def apply_location_indicators(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Crée deux indicateurs distincts.

    coordonnees_renseignees :
        latitude ET longitude présentes.

    localisation_renseignee :
        au moins une information géographique exploitable :
        - libellé lieu ;
        - code commune ;
        - code postal ;
        - coordonnées GPS.

    Cette distinction évite d'assimiler absence de GPS
    et absence de localisation.
    """

    df[
        "coordonnees_renseignees"
    ] = (
        df[
            "latitude"
        ]
        .notna()
        & df[
            "longitude"
        ]
        .notna()
    )

    df[
        "localisation_renseignee"
    ] = (
        df[
            "libelle_lieu_travail"
        ]
        .notna()
        | df[
            "code_commune"
        ]
        .notna()
        | df[
            "code_postal"
        ]
        .notna()
        | df[
            "coordonnees_renseignees"
        ]
    )

    return df


# ============================================================
# Booléens
# ============================================================

def normalize_boolean_value(
    value: Any,
) -> Any:
    """
    Normalise les booléens provenant de la source.
    """

    if pd.isna(
        value
    ):
        return pd.NA

    if isinstance(
        value,
        bool,
    ):
        return value

    if isinstance(
        value,
        str,
    ):

        normalized = (
            value
            .strip()
            .lower()
        )

        if normalized in {
            "true",
            "1",
            "yes",
            "oui",
        }:
            return True

        if normalized in {
            "false",
            "0",
            "no",
            "non",
        }:
            return False

    return value


# ============================================================
# Typage final
# ============================================================

def apply_final_typing(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Applique les types Silver.
    """

    df[
        "id_offre"
    ] = (
        df[
            "id_offre"
        ]
        .astype(
            "string"
        )
    )

    df[
        "nombre_postes"
    ] = (
        df[
            "nombre_postes"
        ]
        .astype(
            "Int64"
        )
    )

    for column in (
        BOOLEAN_COLUMNS_FULL
    ):

        df[
            column
        ] = (
            df[
                column
            ]
            .apply(
                normalize_boolean_value
            )
            .astype(
                "boolean"
            )
        )

    for column in (
        BOOLEAN_COLUMNS_NULLABLE
    ):

        df[
            column
        ] = (
            df[
                column
            ]
            .apply(
                normalize_boolean_value
            )
            .astype(
                "boolean"
            )
        )

    calculated_boolean_columns = [
        "date_actualisation_coherente",
        "entreprise_renseignee",
        "description_entreprise_renseignee",
        "information_entreprise_disponible",
        "salaire_renseigne",
        "coordonnees_renseignees",
        "localisation_renseignee",
    ]

    for column in (
        calculated_boolean_columns
    ):

        df[
            column
        ] = (
            df[
                column
            ]
            .astype(
                "boolean"
            )
        )

    return df


# ============================================================
# Garantie de non-perte
# ============================================================

def validate_non_loss(
    bronze_ids: set[str],
    silver_df: pd.DataFrame,
    bronze_row_count: int,
) -> None:
    """
    Vérifie strictement Bronze = Silver au niveau offre.
    """

    if (
        len(
            silver_df
        )
        != bronze_row_count
    ):

        raise RuntimeError(
            "Perte de lignes Bronze -> Silver : "
            f"Bronze={bronze_row_count}, "
            f"Silver={len(silver_df)}."
        )

    silver_ids = set(
        silver_df[
            "id_offre"
        ]
        .astype(str)
        .tolist()
    )

    missing_ids = (
        bronze_ids
        - silver_ids
    )

    unexpected_ids = (
        silver_ids
        - bronze_ids
    )

    if missing_ids:
        raise RuntimeError(
            f"{len(missing_ids)} "
            "offre(s) Bronze absente(s) "
            "du Silver."
        )

    if unexpected_ids:
        raise RuntimeError(
            f"{len(unexpected_ids)} "
            "offre(s) Silver absente(s) "
            "du Bronze."
        )


# ============================================================
# Transformation principale
# ============================================================

def run_silver_transformation(
    bronze_object_key: str,
    processing_run_id: str | None = None,
) -> dict[str, Any]:
    """
    Transforme Bronze processing_v2 vers Silver processing_v2.

    Le traitement garantit :

    - parent Bronze explicitement identifié ;
    - intégrité SHA-256 du parent ;
    - zéro perte d'offre ;
    - distinction du run Bronze et du run Silver ;
    - provenance Git ;
    - schéma versionné ;
    - stockage immutable R2.
    """

    logger.info(
        "Début transformation Bronze -> Silver"
    )

    # ========================================================
    # 1. Parsing clé Bronze
    # ========================================================

    bronze_parts = (
        parse_data_lake_object_key(
            bronze_object_key
        )
    )

    if (
        bronze_parts[
            "layer"
        ]
        != "bronze"
    ):
        raise ValueError(
            "L'objet source doit appartenir "
            "à la couche Bronze."
        )

    if (
        bronze_parts[
            "path_version"
        ]
        != EXPECTED_BRONZE_PATH_VERSION
    ):
        raise RuntimeError(
            "Silver v2 exige un parent Bronze "
            "processing_v2."
        )

    source = str(
        bronze_parts[
            "source"
        ]
    )

    ingestion_date = str(
        bronze_parts[
            "ingestion_date"
        ]
    )

    batch_id = str(
        bronze_parts[
            "batch_id"
        ]
    )

    bronze_processing_run_id = (
        bronze_parts[
            "processing_run_id"
        ]
    )

    if not bronze_processing_run_id:
        raise RuntimeError(
            "processing_run_id absent "
            "du chemin Bronze."
        )

    bronze_processing_run_id = str(
        bronze_processing_run_id
    )

    if (
        source
        != SOURCE_NAME
    ):
        raise RuntimeError(
            "Source Bronze inattendue : "
            f"{source}"
        )

    # ========================================================
    # 2. R2
    # ========================================================

    storage = R2Storage()

    if not storage.object_exists(
        bronze_object_key
    ):
        raise FileNotFoundError(
            "Objet Bronze introuvable : "
            f"{bronze_object_key}"
        )

    bronze_info = (
        storage.get_object_info(
            bronze_object_key
        )
    )

    bronze_metadata = (
        bronze_info.get(
            "metadata",
            {},
        )
    )

    # ========================================================
    # 3. Contrat Bronze
    # ========================================================

    expected_bronze_metadata = {
        "layer": "bronze",
        "source": source,
        "batch-id": batch_id,
        "ingestion-date-utc": (
            ingestion_date
        ),
        "processing-run-id": (
            bronze_processing_run_id
        ),
        "schema-version": (
            EXPECTED_BRONZE_SCHEMA_VERSION
        ),
        "path-version": (
            EXPECTED_BRONZE_PATH_VERSION
        ),
    }

    for key, expected_value in (
        expected_bronze_metadata.items()
    ):

        actual_value = (
            bronze_metadata.get(
                key
            )
        )

        if (
            actual_value
            != expected_value
        ):
            raise RuntimeError(
                "Métadonnée Bronze incohérente : "
                f"{key}. "
                f"Attendu={expected_value}, "
                f"obtenu={actual_value}"
            )

    # ========================================================
    # 4. SHA parent Bronze
    # ========================================================

    bronze_bytes = (
        storage.download_bytes(
            bronze_object_key
        )
    )

    if not bronze_bytes:
        raise RuntimeError(
            "Le Bronze téléchargé est vide."
        )

    if (
        len(
            bronze_bytes
        )
        != bronze_info[
            "size_bytes"
        ]
    ):
        raise RuntimeError(
            "Taille Bronze incohérente."
        )

    bronze_sha256 = (
        hashlib.sha256(
            bronze_bytes
        )
        .hexdigest()
    )

    if (
        bronze_sha256
        != bronze_metadata.get(
            "sha256"
        )
    ):
        raise RuntimeError(
            "SHA-256 Bronze invalide."
        )

    # ========================================================
    # 5. Contexte Silver
    # ========================================================

    context = (
        resolve_silver_processing_context(
            bronze_metadata=(
                bronze_metadata
            ),
            bronze_processing_run_id=(
                bronze_processing_run_id
            ),
            requested_processing_run_id=(
                processing_run_id
            ),
        )
    )

    silver_processing_run_id = (
        context.processing_run_id
    )

    logger.info(
        "Bronze processing_run_id : "
        f"{bronze_processing_run_id}"
    )

    logger.info(
        "Silver processing_run_id : "
        f"{silver_processing_run_id}"
    )

    # ========================================================
    # 6. Lecture Bronze
    # ========================================================

    try:

        df = pd.read_parquet(
            BytesIO(
                bronze_bytes
            ),
            engine="pyarrow",
        )

    except Exception as exc:

        raise RuntimeError(
            "Impossible de lire "
            "le Parquet Bronze."
        ) from exc

    bronze_row_count = len(
        df
    )

    if bronze_row_count == 0:
        raise RuntimeError(
            "Dataset Bronze vide."
        )

    expected_record_count = (
        bronze_metadata.get(
            "record-count"
        )
    )

    if expected_record_count is None:
        raise RuntimeError(
            "record-count Bronze absent."
        )

    try:

        expected_record_count_int = int(
            expected_record_count
        )

    except ValueError as exc:

        raise RuntimeError(
            "record-count Bronze invalide."
        ) from exc

    if (
        expected_record_count_int
        != bronze_row_count
    ):
        raise RuntimeError(
            "Volume Bronze incohérent."
        )

    # ========================================================
    # 7. Schéma
    # ========================================================

    validate_required_columns(
        df
    )

    # ========================================================
    # 8. Contrôles techniques Bronze
    # ========================================================

    if (
        df[
            "batch_id"
        ]
        .isna()
        .any()
    ):
        raise RuntimeError(
            "batch_id manquant dans Bronze."
        )

    if set(
        df[
            "batch_id"
        ]
        .astype(str)
        .unique()
    ) != {
        batch_id
    }:
        raise RuntimeError(
            "batch_id incohérent dans Bronze."
        )

    if (
        df[
            "processing_run_id"
        ]
        .isna()
        .any()
    ):
        raise RuntimeError(
            "processing_run_id manquant "
            "dans Bronze."
        )

    if set(
        df[
            "processing_run_id"
        ]
        .astype(str)
        .unique()
    ) != {
        bronze_processing_run_id
    }:
        raise RuntimeError(
            "processing_run_id Bronze "
            "incohérent."
        )

    if set(
        df[
            "bronze_schema_version"
        ]
        .dropna()
        .astype(str)
        .unique()
    ) != {
        EXPECTED_BRONZE_SCHEMA_VERSION
    }:
        raise RuntimeError(
            "bronze_schema_version "
            "incohérent."
        )

    # ========================================================
    # 9. IDs Bronze
    # ========================================================

    missing_bronze_ids = int(
        (
            df[
                "id"
            ]
            .isna()
            |
            df[
                "id"
            ]
            .astype(str)
            .str.strip()
            .eq("")
        )
        .sum()
    )

    if missing_bronze_ids > 0:
        raise RuntimeError(
            f"{missing_bronze_ids} "
            "ID Bronze manquant(s)."
        )

    duplicate_bronze_ids = int(
        df[
            "id"
        ]
        .astype(str)
        .duplicated()
        .sum()
    )

    if duplicate_bronze_ids > 0:
        raise RuntimeError(
            f"{duplicate_bronze_ids} "
            "doublon(s) Bronze."
        )

    bronze_ids = set(
        df[
            "id"
        ]
        .astype(str)
        .tolist()
    )

    # ========================================================
    # 10. Suppression colonne explicitement exclue
    # ========================================================

    df = df.drop(
        columns=(
            EXCLUDED_COLUMNS
        ),
        errors="ignore",
    )

    # ========================================================
    # 11. Renommage
    # ========================================================

    df = df.rename(
        columns=(
            RENAME_MAP
        )
    )

    # ========================================================
    # 12. Nettoyage
    # ========================================================

    df = clean_string_columns(
        df
    )

    if (
        "mot_cle_recherche"
        in df.columns
    ):

        df[
            "mot_cle_recherche"
        ] = (
            df[
                "mot_cle_recherche"
            ]
            .str.lower()
        )

    # ========================================================
    # 13. Dates
    # ========================================================

    df = parse_dates(
        df
    )

    # ========================================================
    # 14. IDs Silver
    # ========================================================

    validate_unique_ids(
        df
    )

    # ========================================================
    # 15. Règles métier Silver
    # ========================================================

    df = compute_date_coherence(
        df
    )

    df = apply_entreprise_indicators(
        df
    )

    df = apply_salary_indicators(
        df
    )

    df = apply_location_indicators(
        df
    )

    # ========================================================
    # 16. Colonnes techniques Silver
    # ========================================================

    processing_time = (
        datetime.now(
            timezone.utc
        )
    )

    df[
        "processing_run_id"
    ] = (
        silver_processing_run_id
    )

    df[
        "silver_schema_version"
    ] = (
        SILVER_SCHEMA_VERSION
    )

    df[
        "objet_source_bronze"
    ] = (
        bronze_object_key
    )

    df[
        "date_traitement_silver"
    ] = pd.Timestamp(
        processing_time
    )

    # ========================================================
    # 17. Typage
    # ========================================================

    df = apply_final_typing(
        df
    )

    # ========================================================
    # 18. Garantie non-perte
    # ========================================================

    validate_non_loss(
        bronze_ids=(
            bronze_ids
        ),
        silver_df=(
            df
        ),
        bronze_row_count=(
            bronze_row_count
        ),
    )

    silver_row_count = len(
        df
    )

    unique_ids = int(
        df[
            "id_offre"
        ]
        .nunique(
            dropna=True
        )
    )

    # ========================================================
    # 19. Lineage Raw hérité
    # ========================================================

    raw_object_key = (
        bronze_metadata.get(
            "raw-object-key"
        )
    )

    raw_sha256 = (
        bronze_metadata.get(
            "raw-sha256"
        )
    )

    if not raw_object_key:
        raise RuntimeError(
            "raw-object-key absent "
            "du Bronze."
        )

    if not raw_sha256:
        raise RuntimeError(
            "raw-sha256 absent "
            "du Bronze."
        )

    # ========================================================
    # 20. Construction clé Silver
    # ========================================================

    silver_object_key = (
        build_silver_object_key(
            source=source,
            batch_id=batch_id,
            ingestion_date=(
                ingestion_date
            ),
            processing_run_id=(
                silver_processing_run_id
            ),
            filename="offres.parquet",
        )
    )

    parsed_silver_key = (
        parse_data_lake_object_key(
            silver_object_key
        )
    )

    if (
        parsed_silver_key[
            "path_version"
        ]
        != SILVER_PATH_VERSION
    ):
        raise RuntimeError(
            "Le chemin Silver généré "
            "n'utilise pas processing_v2."
        )

    # ========================================================
    # 21. Sérialisation
    # ========================================================

    buffer = BytesIO()

    df.to_parquet(
        buffer,
        index=False,
        engine="pyarrow",
    )

    silver_bytes = (
        buffer.getvalue()
    )

    if not silver_bytes:
        raise RuntimeError(
            "Parquet Silver vide."
        )

    # ========================================================
    # 22. Métadonnées Silver
    # ========================================================

    silver_metadata = {
        # ----------------------------------------------------
        # Identité
        # ----------------------------------------------------
        "layer": "silver",
        "source": source,

        # ----------------------------------------------------
        # Dataset
        # ----------------------------------------------------
        "batch-id": (
            batch_id
        ),

        "ingestion-date-utc": (
            ingestion_date
        ),

        # ----------------------------------------------------
        # Contrat Silver
        # ----------------------------------------------------
        "schema-version": (
            SILVER_SCHEMA_VERSION
        ),

        "path-version": (
            SILVER_PATH_VERSION
        ),

        # ----------------------------------------------------
        # Volumes
        # ----------------------------------------------------
        "record-count": str(
            silver_row_count
        ),

        "column-count": str(
            df.shape[
                1
            ]
        ),

        # ----------------------------------------------------
        # Parent générique
        # ----------------------------------------------------
        "parent-layer": "bronze",

        "parent-object-key": (
            bronze_object_key
        ),

        "parent-sha256": (
            bronze_sha256
        ),

        "parent-processing-run-id": (
            bronze_processing_run_id
        ),

        "parent-schema-version": (
            EXPECTED_BRONZE_SCHEMA_VERSION
        ),

        # ----------------------------------------------------
        # Lineage explicite Bronze
        # ----------------------------------------------------
        "bronze-object-key": (
            bronze_object_key
        ),

        "bronze-sha256": (
            bronze_sha256
        ),

        "bronze-processing-run-id": (
            bronze_processing_run_id
        ),

        # ----------------------------------------------------
        # Lineage Raw
        # ----------------------------------------------------
        "raw-object-key": (
            raw_object_key
        ),

        "raw-sha256": (
            raw_sha256
        ),
    }

    silver_metadata.update(
        context.to_r2_metadata()
    )

    # ========================================================
    # 23. Immutabilité
    # ========================================================

    if storage.object_exists(
        silver_object_key
    ):
        raise RuntimeError(
            "L'objet Silver existe déjà : "
            f"{silver_object_key}"
        )

    # ========================================================
    # 24. Upload
    # ========================================================

    upload_result = (
        storage.upload_bytes(
            object_key=(
                silver_object_key
            ),
            data=(
                silver_bytes
            ),
            content_type=(
                "application/vnd.apache.parquet"
            ),
            metadata=(
                silver_metadata
            ),
            overwrite=False,
        )
    )

    # ========================================================
    # 25. Validation post-upload
    # ========================================================

    if not storage.object_exists(
        silver_object_key
    ):
        raise RuntimeError(
            "Objet Silver inaccessible "
            "après upload."
        )

    uploaded_info = (
        storage.get_object_info(
            silver_object_key
        )
    )

    uploaded_metadata = (
        uploaded_info.get(
            "metadata",
            {},
        )
    )

    if (
        uploaded_metadata.get(
            "processing-run-id"
        )
        != silver_processing_run_id
    ):
        raise RuntimeError(
            "processing_run_id Silver "
            "incohérent après upload."
        )

    if (
        uploaded_metadata.get(
            "schema-version"
        )
        != SILVER_SCHEMA_VERSION
    ):
        raise RuntimeError(
            "schema_version Silver "
            "incohérent après upload."
        )

    # ========================================================
    # 26. Métriques
    # ========================================================

    localisation_count = int(
        df[
            "localisation_renseignee"
        ]
        .fillna(False)
        .sum()
    )

    coordinates_count = int(
        df[
            "coordonnees_renseignees"
        ]
        .fillna(False)
        .sum()
    )

    salary_count = int(
        df[
            "salaire_renseigne"
        ]
        .fillna(False)
        .sum()
    )

    entreprise_count = int(
        df[
            "information_entreprise_disponible"
        ]
        .fillna(False)
        .sum()
    )

    # ========================================================
    # 27. Résumé
    # ========================================================

    print()
    print("=" * 70)
    print(
        "TRANSFORMATION BRONZE -> SILVER V2 TERMINÉE"
    )
    print("=" * 70)
    print()

    print(
        f"Batch ID                : "
        f"{batch_id}"
    )

    print(
        f"Bronze Processing Run   : "
        f"{bronze_processing_run_id}"
    )

    print(
        f"Silver Processing Run   : "
        f"{silver_processing_run_id}"
    )

    print(
        f"Silver Schema Version   : "
        f"{SILVER_SCHEMA_VERSION}"
    )

    print(
        f"Path Version            : "
        f"{SILVER_PATH_VERSION}"
    )

    print(
        f"Lignes Bronze           : "
        f"{bronze_row_count}"
    )

    print(
        f"Lignes Silver           : "
        f"{silver_row_count}"
    )

    print(
        f"IDs uniques             : "
        f"{unique_ids}"
    )

    print(
        f"Colonnes Silver         : "
        f"{df.shape[1]}"
    )

    print(
        f"Localisation renseignée : "
        f"{localisation_count} "
        f"({localisation_count / silver_row_count * 100:.2f} %)"
    )

    print(
        f"Coordonnées GPS         : "
        f"{coordinates_count} "
        f"({coordinates_count / silver_row_count * 100:.2f} %)"
    )

    print(
        f"Salaire renseigné       : "
        f"{salary_count} "
        f"({salary_count / silver_row_count * 100:.2f} %)"
    )

    print(
        f"Information entreprise  : "
        f"{entreprise_count} "
        f"({entreprise_count / silver_row_count * 100:.2f} %)"
    )

    print(
        f"Git branch              : "
        f"{context.git_branch}"
    )

    print(
        f"Git commit              : "
        f"{context.git_commit_sha}"
    )

    print(
        f"Git worktree dirty      : "
        f"{context.git_worktree_dirty}"
    )

    print(
        f"Objet Bronze            : "
        f"{bronze_object_key}"
    )

    print(
        f"Objet Silver            : "
        f"{silver_object_key}"
    )

    print(
        f"Taille Silver           : "
        f"{upload_result['size_bytes']} octets"
    )

    print(
        f"SHA-256 Silver          : "
        f"{upload_result['sha256']}"
    )

    print()

    return {
        "batch_id": (
            batch_id
        ),

        "bronze_processing_run_id": (
            bronze_processing_run_id
        ),

        "processing_run_id": (
            silver_processing_run_id
        ),

        "source": (
            source
        ),

        "ingestion_date": (
            ingestion_date
        ),

        "schema_version": (
            SILVER_SCHEMA_VERSION
        ),

        "path_version": (
            SILVER_PATH_VERSION
        ),

        "raw_object_key": (
            raw_object_key
        ),

        "bronze_object_key": (
            bronze_object_key
        ),

        "silver_object_key": (
            silver_object_key
        ),

        "nombre_lignes_bronze": (
            bronze_row_count
        ),

        "nombre_lignes_silver": (
            silver_row_count
        ),

        "nombre_ids_uniques": (
            unique_ids
        ),

        "nombre_colonnes_silver": (
            df.shape[
                1
            ]
        ),

        "localisation_renseignee": (
            localisation_count
        ),

        "coordonnees_renseignees": (
            coordinates_count
        ),

        "salaires_renseignes": (
            salary_count
        ),

        "information_entreprise_disponible": (
            entreprise_count
        ),

        "git_commit_sha": (
            context.git_commit_sha
        ),

        "git_worktree_dirty": (
            context.git_worktree_dirty
        ),

        "git_branch": (
            context.git_branch
        ),

        "processing_started_at_utc": (
            context.processing_started_at_utc
        ),

        "size_bytes": (
            upload_result[
                "size_bytes"
            ]
        ),

        "sha256": (
            upload_result[
                "sha256"
            ]
        ),
    }


# ============================================================
# CLI
# ============================================================

def main() -> None:
    """
    CLI Bronze -> Silver.

    Pipeline normal :

        aucun --processing-run-id nécessaire.
        Le run Bronze est propagé.

    Replay Silver :

        fournir explicitement un nouvel UUID avec
        --processing-run-id.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Transformation Bronze processing_v2 "
            "France Travail vers Silver processing_v2 "
            "dans Cloudflare R2."
        )
    )

    parser.add_argument(
        "--bronze-object-key",
        required=True,
        help=(
            "Clé du Parquet Bronze processing_v2 "
            "dans Cloudflare R2."
        ),
    )

    parser.add_argument(
        "--processing-run-id",
        required=False,
        default=None,
        help=(
            "Nouvel UUID uniquement pour un replay "
            "Silver indépendant. "
            "S'il est absent, le processing_run_id "
            "du Bronze parent est propagé."
        ),
    )

    args = parser.parse_args()

    run_silver_transformation(
        bronze_object_key=(
            args.bronze_object_key
        ),
        processing_run_id=(
            args.processing_run_id
        ),
    )


if __name__ == "__main__":
    main()