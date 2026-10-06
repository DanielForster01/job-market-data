import argparse
import hashlib
import json
import re
from datetime import datetime
from io import BytesIO
from typing import Any, Optional

import pandas as pd

from src.storage.r2_paths import (
    parse_data_lake_object_key,
)
from src.storage.r2_storage import R2Storage


# ============================================================
# Contrats attendus
# ============================================================

EXPECTED_BRONZE_SCHEMA_VERSION = "2.0.0"
EXPECTED_BRONZE_PATH_VERSION = "processing_v2"

EXPECTED_SILVER_SCHEMA_VERSION = "2.0.0"
EXPECTED_SILVER_PATH_VERSION = "processing_v2"


# ============================================================
# Salaire
# ============================================================

PERIOD_PATTERN = re.compile(
    r"^\s*(Annuel|Mensuel|Horaire)",
    re.IGNORECASE,
)


# ============================================================
# Helpers métadonnées
# ============================================================

def parse_integer_metadata(
    metadata: dict[str, str],
    key: str,
    allow_zero: bool = False,
) -> int:
    """
    Lit et valide une métadonnée entière.
    """

    value = metadata.get(
        key
    )

    if value is None:
        raise RuntimeError(
            f"Métadonnée obligatoire absente : {key}"
        )

    try:

        parsed_value = int(
            value
        )

    except ValueError as exc:

        raise RuntimeError(
            f"Métadonnée entière invalide : "
            f"{key}={value}"
        ) from exc

    minimum = (
        0
        if allow_zero
        else 1
    )

    if parsed_value < minimum:
        raise RuntimeError(
            f"Métadonnée {key} invalide : "
            f"{parsed_value}"
        )

    return parsed_value


def validate_utc_timestamp(
    value: str,
    field_name: str,
) -> None:
    """
    Vérifie un timestamp ISO 8601 UTC terminé par Z.
    """

    if (
        not isinstance(
            value,
            str,
        )
        or not value
    ):
        raise RuntimeError(
            f"{field_name} absent ou invalide."
        )

    if not value.endswith(
        "Z"
    ):
        raise RuntimeError(
            f"{field_name} doit être exprimé "
            "en UTC avec suffixe Z."
        )

    try:

        datetime.fromisoformat(
            value.replace(
                "Z",
                "+00:00",
            )
        )

    except ValueError as exc:

        raise RuntimeError(
            f"{field_name} n'est pas "
            "un timestamp ISO valide."
        ) from exc


# ============================================================
# Provenance Git
# ============================================================

def validate_git_provenance(
    metadata: dict[str, str],
) -> dict[str, Any]:
    """
    Valide la provenance Git enregistrée
    dans les métadonnées R2.
    """

    git_commit_sha = (
        metadata.get(
            "git-commit-sha"
        )
    )

    if not git_commit_sha:
        raise RuntimeError(
            "git-commit-sha absent."
        )

    if not re.fullmatch(
        r"[0-9a-f]{40}|[0-9a-f]{64}",
        git_commit_sha,
    ):
        raise RuntimeError(
            "git-commit-sha invalide."
        )

    git_branch = (
        metadata.get(
            "git-branch"
        )
    )

    if (
        not isinstance(
            git_branch,
            str,
        )
        or not git_branch.strip()
    ):
        raise RuntimeError(
            "git-branch absent ou invalide."
        )

    git_dirty_raw = (
        metadata.get(
            "git-worktree-dirty"
        )
    )

    if git_dirty_raw not in {
        "true",
        "false",
    }:
        raise RuntimeError(
            "git-worktree-dirty doit valoir "
            "'true' ou 'false'."
        )

    processing_started_at = (
        metadata.get(
            "processing-started-at-utc"
        )
    )

    validate_utc_timestamp(
        processing_started_at,
        "processing-started-at-utc",
    )

    return {
        "git_commit_sha": (
            git_commit_sha
        ),
        "git_branch": (
            git_branch
        ),
        "git_worktree_dirty": (
            git_dirty_raw
            == "true"
        ),
        "processing_started_at_utc": (
            processing_started_at
        ),
    }


# ============================================================
# Normalisation mots-clés
# ============================================================

def normalize_keyword(
    value: Any,
) -> Optional[str]:
    """
    Normalise uniquement pour les contrôles qualité.

    Exemple :
        BI analyst
            ->
        bi analyst
    """

    if not isinstance(
        value,
        str,
    ):
        return None

    normalized = (
        value
        .strip()
        .casefold()
    )

    if not normalized:
        return None

    return normalized


# ============================================================
# Salaire attendu
# ============================================================

def extract_salary_period(
    value: Any,
) -> Optional[str]:
    """
    Reproduit la règle métier Silver
    pour periode_salaire.
    """

    if (
        not isinstance(
            value,
            str,
        )
        or not value.strip()
    ):
        return None

    match = (
        PERIOD_PATTERN.match(
            value
        )
    )

    if not match:
        return None

    return (
        match
        .group(1)
        .capitalize()
    )


# ============================================================
# Validation indicateur booléen
# ============================================================

def validate_boolean_indicator(
    df: pd.DataFrame,
    column: str,
    expected: pd.Series,
) -> None:
    """
    Vérifie qu'un indicateur booléen calculé
    correspond exactement à la règle attendue.
    """

    if column not in df.columns:
        raise RuntimeError(
            f"Colonne absente : {column}"
        )

    null_count = int(
        df[
            column
        ]
        .isna()
        .sum()
    )

    if null_count > 0:
        raise RuntimeError(
            f"{column} contient "
            f"{null_count} NULL."
        )

    actual = (
        df[
            column
        ]
        .astype(bool)
    )

    expected_boolean = (
        expected
        .astype(bool)
    )

    mismatch_count = int(
        (
            actual
            != expected_boolean
        )
        .sum()
    )

    if mismatch_count > 0:
        raise RuntimeError(
            f"{mismatch_count} incohérence(s) "
            f"dans {column}."
        )


# ============================================================
# Cohérence dates
# ============================================================

def validate_date_coherence(
    df: pd.DataFrame,
) -> None:
    """
    Recalcule date_actualisation_coherente.

    True :
        actualisation >= création

    False :
        actualisation < création

    NA :
        au moins une date absente
    """

    column = (
        "date_actualisation_coherente"
    )

    expected = pd.Series(
        pd.NA,
        index=df.index,
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

    expected.loc[
        both_valid
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

    actual = (
        df[
            column
        ]
        .astype(
            "boolean"
        )
    )

    both_null = (
        actual.isna()
        & expected.isna()
    )

    equal_values = (
        actual
        .eq(
            expected
        )
        .fillna(
            False
        )
    )

    coherent = (
        both_null
        | equal_values
    )

    mismatch_count = int(
        (
            ~coherent
        )
        .sum()
    )

    if mismatch_count > 0:
        raise RuntimeError(
            f"{mismatch_count} incohérence(s) "
            "dans date_actualisation_coherente."
        )


# ============================================================
# Quality Gate principal
# ============================================================

def validate_silver_object(
    silver_object_key: str,
) -> dict[str, Any]:
    """
    Quality Gate complet Silver processing_v2.

    Le contrôle remonte la totalité du lineage :

        Raw
          ↓
        Bronze processing_v2
          ↓
        Silver processing_v2

    Les vérifications couvrent :

    - chemin ;
    - schéma ;
    - processing_run_id ;
    - parent exact ;
    - SHA-256 ;
    - provenance Git ;
    - volumes ;
    - ensembles d'IDs ;
    - données Raw conservées ;
    - indicateurs métier ;
    - localisation ;
    - dates ;
    - mots-clés d'acquisition.
    """

    storage = R2Storage()

    print()
    print("=" * 70)
    print(
        "CONTRÔLE QUALITÉ SILVER R2 - PROCESSING V2"
    )
    print("=" * 70)
    print()

    # ========================================================
    # 1. Parsing Silver
    # ========================================================

    silver_parts = (
        parse_data_lake_object_key(
            silver_object_key
        )
    )

    if (
        silver_parts[
            "layer"
        ]
        != "silver"
    ):
        raise RuntimeError(
            "L'objet fourni n'appartient "
            "pas à la couche Silver."
        )

    if (
        silver_parts[
            "path_version"
        ]
        != EXPECTED_SILVER_PATH_VERSION
    ):
        raise RuntimeError(
            "Ce Quality Gate accepte uniquement "
            "les Silver processing_v2."
        )

    source = str(
        silver_parts[
            "source"
        ]
    )

    ingestion_date = str(
        silver_parts[
            "ingestion_date"
        ]
    )

    batch_id = str(
        silver_parts[
            "batch_id"
        ]
    )

    silver_processing_run_id = (
        silver_parts[
            "processing_run_id"
        ]
    )

    if not silver_processing_run_id:
        raise RuntimeError(
            "processing_run_id absent "
            "du chemin Silver."
        )

    silver_processing_run_id = str(
        silver_processing_run_id
    )

    print(
        "[OK] Convention de chemin Silver processing_v2"
    )

    # ========================================================
    # 2. Existence Silver
    # ========================================================

    if not storage.object_exists(
        silver_object_key
    ):
        raise FileNotFoundError(
            "Objet Silver introuvable : "
            f"{silver_object_key}"
        )

    print(
        "[OK] Objet Silver présent dans R2"
    )

    # ========================================================
    # 3. Métadonnées Silver
    # ========================================================

    silver_info = (
        storage.get_object_info(
            silver_object_key
        )
    )

    silver_metadata = (
        silver_info.get(
            "metadata",
            {},
        )
    )

    print(
        "[OK] Métadonnées Silver accessibles"
    )

    # ========================================================
    # 4. Identité Silver
    # ========================================================

    expected_silver_metadata = {
        "layer": "silver",
        "source": source,
        "batch-id": batch_id,
        "ingestion-date-utc": (
            ingestion_date
        ),
        "processing-run-id": (
            silver_processing_run_id
        ),
        "schema-version": (
            EXPECTED_SILVER_SCHEMA_VERSION
        ),
        "path-version": (
            EXPECTED_SILVER_PATH_VERSION
        ),
    }

    for key, expected_value in (
        expected_silver_metadata.items()
    ):

        actual_value = (
            silver_metadata.get(
                key
            )
        )

        if (
            actual_value
            != expected_value
        ):
            raise RuntimeError(
                "Métadonnée Silver incohérente : "
                f"{key}. "
                f"Attendu={expected_value}, "
                f"obtenu={actual_value}"
            )

    print(
        f"[OK] Batch ID cohérent : {batch_id}"
    )

    print(
        "[OK] Processing Run ID Silver cohérent : "
        f"{silver_processing_run_id}"
    )

    print(
        "[OK] Silver schema version : "
        f"{EXPECTED_SILVER_SCHEMA_VERSION}"
    )

    print(
        "[OK] Silver path version : "
        f"{EXPECTED_SILVER_PATH_VERSION}"
    )

    # ========================================================
    # 5. Provenance Git Silver
    # ========================================================

    silver_git = (
        validate_git_provenance(
            silver_metadata
        )
    )

    print(
        "[OK] Provenance Git Silver valide"
    )

    # ========================================================
    # 6. Téléchargement Silver
    # ========================================================

    silver_bytes = (
        storage.download_bytes(
            silver_object_key
        )
    )

    if not silver_bytes:
        raise RuntimeError(
            "Parquet Silver vide."
        )

    if (
        len(
            silver_bytes
        )
        != silver_info[
            "size_bytes"
        ]
    ):
        raise RuntimeError(
            "Taille Silver incohérente."
        )

    silver_sha256 = (
        hashlib.sha256(
            silver_bytes
        )
        .hexdigest()
    )

    if (
        silver_sha256
        != silver_metadata.get(
            "sha256"
        )
    ):
        raise RuntimeError(
            "Échec du contrôle SHA-256 Silver."
        )

    print(
        f"[OK] Taille Silver : "
        f"{len(silver_bytes)} octets"
    )

    print(
        "[OK] Intégrité SHA-256 Silver validée"
    )

    # ========================================================
    # 7. Lecture Silver
    # ========================================================

    try:

        silver_df = (
            pd.read_parquet(
                BytesIO(
                    silver_bytes
                ),
                engine="pyarrow",
            )
        )

    except Exception as exc:

        raise RuntimeError(
            "Impossible de lire "
            "le Parquet Silver."
        ) from exc

    if silver_df.empty:
        raise RuntimeError(
            "Dataset Silver vide."
        )

    print(
        "[OK] Parquet Silver lisible"
    )

    # ========================================================
    # 8. Volumes et colonnes
    # ========================================================

    expected_record_count = (
        parse_integer_metadata(
            silver_metadata,
            "record-count",
        )
    )

    expected_column_count = (
        parse_integer_metadata(
            silver_metadata,
            "column-count",
        )
    )

    if (
        len(
            silver_df
        )
        != expected_record_count
    ):
        raise RuntimeError(
            "record-count Silver incohérent."
        )

    if (
        len(
            silver_df.columns
        )
        != expected_column_count
    ):
        raise RuntimeError(
            "column-count Silver incohérent."
        )

    print(
        f"[OK] Volume Silver : "
        f"{len(silver_df)} lignes"
    )

    print(
        f"[OK] Schéma Silver : "
        f"{len(silver_df.columns)} colonnes"
    )

    # ========================================================
    # 9. Colonnes obligatoires
    # ========================================================

    required_columns = {
        "id_offre",
        "intitule_offre",

        "batch_id",

        "bronze_processing_run_id",
        "processing_run_id",

        "bronze_schema_version",
        "silver_schema_version",

        "mot_cle_recherche",
        "mots_cles_recherche",

        "date_creation",
        "date_actualisation",
        "date_ingestion",
        "date_traitement_silver",
        "date_actualisation_coherente",

        "nom_entreprise",
        "description_entreprise",
        "entreprise_renseignee",
        "description_entreprise_renseignee",
        "information_entreprise_disponible",

        "salaire_libelle",
        "salaire_renseigne",
        "periode_salaire",

        "libelle_lieu_travail",
        "code_commune",
        "code_postal",
        "latitude",
        "longitude",

        "coordonnees_renseignees",
        "localisation_renseignee",

        "fichier_source",
        "objet_source_raw",
        "objet_source_bronze",
        "enregistrement_brut",
    }

    missing_columns = (
        required_columns
        - set(
            silver_df.columns
        )
    )

    if missing_columns:
        raise RuntimeError(
            "Colonnes Silver obligatoires absentes : "
            f"{sorted(missing_columns)}"
        )

    print(
        "[OK] Colonnes Silver obligatoires présentes"
    )

    # ========================================================
    # 10. IDs Silver
    # ========================================================

    missing_id_mask = (
        silver_df[
            "id_offre"
        ]
        .isna()
        |
        silver_df[
            "id_offre"
        ]
        .astype(str)
        .str.strip()
        .eq("")
    )

    missing_ids = int(
        missing_id_mask.sum()
    )

    if missing_ids > 0:
        raise RuntimeError(
            f"{missing_ids} ID(s) "
            "Silver manquant(s)."
        )

    normalized_silver_ids = (
        silver_df[
            "id_offre"
        ]
        .astype(str)
    )

    duplicate_ids = int(
        normalized_silver_ids
        .duplicated()
        .sum()
    )

    if duplicate_ids > 0:
        raise RuntimeError(
            f"{duplicate_ids} doublon(s) "
            "Silver."
        )

    silver_ids = set(
        normalized_silver_ids
        .tolist()
    )

    print(
        "[OK] Aucun ID Silver manquant"
    )

    print(
        "[OK] Aucun doublon d'ID Silver"
    )

    # ========================================================
    # 11. batch_id ligne par ligne
    # ========================================================

    if (
        silver_df[
            "batch_id"
        ]
        .isna()
        .any()
    ):
        raise RuntimeError(
            "batch_id manquant "
            "dans certaines lignes Silver."
        )

    batch_ids_df = set(
        silver_df[
            "batch_id"
        ]
        .astype(str)
        .unique()
    )

    if batch_ids_df != {
        batch_id
    }:
        raise RuntimeError(
            "batch_id incohérent "
            "dans le Silver."
        )

    print(
        "[OK] batch_id Silver cohérent"
    )

    # ========================================================
    # 12. processing_run_id Silver
    # ========================================================

    silver_runs_df = set(
        silver_df[
            "processing_run_id"
        ]
        .dropna()
        .astype(str)
        .unique()
    )

    if silver_runs_df != {
        silver_processing_run_id
    }:
        raise RuntimeError(
            "processing_run_id incohérent "
            "dans les lignes Silver."
        )

    print(
        "[OK] processing_run_id Silver "
        "cohérent sur toutes les lignes"
    )

    # ========================================================
    # 13. Silver schema_version
    # ========================================================

    silver_schema_versions = set(
        silver_df[
            "silver_schema_version"
        ]
        .dropna()
        .astype(str)
        .unique()
    )

    if silver_schema_versions != {
        EXPECTED_SILVER_SCHEMA_VERSION
    }:
        raise RuntimeError(
            "silver_schema_version incohérent."
        )

    print(
        "[OK] silver_schema_version cohérent"
    )

    # ========================================================
    # 14. Parent Bronze déclaré
    # ========================================================

    if (
        silver_metadata.get(
            "parent-layer"
        )
        != "bronze"
    ):
        raise RuntimeError(
            "parent-layer Silver doit valoir bronze."
        )

    bronze_object_key = (
        silver_metadata.get(
            "bronze-object-key"
        )
    )

    parent_object_key = (
        silver_metadata.get(
            "parent-object-key"
        )
    )

    if not bronze_object_key:
        raise RuntimeError(
            "bronze-object-key absent."
        )

    if (
        parent_object_key
        != bronze_object_key
    ):
        raise RuntimeError(
            "parent-object-key et bronze-object-key "
            "sont incohérents."
        )

    bronze_sha256_declared = (
        silver_metadata.get(
            "bronze-sha256"
        )
    )

    parent_sha256 = (
        silver_metadata.get(
            "parent-sha256"
        )
    )

    if not bronze_sha256_declared:
        raise RuntimeError(
            "bronze-sha256 absent."
        )

    if (
        parent_sha256
        != bronze_sha256_declared
    ):
        raise RuntimeError(
            "parent-sha256 et bronze-sha256 "
            "sont incohérents."
        )

    bronze_processing_run_id = (
        silver_metadata.get(
            "bronze-processing-run-id"
        )
    )

    parent_processing_run_id = (
        silver_metadata.get(
            "parent-processing-run-id"
        )
    )

    if not bronze_processing_run_id:
        raise RuntimeError(
            "bronze-processing-run-id absent."
        )

    if (
        parent_processing_run_id
        != bronze_processing_run_id
    ):
        raise RuntimeError(
            "parent-processing-run-id "
            "et bronze-processing-run-id "
            "sont incohérents."
        )

    if (
        silver_metadata.get(
            "parent-schema-version"
        )
        != EXPECTED_BRONZE_SCHEMA_VERSION
    ):
        raise RuntimeError(
            "parent-schema-version incorrect."
        )

    print(
        "[OK] Lineage parent Silver -> Bronze déclaré"
    )

    # ========================================================
    # 15. Parsing clé Bronze
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
        raise RuntimeError(
            "Le parent Silver n'est pas Bronze."
        )

    if (
        bronze_parts[
            "path_version"
        ]
        != EXPECTED_BRONZE_PATH_VERSION
    ):
        raise RuntimeError(
            "Le Bronze parent n'est pas processing_v2."
        )

    if (
        bronze_parts[
            "batch_id"
        ]
        != batch_id
    ):
        raise RuntimeError(
            "batch_id Bronze/Silver différent."
        )

    if (
        bronze_parts[
            "source"
        ]
        != source
    ):
        raise RuntimeError(
            "Source Bronze/Silver différente."
        )

    if (
        bronze_parts[
            "ingestion_date"
        ]
        != ingestion_date
    ):
        raise RuntimeError(
            "ingestion_date Bronze/Silver différente."
        )

    if (
        bronze_parts[
            "processing_run_id"
        ]
        != bronze_processing_run_id
    ):
        raise RuntimeError(
            "processing_run_id Bronze du chemin "
            "ne correspond pas au lineage Silver."
        )

    if not storage.object_exists(
        bronze_object_key
    ):
        raise RuntimeError(
            "Le Bronze parent "
            "n'existe plus dans R2."
        )

    print(
        "[OK] Clé Bronze parent cohérente"
    )

    # ========================================================
    # 16. Métadonnées Bronze parent
    # ========================================================

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
                "Métadonnée Bronze parent "
                f"incohérente : {key}."
            )

    bronze_git = (
        validate_git_provenance(
            bronze_metadata
        )
    )

    print(
        "[OK] Contrat Bronze parent valide"
    )

    # ========================================================
    # 17. SHA-256 Bronze réel
    # ========================================================

    bronze_bytes = (
        storage.download_bytes(
            bronze_object_key
        )
    )

    if not bronze_bytes:
        raise RuntimeError(
            "Bronze parent vide."
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
            "Taille Bronze parent incohérente."
        )

    bronze_sha256_actual = (
        hashlib.sha256(
            bronze_bytes
        )
        .hexdigest()
    )

    if (
        bronze_sha256_actual
        != bronze_metadata.get(
            "sha256"
        )
    ):
        raise RuntimeError(
            "SHA-256 Bronze parent invalide."
        )

    if (
        bronze_sha256_actual
        != bronze_sha256_declared
    ):
        raise RuntimeError(
            "Le SHA-256 Bronze enregistré "
            "dans Silver ne correspond pas "
            "au Bronze réel."
        )

    print(
        "[OK] SHA-256 réel du Bronze parent validé"
    )

    # ========================================================
    # 18. Pipeline normal ou replay
    # ========================================================

    is_replay = (
        silver_processing_run_id
        != bronze_processing_run_id
    )

    if not is_replay:

        # Même exécution logique :
        # la provenance doit être identique.

        fields_to_compare = [
            "git_commit_sha",
            "git_branch",
            "git_worktree_dirty",
            "processing_started_at_utc",
        ]

        for field in fields_to_compare:

            if (
                silver_git[
                    field
                ]
                != bronze_git[
                    field
                ]
            ):
                raise RuntimeError(
                    "Même processing_run_id "
                    "mais provenance Git différente "
                    f"pour {field}."
                )

        print(
            "[OK] Pipeline normal : "
            "provenance Bronze/Silver identique"
        )

    else:

        print(
            "[OK] Replay Silver détecté : "
            f"{bronze_processing_run_id} "
            "-> "
            f"{silver_processing_run_id}"
        )

    # ========================================================
    # 19. Lecture Bronze
    # ========================================================

    try:

        bronze_df = (
            pd.read_parquet(
                BytesIO(
                    bronze_bytes
                ),
                engine="pyarrow",
            )
        )

    except Exception as exc:

        raise RuntimeError(
            "Impossible de lire "
            "le Bronze parent."
        ) from exc

    if bronze_df.empty:
        raise RuntimeError(
            "Bronze parent vide."
        )

    bronze_record_count = (
        parse_integer_metadata(
            bronze_metadata,
            "record-count",
        )
    )

    if (
        len(
            bronze_df
        )
        != bronze_record_count
    ):
        raise RuntimeError(
            "record-count Bronze incohérent."
        )

    # ========================================================
    # 20. IDs Bronze
    # ========================================================

    if "id" not in bronze_df.columns:
        raise RuntimeError(
            "Colonne id absente du Bronze."
        )

    bronze_missing_mask = (
        bronze_df[
            "id"
        ]
        .isna()
        |
        bronze_df[
            "id"
        ]
        .astype(str)
        .str.strip()
        .eq("")
    )

    if int(
        bronze_missing_mask.sum()
    ) > 0:
        raise RuntimeError(
            "ID manquant dans Bronze."
        )

    bronze_id_series = (
        bronze_df[
            "id"
        ]
        .astype(str)
    )

    if int(
        bronze_id_series
        .duplicated()
        .sum()
    ) > 0:
        raise RuntimeError(
            "Doublon ID dans Bronze."
        )

    bronze_ids = set(
        bronze_id_series
        .tolist()
    )

    # ========================================================
    # 21. Bronze = Silver
    # ========================================================

    if (
        len(
            bronze_df
        )
        != len(
            silver_df
        )
    ):
        raise RuntimeError(
            "Volumes Bronze/Silver différents."
        )

    missing_in_silver = (
        bronze_ids
        - silver_ids
    )

    unexpected_in_silver = (
        silver_ids
        - bronze_ids
    )

    if missing_in_silver:
        raise RuntimeError(
            f"{len(missing_in_silver)} "
            "ID Bronze absents du Silver."
        )

    if unexpected_in_silver:
        raise RuntimeError(
            f"{len(unexpected_in_silver)} "
            "ID Silver absents du Bronze."
        )

    print(
        "[OK] Bronze = Silver : aucune offre perdue"
    )

    print(
        "[OK] Ensemble des IDs Bronze/Silver "
        f"strictement identique : "
        f"{len(silver_ids)}"
    )

    # ========================================================
    # 22. Run Bronze dans les lignes Silver
    # ========================================================

    bronze_runs_df = set(
        silver_df[
            "bronze_processing_run_id"
        ]
        .dropna()
        .astype(str)
        .unique()
    )

    if bronze_runs_df != {
        bronze_processing_run_id
    }:
        raise RuntimeError(
            "bronze_processing_run_id "
            "incohérent dans Silver."
        )

    bronze_schema_versions_df = set(
        silver_df[
            "bronze_schema_version"
        ]
        .dropna()
        .astype(str)
        .unique()
    )

    if bronze_schema_versions_df != {
        EXPECTED_BRONZE_SCHEMA_VERSION
    }:
        raise RuntimeError(
            "bronze_schema_version "
            "incohérent dans Silver."
        )

    print(
        "[OK] Référence au run Bronze "
        "présente sur toutes les lignes Silver"
    )

    # ========================================================
    # 23. objet_source_bronze
    # ========================================================

    bronze_keys_df = set(
        silver_df[
            "objet_source_bronze"
        ]
        .dropna()
        .astype(str)
        .unique()
    )

    if bronze_keys_df != {
        bronze_object_key
    }:
        raise RuntimeError(
            "objet_source_bronze "
            "incohérent."
        )

    print(
        "[OK] objet_source_bronze cohérent"
    )

    # ========================================================
    # 24. Lineage Raw
    # ========================================================

    raw_object_key = (
        silver_metadata.get(
            "raw-object-key"
        )
    )

    raw_sha256_declared = (
        silver_metadata.get(
            "raw-sha256"
        )
    )

    if not raw_object_key:
        raise RuntimeError(
            "raw-object-key absent "
            "des métadonnées Silver."
        )

    if not raw_sha256_declared:
        raise RuntimeError(
            "raw-sha256 absent."
        )

    if (
        bronze_metadata.get(
            "raw-object-key"
        )
        != raw_object_key
    ):
        raise RuntimeError(
            "Raw référencé par Silver différent "
            "du Raw référencé par Bronze."
        )

    if (
        bronze_metadata.get(
            "raw-sha256"
        )
        != raw_sha256_declared
    ):
        raise RuntimeError(
            "raw-sha256 Silver/Bronze différent."
        )

    raw_parts = (
        parse_data_lake_object_key(
            raw_object_key
        )
    )

    if (
        raw_parts[
            "layer"
        ]
        != "raw"
    ):
        raise RuntimeError(
            "raw-object-key ne référence "
            "pas la couche Raw."
        )

    if (
        raw_parts[
            "batch_id"
        ]
        != batch_id
    ):
        raise RuntimeError(
            "batch_id Raw/Silver différent."
        )

    if (
        raw_parts[
            "source"
        ]
        != source
    ):
        raise RuntimeError(
            "source Raw/Silver différente."
        )

    if (
        raw_parts[
            "ingestion_date"
        ]
        != ingestion_date
    ):
        raise RuntimeError(
            "ingestion_date Raw/Silver différente."
        )

    if not storage.object_exists(
        raw_object_key
    ):
        raise RuntimeError(
            "Objet Raw parent introuvable."
        )

    print(
        "[OK] Lineage Raw -> Bronze -> Silver cohérent"
    )

    # ========================================================
    # 25. objet_source_raw dans Silver
    # ========================================================

    raw_keys_df = set(
        silver_df[
            "objet_source_raw"
        ]
        .dropna()
        .astype(str)
        .unique()
    )

    if raw_keys_df != {
        raw_object_key
    }:
        raise RuntimeError(
            "objet_source_raw incohérent "
            "dans Silver."
        )

    expected_raw_filename = str(
        raw_parts[
            "filename"
        ]
    )

    source_files_df = set(
        silver_df[
            "fichier_source"
        ]
        .dropna()
        .astype(str)
        .unique()
    )

    if source_files_df != {
        expected_raw_filename
    }:
        raise RuntimeError(
            "fichier_source incohérent."
        )

    print(
        "[OK] Lineage Raw présent "
        "sur toutes les lignes Silver"
    )

    # ========================================================
    # 26. SHA réel Raw
    # ========================================================

    raw_info = (
        storage.get_object_info(
            raw_object_key
        )
    )

    raw_metadata = (
        raw_info.get(
            "metadata",
            {},
        )
    )

    raw_bytes = (
        storage.download_bytes(
            raw_object_key
        )
    )

    if not raw_bytes:
        raise RuntimeError(
            "Objet Raw vide."
        )

    raw_sha256_actual = (
        hashlib.sha256(
            raw_bytes
        )
        .hexdigest()
    )

    if (
        raw_sha256_actual
        != raw_metadata.get(
            "sha256"
        )
    ):
        raise RuntimeError(
            "SHA-256 réel Raw invalide."
        )

    if (
        raw_sha256_actual
        != raw_sha256_declared
    ):
        raise RuntimeError(
            "SHA-256 Raw enregistré "
            "dans Silver incorrect."
        )

    print(
        "[OK] SHA-256 réel du Raw validé"
    )

    # ========================================================
    # 27. Lecture Raw
    # ========================================================

    try:

        raw_jobs = json.loads(
            raw_bytes.decode(
                "utf-8"
            )
        )

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:

        raise RuntimeError(
            "JSON Raw illisible."
        ) from exc

    if not isinstance(
        raw_jobs,
        list,
    ):
        raise RuntimeError(
            "Le Raw doit être une liste JSON."
        )

    raw_by_id: dict[
        str,
        dict[str, Any],
    ] = {}

    for raw_job in raw_jobs:

        if not isinstance(
            raw_job,
            dict,
        ):
            raise RuntimeError(
                "Enregistrement Raw invalide."
            )

        raw_id = (
            raw_job.get(
                "id"
            )
        )

        if (
            raw_id is None
            or not str(
                raw_id
            ).strip()
        ):
            raise RuntimeError(
                "ID manquant dans Raw."
            )

        raw_id = str(
            raw_id
        )

        if raw_id in raw_by_id:
            raise RuntimeError(
                f"Doublon ID Raw : {raw_id}"
            )

        raw_by_id[
            raw_id
        ] = raw_job

    raw_ids = set(
        raw_by_id.keys()
    )

    # ========================================================
    # 28. Raw = Bronze = Silver
    # ========================================================

    if not (
        raw_ids
        == bronze_ids
        == silver_ids
    ):
        raise RuntimeError(
            "Les ensembles d'IDs "
            "Raw/Bronze/Silver diffèrent."
        )

    if not (
        len(
            raw_jobs
        )
        == len(
            bronze_df
        )
        == len(
            silver_df
        )
    ):
        raise RuntimeError(
            "Les volumes Raw/Bronze/Silver diffèrent."
        )

    print(
        "[OK] Raw = Bronze = Silver : "
        f"{len(silver_ids)} offres"
    )

    # ========================================================
    # 29. enregistrement_brut exact
    # ========================================================

    raw_record_invalid = 0
    raw_record_mismatch = 0
    raw_id_mismatch = 0

    keyword_list_invalid = 0
    keyword_list_mismatch = 0
    main_keyword_mismatch = 0
    main_keyword_not_in_list = 0

    for _, row in (
        silver_df.iterrows()
    ):

        silver_id = str(
            row[
                "id_offre"
            ]
        )

        expected_raw_job = (
            raw_by_id[
                silver_id
            ]
        )

        # ----------------------------------------------------
        # Raw record
        # ----------------------------------------------------

        try:

            parsed_raw_record = (
                json.loads(
                    row[
                        "enregistrement_brut"
                    ]
                )
            )

        except (
            TypeError,
            json.JSONDecodeError,
        ):

            raw_record_invalid += 1
            continue

        if (
            str(
                parsed_raw_record.get(
                    "id"
                )
            )
            != silver_id
        ):
            raw_id_mismatch += 1

        if (
            parsed_raw_record
            != expected_raw_job
        ):
            raw_record_mismatch += 1

        # ----------------------------------------------------
        # mots_cles_recherche
        # ----------------------------------------------------

        try:

            parsed_keywords = (
                json.loads(
                    row[
                        "mots_cles_recherche"
                    ]
                )
            )

        except (
            TypeError,
            json.JSONDecodeError,
        ):

            keyword_list_invalid += 1
            continue

        if (
            not isinstance(
                parsed_keywords,
                list,
            )
            or not parsed_keywords
        ):
            keyword_list_invalid += 1
            continue

        expected_keywords = (
            expected_raw_job.get(
                "search_keywords"
            )
        )

        if (
            parsed_keywords
            != expected_keywords
        ):
            keyword_list_mismatch += 1

        normalized_keywords = {
            normalize_keyword(
                keyword
            )
            for keyword
            in parsed_keywords
            if normalize_keyword(
                keyword
            )
            is not None
        }

        normalized_silver_keyword = (
            normalize_keyword(
                row[
                    "mot_cle_recherche"
                ]
            )
        )

        normalized_raw_keyword = (
            normalize_keyword(
                expected_raw_job.get(
                    "search_keyword"
                )
            )
        )

        if (
            normalized_silver_keyword
            != normalized_raw_keyword
        ):
            main_keyword_mismatch += 1

        if (
            normalized_silver_keyword
            not in normalized_keywords
        ):
            main_keyword_not_in_list += 1

    if raw_record_invalid > 0:
        raise RuntimeError(
            f"{raw_record_invalid} "
            "enregistrement_brut invalide(s)."
        )

    if raw_id_mismatch > 0:
        raise RuntimeError(
            f"{raw_id_mismatch} "
            "ID incohérent(s) "
            "dans enregistrement_brut."
        )

    if raw_record_mismatch > 0:
        raise RuntimeError(
            f"{raw_record_mismatch} "
            "enregistrement_brut diffèrent "
            "du Raw."
        )

    print(
        "[OK] enregistrement_brut "
        "strictement identique au Raw"
    )

    if keyword_list_invalid > 0:
        raise RuntimeError(
            f"{keyword_list_invalid} "
            "mots_cles_recherche invalide(s)."
        )

    if keyword_list_mismatch > 0:
        raise RuntimeError(
            f"{keyword_list_mismatch} "
            "liste(s) de mots-clés différente(s) "
            "du Raw."
        )

    if main_keyword_mismatch > 0:
        raise RuntimeError(
            f"{main_keyword_mismatch} "
            "mot_cle_recherche incohérent(s)."
        )

    if main_keyword_not_in_list > 0:
        raise RuntimeError(
            f"{main_keyword_not_in_list} "
            "mot(s)-clé(s) principal(aux) "
            "absent(s) de mots_cles_recherche."
        )

    print(
        "[OK] Mots-clés d'acquisition "
        "strictement conservés"
    )

    # ========================================================
    # 30. Dates techniques
    # ========================================================

    ingestion_dates = (
        pd.to_datetime(
            silver_df[
                "date_ingestion"
            ],
            utc=True,
            errors="coerce",
        )
    )

    if (
        ingestion_dates
        .isna()
        .any()
    ):
        raise RuntimeError(
            "date_ingestion invalide."
        )

    if (
        len(
            ingestion_dates.unique()
        )
        != 1
    ):
        raise RuntimeError(
            "Plusieurs date_ingestion "
            "dans le batch Silver."
        )

    processing_dates = (
        pd.to_datetime(
            silver_df[
                "date_traitement_silver"
            ],
            utc=True,
            errors="coerce",
        )
    )

    if (
        processing_dates
        .isna()
        .any()
    ):
        raise RuntimeError(
            "date_traitement_silver invalide."
        )

    if (
        len(
            processing_dates.unique()
        )
        != 1
    ):
        raise RuntimeError(
            "Plusieurs date_traitement_silver "
            "dans le même artefact."
        )

    print(
        "[OK] Dates techniques cohérentes"
    )

    # ========================================================
    # 31. Indicateurs entreprise
    # ========================================================

    expected_entreprise = (
        silver_df[
            "nom_entreprise"
        ]
        .notna()
    )

    validate_boolean_indicator(
        silver_df,
        "entreprise_renseignee",
        expected_entreprise,
    )

    expected_description = (
        silver_df[
            "description_entreprise"
        ]
        .notna()
    )

    validate_boolean_indicator(
        silver_df,
        "description_entreprise_renseignee",
        expected_description,
    )

    expected_information = (
        expected_entreprise
        | expected_description
    )

    validate_boolean_indicator(
        silver_df,
        "information_entreprise_disponible",
        expected_information,
    )

    print(
        "[OK] Indicateurs entreprise cohérents"
    )

    # ========================================================
    # 32. Salaire
    # ========================================================

    expected_salary = (
        silver_df[
            "salaire_libelle"
        ]
        .notna()
    )

    validate_boolean_indicator(
        silver_df,
        "salaire_renseigne",
        expected_salary,
    )

    expected_period = (
        silver_df[
            "salaire_libelle"
        ]
        .apply(
            extract_salary_period
        )
    )

    period_mismatch = (
        silver_df[
            "periode_salaire"
        ]
        .fillna(
            "__NULL__"
        )
        != expected_period.fillna(
            "__NULL__"
        )
    )

    period_mismatch_count = int(
        period_mismatch.sum()
    )

    if period_mismatch_count > 0:
        raise RuntimeError(
            f"{period_mismatch_count} "
            "periode_salaire incohérente(s)."
        )

    print(
        "[OK] Indicateurs salaire cohérents"
    )

    # ========================================================
    # 33. Coordonnées GPS
    # ========================================================

    expected_coordinates = (
        silver_df[
            "latitude"
        ]
        .notna()
        & silver_df[
            "longitude"
        ]
        .notna()
    )

    validate_boolean_indicator(
        silver_df,
        "coordonnees_renseignees",
        expected_coordinates,
    )

    print(
        "[OK] coordonnees_renseignees cohérent"
    )

    # ========================================================
    # 34. Localisation exploitable
    # ========================================================

    expected_location = (
        silver_df[
            "libelle_lieu_travail"
        ]
        .notna()
        | silver_df[
            "code_commune"
        ]
        .notna()
        | silver_df[
            "code_postal"
        ]
        .notna()
        | expected_coordinates
    )

    validate_boolean_indicator(
        silver_df,
        "localisation_renseignee",
        expected_location,
    )

    print(
        "[OK] localisation_renseignee cohérent"
    )

    # ========================================================
    # 35. Dates métier
    # ========================================================

    validate_date_coherence(
        silver_df
    )

    print(
        "[OK] date_actualisation_coherente cohérente"
    )

    # ========================================================
    # 36. Métriques qualité
    # ========================================================

    total = len(
        silver_df
    )

    entreprise_count = int(
        silver_df[
            "information_entreprise_disponible"
        ]
        .fillna(False)
        .sum()
    )

    salary_count = int(
        silver_df[
            "salaire_renseigne"
        ]
        .fillna(False)
        .sum()
    )

    coordinates_count = int(
        silver_df[
            "coordonnees_renseignees"
        ]
        .fillna(False)
        .sum()
    )

    location_count = int(
        silver_df[
            "localisation_renseignee"
        ]
        .fillna(False)
        .sum()
    )

    entreprise_rate = (
        entreprise_count
        / total
        * 100
    )

    salary_rate = (
        salary_count
        / total
        * 100
    )

    coordinates_rate = (
        coordinates_count
        / total
        * 100
    )

    location_rate = (
        location_count
        / total
        * 100
    )

    # ========================================================
    # 37. Résumé
    # ========================================================

    print()
    print("-" * 70)
    print("RÉSUMÉ")
    print("-" * 70)

    print(
        f"Batch ID                  : "
        f"{batch_id}"
    )

    print(
        f"Bronze Processing Run     : "
        f"{bronze_processing_run_id}"
    )

    print(
        f"Silver Processing Run     : "
        f"{silver_processing_run_id}"
    )

    print(
        f"Replay Silver             : "
        f"{is_replay}"
    )

    print(
        f"Lignes Raw                : "
        f"{len(raw_jobs)}"
    )

    print(
        f"Lignes Bronze             : "
        f"{len(bronze_df)}"
    )

    print(
        f"Lignes Silver             : "
        f"{len(silver_df)}"
    )

    print(
        f"IDs uniques Silver        : "
        f"{len(silver_ids)}"
    )

    print(
        f"Colonnes Silver           : "
        f"{len(silver_df.columns)}"
    )

    print(
        f"Localisation renseignée   : "
        f"{location_count} "
        f"({location_rate:.2f} %)"
    )

    print(
        f"Coordonnées GPS           : "
        f"{coordinates_count} "
        f"({coordinates_rate:.2f} %)"
    )

    print(
        f"Salaire renseigné         : "
        f"{salary_count} "
        f"({salary_rate:.2f} %)"
    )

    print(
        f"Information entreprise    : "
        f"{entreprise_count} "
        f"({entreprise_rate:.2f} %)"
    )

    print(
        f"Git branch Silver         : "
        f"{silver_git['git_branch']}"
    )

    print(
        f"Git commit Silver         : "
        f"{silver_git['git_commit_sha']}"
    )

    print(
        f"Git worktree dirty        : "
        f"{silver_git['git_worktree_dirty']}"
    )

    print(
        f"Objet Raw                 : "
        f"{raw_object_key}"
    )

    print(
        f"Objet Bronze              : "
        f"{bronze_object_key}"
    )

    print(
        f"Objet Silver              : "
        f"{silver_object_key}"
    )

    print(
        f"SHA-256 Raw               : "
        f"{raw_sha256_actual}"
    )

    print(
        f"SHA-256 Bronze            : "
        f"{bronze_sha256_actual}"
    )

    print(
        f"SHA-256 Silver            : "
        f"{silver_sha256}"
    )

    print()
    print(
        "STATUT SILVER V2 : VALIDE"
    )
    print()

    # ========================================================
    # 38. Résultat pipeline
    # ========================================================

    return {
        "statut": (
            "VALIDE"
        ),

        "batch_id": (
            batch_id
        ),

        "bronze_processing_run_id": (
            bronze_processing_run_id
        ),

        "processing_run_id": (
            silver_processing_run_id
        ),

        "replay_silver": (
            is_replay
        ),

        "schema_version": (
            EXPECTED_SILVER_SCHEMA_VERSION
        ),

        "path_version": (
            EXPECTED_SILVER_PATH_VERSION
        ),

        "nombre_lignes_raw": (
            len(
                raw_jobs
            )
        ),

        "nombre_lignes_bronze": (
            len(
                bronze_df
            )
        ),

        "nombre_lignes_silver": (
            len(
                silver_df
            )
        ),

        "nombre_ids_uniques": (
            len(
                silver_ids
            )
        ),

        "nombre_colonnes_silver": (
            len(
                silver_df.columns
            )
        ),

        "localisation_renseignee": (
            location_count
        ),

        "taux_localisation_pct": round(
            location_rate,
            2,
        ),

        "coordonnees_renseignees": (
            coordinates_count
        ),

        "taux_coordonnees_pct": round(
            coordinates_rate,
            2,
        ),

        "salaires_renseignes": (
            salary_count
        ),

        "taux_salaires_pct": round(
            salary_rate,
            2,
        ),

        "information_entreprise_disponible": (
            entreprise_count
        ),

        "taux_information_entreprise_pct": round(
            entreprise_rate,
            2,
        ),

        "git_commit_sha": (
            silver_git[
                "git_commit_sha"
            ]
        ),

        "git_worktree_dirty": (
            silver_git[
                "git_worktree_dirty"
            ]
        ),

        "git_branch": (
            silver_git[
                "git_branch"
            ]
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

        "raw_sha256": (
            raw_sha256_actual
        ),

        "bronze_sha256": (
            bronze_sha256_actual
        ),

        "silver_sha256": (
            silver_sha256
        ),
    }


# ============================================================
# CLI
# ============================================================

def main() -> None:
    """
    CLI du Quality Gate Silver processing_v2.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Contrôle qualité complet "
            "d'un artefact Silver processing_v2 "
            "France Travail stocké dans R2."
        )
    )

    parser.add_argument(
        "--silver-object-key",
        required=True,
        help=(
            "Clé du Parquet Silver processing_v2 "
            "dans Cloudflare R2."
        ),
    )

    args = parser.parse_args()

    validate_silver_object(
        silver_object_key=(
            args.silver_object_key
        )
    )


if __name__ == "__main__":
    main()