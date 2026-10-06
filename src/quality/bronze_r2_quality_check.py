import argparse
import hashlib
import json
import re
from datetime import datetime
from io import BytesIO
from typing import Any

import pandas as pd

from src.storage.r2_paths import (
    parse_data_lake_object_key,
)
from src.storage.r2_storage import R2Storage


# ============================================================
# Contrat Bronze attendu
# ============================================================

EXPECTED_SCHEMA_VERSION = "2.0.0"

EXPECTED_PATH_VERSION = "processing_v2"


# ============================================================
# Helpers
# ============================================================

def parse_positive_integer_metadata(
    metadata: dict[str, str],
    key: str,
    allow_zero: bool = False,
) -> int:
    """
    Lit une métadonnée entière et la valide.
    """

    value = metadata.get(
        key
    )

    if value is None:
        raise RuntimeError(
            f"Métadonnée obligatoire absente : {key}"
        )

    try:

        parsed = int(
            value
        )

    except ValueError as exc:

        raise RuntimeError(
            f"Métadonnée {key} invalide : {value}"
        ) from exc

    minimum = (
        0
        if allow_zero
        else 1
    )

    if parsed < minimum:
        raise RuntimeError(
            f"Métadonnée {key} invalide : {parsed}"
        )

    return parsed


def validate_utc_timestamp(
    value: str,
    field_name: str,
) -> None:
    """
    Vérifie un timestamp ISO 8601 UTC.
    """

    if not isinstance(
        value,
        str,
    ) or not value:

        raise RuntimeError(
            f"{field_name} est absent ou invalide."
        )

    if not value.endswith(
        "Z"
    ):
        raise RuntimeError(
            f"{field_name} doit être exprimé en UTC avec suffixe Z."
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
            f"{field_name} n'est pas un timestamp ISO valide."
        ) from exc


def validate_git_provenance(
    metadata: dict[str, str],
) -> dict[str, Any]:
    """
    Vérifie les métadonnées Git produites
    par ProcessingRunContext.
    """

    git_commit_sha = (
        metadata.get(
            "git-commit-sha"
        )
    )

    if not git_commit_sha:
        raise RuntimeError(
            "git-commit-sha absent "
            "des métadonnées Bronze."
        )

    if not re.fullmatch(
        r"[0-9a-f]{40}|[0-9a-f]{64}",
        git_commit_sha,
    ):
        raise RuntimeError(
            "git-commit-sha Bronze invalide."
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
# Contrôle principal
# ============================================================

def validate_bronze_object(
    bronze_object_key: str,
) -> dict[str, Any]:
    """
    Quality Gate complet d'un artefact Bronze processing_v2.

    Vérifie notamment :

    - convention de chemin R2 ;
    - processing_run_id ;
    - schema_version ;
    - provenance Git ;
    - SHA-256 Bronze ;
    - lineage vers Raw ;
    - SHA-256 réel du parent Raw ;
    - volumes ;
    - schéma minimal ;
    - unicité des IDs ;
    - Raw IDs = Bronze IDs ;
    - raw_record exactement conforme au Raw ;
    - search_keyword/search_keywords ;
    - timestamps d'ingestion ;
    - métadonnées de schema drift.
    """

    storage = R2Storage()

    print()
    print("=" * 70)
    print(
        "CONTRÔLE QUALITÉ BRONZE R2 - PROCESSING V2"
    )
    print("=" * 70)
    print()

    # ========================================================
    # 1. Parsing de la clé Bronze
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
            "L'objet fourni n'appartient "
            "pas à la couche Bronze."
        )

    if (
        bronze_parts[
            "path_version"
        ]
        != EXPECTED_PATH_VERSION
    ):
        raise RuntimeError(
            "Ce Quality Gate attend un Bronze "
            f"{EXPECTED_PATH_VERSION}. "
            f"Version détectée : "
            f"{bronze_parts['path_version']}."
        )

    batch_id = str(
        bronze_parts[
            "batch_id"
        ]
    )

    processing_run_id = (
        bronze_parts[
            "processing_run_id"
        ]
    )

    if not processing_run_id:
        raise RuntimeError(
            "processing_run_id absent "
            "du chemin Bronze."
        )

    processing_run_id = str(
        processing_run_id
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

    print(
        "[OK] Convention de chemin Bronze processing_v2"
    )

    # ========================================================
    # 2. Existence R2
    # ========================================================

    if not storage.object_exists(
        bronze_object_key
    ):
        raise FileNotFoundError(
            "Objet Bronze introuvable dans R2 : "
            f"{bronze_object_key}"
        )

    print(
        "[OK] Objet Bronze présent dans R2"
    )

    # ========================================================
    # 3. Métadonnées Bronze
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

    print(
        "[OK] Métadonnées Bronze accessibles"
    )

    # ========================================================
    # 4. Identité de l'artefact
    # ========================================================

    expected_metadata = {
        "layer": "bronze",
        "source": source,
        "batch-id": batch_id,
        "ingestion-date-utc": (
            ingestion_date
        ),
        "processing-run-id": (
            processing_run_id
        ),
        "schema-version": (
            EXPECTED_SCHEMA_VERSION
        ),
        "path-version": (
            EXPECTED_PATH_VERSION
        ),
    }

    for key, expected_value in (
        expected_metadata.items()
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

    print(
        f"[OK] Batch ID cohérent : {batch_id}"
    )

    print(
        "[OK] Processing Run ID cohérent : "
        f"{processing_run_id}"
    )

    print(
        "[OK] Schema version : "
        f"{EXPECTED_SCHEMA_VERSION}"
    )

    print(
        "[OK] Path version : "
        f"{EXPECTED_PATH_VERSION}"
    )

    # ========================================================
    # 5. Provenance Git
    # ========================================================

    git_context = (
        validate_git_provenance(
            bronze_metadata
        )
    )

    print(
        "[OK] Provenance Git présente et valide"
    )

    # ========================================================
    # 6. Téléchargement Bronze
    # ========================================================

    bronze_bytes = (
        storage.download_bytes(
            bronze_object_key
        )
    )

    if not bronze_bytes:
        raise RuntimeError(
            "Le Parquet Bronze téléchargé est vide."
        )

    if (
        len(bronze_bytes)
        != bronze_info[
            "size_bytes"
        ]
    ):
        raise RuntimeError(
            "La taille Bronze téléchargée "
            "diffère de la taille R2."
        )

    print(
        f"[OK] Taille Bronze : "
        f"{len(bronze_bytes)} octets"
    )

    # ========================================================
    # 7. SHA-256 Bronze
    # ========================================================

    bronze_sha256 = (
        hashlib.sha256(
            bronze_bytes
        )
        .hexdigest()
    )

    bronze_sha256_metadata = (
        bronze_metadata.get(
            "sha256"
        )
    )

    if not bronze_sha256_metadata:
        raise RuntimeError(
            "SHA-256 Bronze absent "
            "des métadonnées."
        )

    if (
        bronze_sha256
        != bronze_sha256_metadata
    ):
        raise RuntimeError(
            "Échec du contrôle SHA-256 Bronze."
        )

    print(
        "[OK] Intégrité SHA-256 Bronze validée"
    )

    # ========================================================
    # 8. Lecture du Parquet
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
            "le Parquet Bronze."
        ) from exc

    if bronze_df.empty:
        raise RuntimeError(
            "Le dataset Bronze est vide."
        )

    print(
        "[OK] Parquet Bronze lisible"
    )

    # ========================================================
    # 9. Volume et schéma
    # ========================================================

    expected_record_count = (
        parse_positive_integer_metadata(
            bronze_metadata,
            "record-count",
        )
    )

    expected_column_count = (
        parse_positive_integer_metadata(
            bronze_metadata,
            "column-count",
        )
    )

    if (
        len(bronze_df)
        != expected_record_count
    ):
        raise RuntimeError(
            "record-count Bronze incohérent : "
            f"metadata={expected_record_count}, "
            f"parquet={len(bronze_df)}."
        )

    if (
        len(bronze_df.columns)
        != expected_column_count
    ):
        raise RuntimeError(
            "column-count Bronze incohérent : "
            f"metadata={expected_column_count}, "
            f"parquet={len(bronze_df.columns)}."
        )

    print(
        f"[OK] Volume Bronze : "
        f"{len(bronze_df)} lignes"
    )

    print(
        f"[OK] Schéma Bronze : "
        f"{len(bronze_df.columns)} colonnes"
    )

    # ========================================================
    # 10. Colonnes obligatoires
    # ========================================================

    required_columns = {
        "id",
        "intitule",

        "search_keyword",
        "search_keywords",

        "ingestion_timestamp",

        "batch_id",
        "processing_run_id",
        "bronze_schema_version",

        "raw_source_file",
        "raw_source_object_key",
        "raw_record",
    }

    missing_columns = (
        required_columns
        - set(
            bronze_df.columns
        )
    )

    if missing_columns:
        raise RuntimeError(
            "Colonnes Bronze obligatoires absentes : "
            f"{sorted(missing_columns)}"
        )

    print(
        "[OK] Colonnes techniques obligatoires présentes"
    )

    # ========================================================
    # 11. IDs Bronze
    # ========================================================

    missing_id_mask = (
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

    missing_ids = int(
        missing_id_mask.sum()
    )

    if missing_ids > 0:
        raise RuntimeError(
            f"{missing_ids} ID(s) manquant(s) "
            "dans Bronze."
        )

    normalized_bronze_ids = (
        bronze_df[
            "id"
        ]
        .astype(str)
    )

    duplicated_ids = int(
        normalized_bronze_ids
        .duplicated()
        .sum()
    )

    if duplicated_ids > 0:
        raise RuntimeError(
            f"{duplicated_ids} doublon(s) "
            "d'ID détecté(s) dans Bronze."
        )

    bronze_ids = set(
        normalized_bronze_ids
        .tolist()
    )

    print(
        "[OK] Aucun ID manquant"
    )

    print(
        "[OK] Aucun doublon d'ID"
    )

    # ========================================================
    # 12. batch_id dans les lignes
    # ========================================================

    if (
        bronze_df[
            "batch_id"
        ]
        .isna()
        .any()
    ):
        raise RuntimeError(
            "batch_id manquant "
            "sur certaines lignes Bronze."
        )

    batch_ids_df = set(
        bronze_df[
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
            "dans les lignes Bronze."
        )

    print(
        "[OK] batch_id présent et unique "
        "sur toutes les lignes Bronze"
    )

    # ========================================================
    # 13. processing_run_id dans les lignes
    # ========================================================

    if (
        bronze_df[
            "processing_run_id"
        ]
        .isna()
        .any()
    ):
        raise RuntimeError(
            "processing_run_id manquant "
            "sur certaines lignes Bronze."
        )

    processing_ids_df = set(
        bronze_df[
            "processing_run_id"
        ]
        .astype(str)
        .unique()
    )

    if processing_ids_df != {
        processing_run_id
    }:
        raise RuntimeError(
            "processing_run_id incohérent "
            "dans les lignes Bronze."
        )

    print(
        "[OK] processing_run_id cohérent "
        "sur toutes les lignes"
    )

    # ========================================================
    # 14. Schema version dans les lignes
    # ========================================================

    schema_versions_df = set(
        bronze_df[
            "bronze_schema_version"
        ]
        .dropna()
        .astype(str)
        .unique()
    )

    if schema_versions_df != {
        EXPECTED_SCHEMA_VERSION
    }:
        raise RuntimeError(
            "bronze_schema_version incohérent "
            "dans le Parquet."
        )

    print(
        "[OK] bronze_schema_version cohérent"
    )

    # ========================================================
    # 15. Schema drift metadata
    # ========================================================

    drift_detected_raw = (
        bronze_metadata.get(
            "schema-drift-detected"
        )
    )

    if drift_detected_raw not in {
        "true",
        "false",
    }:
        raise RuntimeError(
            "schema-drift-detected doit valoir "
            "'true' ou 'false'."
        )

    schema_drift_field_count = (
        parse_positive_integer_metadata(
            bronze_metadata,
            "schema-drift-field-count",
            allow_zero=True,
        )
    )

    schema_drift_detected = (
        drift_detected_raw
        == "true"
    )

    if (
        schema_drift_detected
        != (
            schema_drift_field_count
            > 0
        )
    ):
        raise RuntimeError(
            "Incohérence entre "
            "schema-drift-detected et "
            "schema-drift-field-count."
        )

    print(
        "[OK] Métadonnées de schema drift cohérentes"
    )

    # ========================================================
    # 16. Parent générique
    # ========================================================

    if (
        bronze_metadata.get(
            "parent-layer"
        )
        != "raw"
    ):
        raise RuntimeError(
            "parent-layer Bronze doit valoir raw."
        )

    parent_object_key = (
        bronze_metadata.get(
            "parent-object-key"
        )
    )

    raw_object_key = (
        bronze_metadata.get(
            "raw-object-key"
        )
    )

    if not parent_object_key:
        raise RuntimeError(
            "parent-object-key absent."
        )

    if not raw_object_key:
        raise RuntimeError(
            "raw-object-key absent."
        )

    if (
        parent_object_key
        != raw_object_key
    ):
        raise RuntimeError(
            "parent-object-key et raw-object-key "
            "doivent référencer le même objet."
        )

    parent_sha256 = (
        bronze_metadata.get(
            "parent-sha256"
        )
    )

    raw_sha256_from_bronze = (
        bronze_metadata.get(
            "raw-sha256"
        )
    )

    if not parent_sha256:
        raise RuntimeError(
            "parent-sha256 absent."
        )

    if (
        parent_sha256
        != raw_sha256_from_bronze
    ):
        raise RuntimeError(
            "parent-sha256 et raw-sha256 "
            "sont incohérents."
        )

    print(
        "[OK] Lineage parent Bronze -> Raw déclaré"
    )

    # ========================================================
    # 17. Validation de la clé Raw
    # ========================================================

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
            "Le parent du Bronze "
            "n'est pas un objet Raw."
        )

    if (
        raw_parts[
            "batch_id"
        ]
        != batch_id
    ):
        raise RuntimeError(
            "batch_id Raw/Bronze incohérent."
        )

    if (
        raw_parts[
            "source"
        ]
        != source
    ):
        raise RuntimeError(
            "source Raw/Bronze incohérente."
        )

    if (
        raw_parts[
            "ingestion_date"
        ]
        != ingestion_date
    ):
        raise RuntimeError(
            "ingestion_date Raw/Bronze incohérente."
        )

    if not storage.object_exists(
        raw_object_key
    ):
        raise RuntimeError(
            "L'objet Raw parent "
            "n'existe plus dans R2."
        )

    print(
        "[OK] Clé du parent Raw cohérente"
    )

    # ========================================================
    # 18. Lecture des métadonnées Raw
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

    if (
        raw_metadata.get(
            "batch-id"
        )
        != batch_id
    ):
        raise RuntimeError(
            "batch-id des métadonnées Raw "
            "incohérent."
        )

    raw_sha256_metadata = (
        raw_metadata.get(
            "sha256"
        )
    )

    if not raw_sha256_metadata:
        raise RuntimeError(
            "SHA-256 absent "
            "des métadonnées Raw."
        )

    # ========================================================
    # 19. SHA réel du parent Raw
    # ========================================================

    raw_bytes = (
        storage.download_bytes(
            raw_object_key
        )
    )

    if not raw_bytes:
        raise RuntimeError(
            "Le Raw parent est vide."
        )

    if (
        len(raw_bytes)
        != raw_info[
            "size_bytes"
        ]
    ):
        raise RuntimeError(
            "Taille du Raw parent incohérente."
        )

    raw_sha256_actual = (
        hashlib.sha256(
            raw_bytes
        )
        .hexdigest()
    )

    if (
        raw_sha256_actual
        != raw_sha256_metadata
    ):
        raise RuntimeError(
            "SHA-256 réel du Raw "
            "ne correspond pas à ses métadonnées."
        )

    if (
        raw_sha256_actual
        != parent_sha256
    ):
        raise RuntimeError(
            "Le SHA-256 du parent enregistré "
            "dans Bronze ne correspond pas "
            "au Raw réel."
        )

    print(
        "[OK] SHA-256 réel du parent Raw validé"
    )

    # ========================================================
    # 20. Parsing Raw
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
            "Impossible de lire "
            "le JSON Raw parent."
        ) from exc

    if not isinstance(
        raw_jobs,
        list,
    ):
        raise RuntimeError(
            "Le Raw parent doit être "
            "une liste JSON."
        )

    # ========================================================
    # 21. Volume Raw
    # ========================================================

    raw_record_count = (
        parse_positive_integer_metadata(
            raw_metadata,
            "record-count",
        )
    )

    if (
        raw_record_count
        != len(raw_jobs)
    ):
        raise RuntimeError(
            "record-count Raw incohérent."
        )

    if (
        len(raw_jobs)
        != len(bronze_df)
    ):
        raise RuntimeError(
            "Volumes Raw/Bronze différents : "
            f"Raw={len(raw_jobs)}, "
            f"Bronze={len(bronze_df)}."
        )

    # ========================================================
    # 22. IDs Raw
    # ========================================================

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
                "Enregistrement Raw non dictionnaire."
            )

        raw_id = raw_job.get(
            "id"
        )

        if (
            raw_id is None
            or str(
                raw_id
            ).strip()
            == ""
        ):
            raise RuntimeError(
                "ID manquant dans le Raw."
            )

        raw_id_str = str(
            raw_id
        )

        if raw_id_str in raw_by_id:
            raise RuntimeError(
                "Doublon d'ID dans le Raw : "
                f"{raw_id_str}"
            )

        raw_by_id[
            raw_id_str
        ] = raw_job

    raw_ids = set(
        raw_by_id.keys()
    )

    if (
        raw_ids
        != bronze_ids
    ):
        missing_in_bronze = (
            raw_ids
            - bronze_ids
        )

        unexpected_in_bronze = (
            bronze_ids
            - raw_ids
        )

        raise RuntimeError(
            "Ensembles d'IDs Raw/Bronze différents. "
            f"Absents Bronze={len(missing_in_bronze)}, "
            f"Inattendus Bronze="
            f"{len(unexpected_in_bronze)}."
        )

    print(
        "[OK] Raw = Bronze : aucune offre perdue"
    )

    print(
        "[OK] Ensemble des IDs strictement identique : "
        f"{len(bronze_ids)} IDs"
    )

    # ========================================================
    # 23. Lineage dans chaque ligne
    # ========================================================

    raw_keys_df = set(
        bronze_df[
            "raw_source_object_key"
        ]
        .dropna()
        .astype(str)
        .unique()
    )

    if raw_keys_df != {
        raw_object_key
    }:
        raise RuntimeError(
            "raw_source_object_key incohérent "
            "dans les lignes Bronze."
        )

    expected_raw_filename = str(
        raw_parts[
            "filename"
        ]
    )

    raw_files_df = set(
        bronze_df[
            "raw_source_file"
        ]
        .dropna()
        .astype(str)
        .unique()
    )

    if raw_files_df != {
        expected_raw_filename
    }:
        raise RuntimeError(
            "raw_source_file incohérent "
            "dans les lignes Bronze."
        )

    print(
        "[OK] Lineage Raw présent "
        "sur toutes les lignes Bronze"
    )

    # ========================================================
    # 24. Vérification raw_record exacte
    # ========================================================

    raw_record_invalid_count = 0
    raw_record_id_mismatch = 0
    raw_record_content_mismatch = 0

    search_keywords_invalid_count = 0
    search_keywords_mismatch = 0
    search_keyword_mismatch = 0

    ingestion_timestamp_mismatch = 0

    ingestion_timestamps: set[
        str
    ] = set()

    for _, row in (
        bronze_df.iterrows()
    ):

        bronze_id = str(
            row[
                "id"
            ]
        )

        expected_raw_job = (
            raw_by_id[
                bronze_id
            ]
        )

        # ----------------------------------------------------
        # raw_record
        # ----------------------------------------------------

        try:

            parsed_raw_record = (
                json.loads(
                    row[
                        "raw_record"
                    ]
                )
            )

        except (
            TypeError,
            json.JSONDecodeError,
        ):

            raw_record_invalid_count += 1
            continue

        if (
            str(
                parsed_raw_record.get(
                    "id"
                )
            )
            != bronze_id
        ):
            raw_record_id_mismatch += 1

        if (
            parsed_raw_record
            != expected_raw_job
        ):
            raw_record_content_mismatch += 1

        # ----------------------------------------------------
        # search_keywords Bronze
        # ----------------------------------------------------

        try:

            parsed_search_keywords = (
                json.loads(
                    row[
                        "search_keywords"
                    ]
                )
            )

        except (
            TypeError,
            json.JSONDecodeError,
        ):

            search_keywords_invalid_count += 1
            continue

        if (
            not isinstance(
                parsed_search_keywords,
                list,
            )
            or not parsed_search_keywords
        ):
            search_keywords_invalid_count += 1
            continue

        expected_search_keywords = (
            expected_raw_job.get(
                "search_keywords"
            )
        )

        if (
            parsed_search_keywords
            != expected_search_keywords
        ):
            search_keywords_mismatch += 1

        if (
            row[
                "search_keyword"
            ]
            != expected_raw_job.get(
                "search_keyword"
            )
        ):
            search_keyword_mismatch += 1

        # ----------------------------------------------------
        # ingestion_timestamp
        # ----------------------------------------------------

        expected_timestamp = (
            expected_raw_job.get(
                "ingestion_timestamp"
            )
        )

        actual_timestamp = (
            row[
                "ingestion_timestamp"
            ]
        )

        if (
            actual_timestamp
            != expected_timestamp
        ):
            ingestion_timestamp_mismatch += 1

        if isinstance(
            actual_timestamp,
            str,
        ) and actual_timestamp:

            ingestion_timestamps.add(
                actual_timestamp
            )

    if raw_record_invalid_count > 0:
        raise RuntimeError(
            f"{raw_record_invalid_count} "
            "raw_record JSON invalide(s)."
        )

    if raw_record_id_mismatch > 0:
        raise RuntimeError(
            f"{raw_record_id_mismatch} "
            "ID(s) incohérent(s) dans raw_record."
        )

    if raw_record_content_mismatch > 0:
        raise RuntimeError(
            f"{raw_record_content_mismatch} "
            "raw_record diffèrent "
            "du Raw source."
        )

    print(
        "[OK] raw_record identique "
        "au Raw source sur toutes les lignes"
    )

    if search_keywords_invalid_count > 0:
        raise RuntimeError(
            f"{search_keywords_invalid_count} "
            "search_keywords invalide(s)."
        )

    if search_keywords_mismatch > 0:
        raise RuntimeError(
            f"{search_keywords_mismatch} "
            "search_keywords diffèrent du Raw."
        )

    if search_keyword_mismatch > 0:
        raise RuntimeError(
            f"{search_keyword_mismatch} "
            "search_keyword diffèrent du Raw."
        )

    print(
        "[OK] search_keyword/search_keywords "
        "strictement conservés"
    )

    if (
        ingestion_timestamp_mismatch
        > 0
    ):
        raise RuntimeError(
            f"{ingestion_timestamp_mismatch} "
            "ingestion_timestamp "
            "diffèrent du Raw."
        )

    if (
        len(
            ingestion_timestamps
        )
        != 1
    ):
        raise RuntimeError(
            "Le batch Bronze doit contenir "
            "un ingestion_timestamp unique."
        )

    print(
        "[OK] ingestion_timestamp "
        "strictement conservé et unique"
    )

    # ========================================================
    # 25. Résumé
    # ========================================================

    print()
    print("-" * 70)
    print("RÉSUMÉ")
    print("-" * 70)

    print(
        f"Batch ID                : "
        f"{batch_id}"
    )

    print(
        f"Processing Run ID       : "
        f"{processing_run_id}"
    )

    print(
        f"Schema version          : "
        f"{EXPECTED_SCHEMA_VERSION}"
    )

    print(
        f"Path version            : "
        f"{EXPECTED_PATH_VERSION}"
    )

    print(
        f"Lignes Raw              : "
        f"{len(raw_jobs)}"
    )

    print(
        f"Lignes Bronze           : "
        f"{len(bronze_df)}"
    )

    print(
        f"IDs uniques Bronze      : "
        f"{len(bronze_ids)}"
    )

    print(
        f"Colonnes Bronze         : "
        f"{len(bronze_df.columns)}"
    )

    print(
        f"Schema drift détecté    : "
        f"{schema_drift_detected}"
    )

    print(
        f"Schema drift fields     : "
        f"{schema_drift_field_count}"
    )

    print(
        f"Git branch              : "
        f"{git_context['git_branch']}"
    )

    print(
        f"Git commit              : "
        f"{git_context['git_commit_sha']}"
    )

    print(
        f"Git worktree dirty      : "
        f"{git_context['git_worktree_dirty']}"
    )

    print(
        f"Processing started UTC  : "
        f"{git_context['processing_started_at_utc']}"
    )

    print(
        f"Objet Raw               : "
        f"{raw_object_key}"
    )

    print(
        f"Objet Bronze            : "
        f"{bronze_object_key}"
    )

    print(
        f"Taille Bronze           : "
        f"{len(bronze_bytes)} octets"
    )

    print(
        f"SHA-256 Raw             : "
        f"{raw_sha256_actual}"
    )

    print(
        f"SHA-256 Bronze          : "
        f"{bronze_sha256}"
    )

    print()
    print(
        "STATUT BRONZE V2 : VALIDE"
    )
    print()

    return {
        "statut": (
            "VALIDE"
        ),

        "batch_id": (
            batch_id
        ),

        "processing_run_id": (
            processing_run_id
        ),

        "schema_version": (
            EXPECTED_SCHEMA_VERSION
        ),

        "path_version": (
            EXPECTED_PATH_VERSION
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

        "nombre_ids_uniques": (
            len(
                bronze_ids
            )
        ),

        "nombre_colonnes_bronze": (
            len(
                bronze_df.columns
            )
        ),

        "schema_drift_detected": (
            schema_drift_detected
        ),

        "schema_drift_field_count": (
            schema_drift_field_count
        ),

        "git_commit_sha": (
            git_context[
                "git_commit_sha"
            ]
        ),

        "git_worktree_dirty": (
            git_context[
                "git_worktree_dirty"
            ]
        ),

        "git_branch": (
            git_context[
                "git_branch"
            ]
        ),

        "raw_object_key": (
            raw_object_key
        ),

        "bronze_object_key": (
            bronze_object_key
        ),

        "raw_sha256": (
            raw_sha256_actual
        ),

        "bronze_sha256": (
            bronze_sha256
        ),
    }


# ============================================================
# CLI
# ============================================================

def main() -> None:
    """
    Point d'entrée CLI du Quality Gate Bronze v2.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Contrôle qualité d'un artefact "
            "Bronze processing_v2 "
            "France Travail stocké dans R2."
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

    args = parser.parse_args()

    validate_bronze_object(
        bronze_object_key=(
            args.bronze_object_key
        )
    )


if __name__ == "__main__":
    main()