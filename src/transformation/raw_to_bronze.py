import argparse
import hashlib
import json
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd

from src.storage.r2_paths import (
    build_bronze_object_key,
    parse_data_lake_object_key,
)
from src.storage.r2_storage import R2Storage
from src.utils.logger import logger
from src.utils.run_context import (
    ProcessingRunContext,
    build_processing_run_context,
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

BRONZE_SCHEMA_VERSION = "2.0.0"

BRONZE_PATH_VERSION = "processing_v2"


# ============================================================
# Champs dictionnaires aplatis
# ============================================================

FLAT_DICT_FIELDS = {
    "lieuTravail": [
        "libelle",
        "latitude",
        "longitude",
        "codePostal",
        "commune",
    ],

    "entreprise": [
        "nom",
        "description",
        "entrepriseAdaptee",
    ],

    "salaire": [
        "libelle",
    ],

    "origineOffre": [
        "origine",
        "urlOrigine",
    ],

    "contexteTravail": [
        "horaires",
    ],
}


# ============================================================
# Champs complexes conservés sous forme JSON
# ============================================================

JSON_STRING_FIELDS = {
    "competences",
    "formations",
    "langues",
    "qualitesProfessionnelles",
    "contact",
    "agence",
    "permis",

    # Liste des requêtes d'acquisition ayant trouvé l'offre.
    "search_keywords",
}


# ============================================================
# Sérialisation JSON
# ============================================================

def to_json_string(
    value: Any,
) -> str | None:
    """
    Convertit une structure complexe Python en chaîne JSON.

    Règles :
    - None reste None ;
    - une chaîne reste une chaîne ;
    - listes / dictionnaires / autres structures
      sont sérialisés en JSON UTF-8.
    """

    if value is None:
        return None

    if isinstance(
        value,
        str,
    ):
        return value

    return json.dumps(
        value,
        ensure_ascii=False,
        default=str,
        separators=(
            ",",
            ":",
        ),
    )


# ============================================================
# Contrôle de la structure Raw
# ============================================================

def validate_raw_records(
    jobs: list[dict[str, Any]],
) -> None:
    """
    Vérifie les préconditions minimales avant transformation.

    Le Raw quality check existe déjà, mais la transformation
    ne doit pas supposer aveuglément que sa source est valide.
    """

    if not jobs:
        raise RuntimeError(
            "Le dataset Raw est vide."
        )

    invalid_records = [
        index
        for index, job
        in enumerate(jobs)
        if not isinstance(
            job,
            dict,
        )
    ]

    if invalid_records:
        raise RuntimeError(
            f"{len(invalid_records)} "
            "enregistrement(s) Raw "
            "ne sont pas des objets JSON."
        )

    missing_ids = []

    ids = []

    for index, job in enumerate(
        jobs
    ):

        job_id = job.get(
            "id"
        )

        if (
            job_id is None
            or str(
                job_id
            ).strip()
            == ""
        ):

            missing_ids.append(
                index
            )

            continue

        ids.append(
            str(
                job_id
            )
        )

    if missing_ids:
        raise RuntimeError(
            f"{len(missing_ids)} "
            "offre(s) Raw sans identifiant."
        )

    duplicate_count = (
        len(ids)
        - len(
            set(ids)
        )
    )

    if duplicate_count > 0:
        raise RuntimeError(
            f"{duplicate_count} "
            "doublon(s) d'ID détecté(s) "
            "dans le Raw."
        )

    invalid_search_keywords = 0

    invalid_primary_keyword = 0

    for job in jobs:

        search_keywords = (
            job.get(
                "search_keywords"
            )
        )

        search_keyword = (
            job.get(
                "search_keyword"
            )
        )

        if (
            not isinstance(
                search_keywords,
                list,
            )
            or not search_keywords
        ):

            invalid_search_keywords += 1

            continue

        if (
            search_keyword
            not in search_keywords
        ):

            invalid_primary_keyword += 1

    if invalid_search_keywords > 0:
        raise RuntimeError(
            f"{invalid_search_keywords} "
            "offre(s) Raw avec "
            "search_keywords invalide."
        )

    if invalid_primary_keyword > 0:
        raise RuntimeError(
            f"{invalid_primary_keyword} "
            "offre(s) où search_keyword "
            "n'appartient pas à "
            "search_keywords."
        )


# ============================================================
# Détection de schema drift
# ============================================================

def discover_flat_dict_fields(
    jobs: list[dict[str, Any]],
) -> dict[str, set[str]]:
    """
    Découvre les sous-clés réellement présentes
    dans les objets dictionnaires que nous aplatissons.

    Cette fonction ne modifie pas le schéma Bronze.
    Elle sert uniquement au monitoring du schema drift.
    """

    discovered: dict[
        str,
        set[str],
    ] = {
        field: set()
        for field
        in FLAT_DICT_FIELDS
    }

    for job in jobs:

        for field in (
            FLAT_DICT_FIELDS
        ):

            value = job.get(
                field
            )

            if value is None:
                continue

            if not isinstance(
                value,
                dict,
            ):
                raise RuntimeError(
                    "Schema drift incompatible : "
                    f"le champ '{field}' "
                    "devrait être un objet JSON "
                    f"mais contient {type(value).__name__}."
                )

            discovered[
                field
            ].update(
                value.keys()
            )

    return discovered


def check_schema_drift(
    jobs: list[dict[str, Any]],
) -> dict[str, list[str]]:
    """
    Détecte les nouvelles sous-clés présentes dans les
    dictionnaires configurés pour aplatissement.

    Politique :

    - changement de TYPE :
        erreur bloquante ;

    - nouvelle sous-clé additive :
        warning non bloquant.

    Les nouvelles informations restent intégralement
    disponibles dans raw_record, donc aucune donnée
    source n'est perdue silencieusement.
    """

    discovered = (
        discover_flat_dict_fields(
            jobs
        )
    )

    drift: dict[
        str,
        list[str],
    ] = {}

    for field, actual_keys in (
        discovered.items()
    ):

        expected_keys = set(
            FLAT_DICT_FIELDS[
                field
            ]
        )

        unexpected_keys = sorted(
            actual_keys
            - expected_keys
        )

        if unexpected_keys:

            drift[
                field
            ] = unexpected_keys

            logger.warning(
                "Schema drift additif détecté "
                f"sur '{field}' : "
                f"{unexpected_keys}. "
                "Ces champs restent préservés "
                "dans raw_record."
            )

    return drift


# ============================================================
# Aplatissement d'une offre
# ============================================================

def flatten_job(
    job: dict[str, Any],
    raw_source_file: str,
    raw_source_object_key: str,
    batch_id: str,
    processing_run_id: str,
) -> dict[str, Any]:
    """
    Transforme une offre Raw en enregistrement Bronze.

    Principes :
    - primitives conservées ;
    - dictionnaires configurés aplatis ;
    - structures complexes configurées sérialisées ;
    - structures complexes non configurées également
      sérialisées pour éviter une perte ;
    - raw_record conserve l'enregistrement Raw complet ;
    - lineage technique ajouté.
    """

    flattened: dict[
        str,
        Any,
    ] = {}

    # --------------------------------------------------------
    # Initialisation des colonnes aplaties
    # --------------------------------------------------------
    #
    # Cela garantit un schéma stable même lorsqu'un
    # dictionnaire est absent sur une offre.
    #
    # --------------------------------------------------------

    for field, nested_fields in (
        FLAT_DICT_FIELDS.items()
    ):

        for nested_field in (
            nested_fields
        ):

            flattened[
                f"{field}_{nested_field}"
            ] = None

    # --------------------------------------------------------
    # Parcours de l'enregistrement Raw
    # --------------------------------------------------------

    for key, value in (
        job.items()
    ):

        # ----------------------------------------------------
        # Dictionnaires explicitement aplatis
        # ----------------------------------------------------

        if key in FLAT_DICT_FIELDS:

            if value is None:
                continue

            if not isinstance(
                value,
                dict,
            ):
                raise RuntimeError(
                    f"Le champ '{key}' "
                    "n'a pas le type dict attendu."
                )

            for nested_field in (
                FLAT_DICT_FIELDS[
                    key
                ]
            ):

                flattened[
                    f"{key}_{nested_field}"
                ] = (
                    value.get(
                        nested_field
                    )
                )

            continue

        # ----------------------------------------------------
        # Champs explicitement conservés en JSON string
        # ----------------------------------------------------

        if key in JSON_STRING_FIELDS:

            flattened[
                key
            ] = to_json_string(
                value
            )

            continue

        # ----------------------------------------------------
        # Structures complexes non configurées
        # ----------------------------------------------------
        #
        # On ne les abandonne jamais silencieusement.
        #
        # ----------------------------------------------------

        if isinstance(
            value,
            (
                dict,
                list,
                tuple,
                set,
            ),
        ):

            flattened[
                key
            ] = to_json_string(
                value
            )

            continue

        # ----------------------------------------------------
        # Valeurs scalaires
        # ----------------------------------------------------

        flattened[
            key
        ] = value

    # --------------------------------------------------------
    # Colonnes techniques de lineage
    # --------------------------------------------------------

    flattened[
        "batch_id"
    ] = batch_id

    flattened[
        "processing_run_id"
    ] = processing_run_id

    flattened[
        "bronze_schema_version"
    ] = BRONZE_SCHEMA_VERSION

    flattened[
        "raw_source_file"
    ] = raw_source_file

    flattened[
        "raw_source_object_key"
    ] = raw_source_object_key

    # --------------------------------------------------------
    # Copie complète de l'enregistrement source
    # --------------------------------------------------------

    flattened[
        "raw_record"
    ] = json.dumps(
        job,
        ensure_ascii=False,
        default=str,
        separators=(
            ",",
            ":",
        ),
    )

    return flattened


# ============================================================
# Contrôle de non-perte
# ============================================================

def validate_transformation_integrity(
    raw_jobs: list[dict[str, Any]],
    bronze_df: pd.DataFrame,
) -> None:
    """
    Garantit que Raw -> Bronze ne perd aucune offre.

    Vérifie :
    - même nombre de lignes ;
    - aucun ID manquant ;
    - aucun doublon ;
    - ensemble d'IDs strictement identique.
    """

    if (
        len(raw_jobs)
        != len(bronze_df)
    ):

        raise RuntimeError(
            "Perte de lignes Raw -> Bronze : "
            f"Raw={len(raw_jobs)}, "
            f"Bronze={len(bronze_df)}."
        )

    if (
        "id"
        not in bronze_df.columns
    ):

        raise RuntimeError(
            "La colonne id est absente "
            "du Bronze."
        )

    bronze_missing_ids = int(
        (
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
        .sum()
    )

    if bronze_missing_ids > 0:

        raise RuntimeError(
            f"{bronze_missing_ids} "
            "ID manquant(s) dans Bronze."
        )

    bronze_duplicate_ids = int(
        bronze_df[
            "id"
        ]
        .astype(str)
        .duplicated()
        .sum()
    )

    if bronze_duplicate_ids > 0:

        raise RuntimeError(
            f"{bronze_duplicate_ids} "
            "doublon(s) d'ID dans Bronze."
        )

    raw_ids = {
        str(
            job[
                "id"
            ]
        )
        for job in raw_jobs
    }

    bronze_ids = set(
        bronze_df[
            "id"
        ]
        .astype(str)
        .tolist()
    )

    ids_missing_in_bronze = (
        raw_ids
        - bronze_ids
    )

    ids_unexpected_in_bronze = (
        bronze_ids
        - raw_ids
    )

    if ids_missing_in_bronze:

        raise RuntimeError(
            f"{len(ids_missing_in_bronze)} "
            "ID(s) Raw absent(s) "
            "du Bronze."
        )

    if ids_unexpected_in_bronze:

        raise RuntimeError(
            f"{len(ids_unexpected_in_bronze)} "
            "ID(s) Bronze absent(s) "
            "du Raw."
        )


# ============================================================
# Transformation principale
# ============================================================

def run_bronze_transformation(
    raw_object_key: str,
    processing_run_id: str | None = None,
) -> dict[str, Any]:
    """
    Transforme un snapshot Raw R2 en Bronze Parquet R2.

    La fonction :

    1. valide la clé Raw ;
    2. construit le contexte d'exécution ;
    3. valide les métadonnées Raw ;
    4. vérifie le SHA-256 du Raw ;
    5. transforme les enregistrements ;
    6. garantit 0 perte ;
    7. sérialise le Bronze en mémoire ;
    8. écrit un nouvel artefact immutable dans R2 ;
    9. retourne les informations nécessaires au pipeline.

    processing_run_id :

    - fourni :
        réutilisé après validation ;

    - absent :
        créé automatiquement.

    Airflow fournira plus tard explicitement cet ID.
    """

    logger.info(
        "Début transformation Raw -> Bronze"
    )

    # ========================================================
    # 1. Analyse de la clé Raw
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

        raise ValueError(
            "L'objet source doit appartenir "
            "à la couche Raw."
        )

    source = str(
        raw_parts[
            "source"
        ]
    )

    ingestion_date = str(
        raw_parts[
            "ingestion_date"
        ]
    )

    batch_id = str(
        raw_parts[
            "batch_id"
        ]
    )

    raw_source_file = str(
        raw_parts[
            "filename"
        ]
    )

    if (
        source
        != SOURCE_NAME
    ):

        raise RuntimeError(
            "Source Raw inattendue : "
            f"{source}. "
            f"Attendu : {SOURCE_NAME}."
        )

    # ========================================================
    # 2. Contexte d'exécution
    # ========================================================

    context: ProcessingRunContext = (
        build_processing_run_context(
            processing_run_id=(
                processing_run_id
            ),
            repo_dir=ROOT_DIR,
        )
    )

    resolved_processing_run_id = (
        context.processing_run_id
    )

    logger.info(
        "processing_run_id : "
        f"{resolved_processing_run_id}"
    )

    logger.info(
        "Git commit : "
        f"{context.git_commit_sha}"
    )

    logger.info(
        "Git worktree dirty : "
        f"{context.git_worktree_dirty}"
    )

    # ========================================================
    # 3. Initialisation R2
    # ========================================================

    storage = R2Storage()

    if not storage.object_exists(
        raw_object_key
    ):

        raise FileNotFoundError(
            "Objet Raw introuvable "
            "dans R2 : "
            f"{raw_object_key}"
        )

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

    # ========================================================
    # 4. Validation des métadonnées Raw
    # ========================================================

    if (
        raw_metadata.get(
            "layer"
        )
        != "raw"
    ):

        raise RuntimeError(
            "Métadonnée layer Raw incorrecte."
        )

    if (
        raw_metadata.get(
            "source"
        )
        != source
    ):

        raise RuntimeError(
            "Métadonnée source Raw incorrecte."
        )

    if (
        raw_metadata.get(
            "batch-id"
        )
        != batch_id
    ):

        raise RuntimeError(
            "batch_id incohérent entre "
            "le chemin Raw et ses métadonnées."
        )

    if (
        raw_metadata.get(
            "ingestion-date-utc"
        )
        != ingestion_date
    ):

        raise RuntimeError(
            "Date d'ingestion incohérente "
            "entre le chemin Raw "
            "et ses métadonnées."
        )

    raw_sha256_metadata = (
        raw_metadata.get(
            "sha256"
        )
    )

    if not raw_sha256_metadata:

        raise RuntimeError(
            "SHA-256 absent des métadonnées Raw."
        )

    # ========================================================
    # 5. Téléchargement Raw
    # ========================================================

    raw_bytes = (
        storage.download_bytes(
            raw_object_key
        )
    )

    if not raw_bytes:

        raise RuntimeError(
            "L'objet Raw téléchargé est vide."
        )

    if (
        len(raw_bytes)
        != raw_info[
            "size_bytes"
        ]
    ):

        raise RuntimeError(
            "La taille de l'objet Raw téléchargé "
            "ne correspond pas aux métadonnées R2."
        )

    # ========================================================
    # 6. Contrôle SHA-256 Raw
    # ========================================================

    raw_sha256_calculated = (
        hashlib.sha256(
            raw_bytes
        )
        .hexdigest()
    )

    if (
        raw_sha256_calculated
        != raw_sha256_metadata
    ):

        raise RuntimeError(
            "Échec du contrôle SHA-256 Raw."
        )

    # ========================================================
    # 7. Parsing JSON Raw
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
            "Impossible de décoder "
            "le JSON Raw."
        ) from exc

    if not isinstance(
        raw_jobs,
        list,
    ):

        raise RuntimeError(
            "Le contenu Raw doit être "
            "une liste JSON."
        )

    # ========================================================
    # 8. Cohérence record-count
    # ========================================================

    raw_record_count_metadata = (
        raw_metadata.get(
            "record-count"
        )
    )

    if (
        raw_record_count_metadata
        is None
    ):

        raise RuntimeError(
            "record-count absent "
            "des métadonnées Raw."
        )

    try:

        expected_raw_count = int(
            raw_record_count_metadata
        )

    except ValueError as exc:

        raise RuntimeError(
            "record-count Raw "
            "n'est pas un entier valide."
        ) from exc

    if (
        expected_raw_count
        != len(raw_jobs)
    ):

        raise RuntimeError(
            "Le volume du Raw ne correspond "
            "pas à record-count : "
            f"metadata={expected_raw_count}, "
            f"payload={len(raw_jobs)}."
        )

    logger.info(
        f"{len(raw_jobs)} "
        "offres Raw à transformer"
    )

    # ========================================================
    # 9. Préconditions Raw
    # ========================================================

    validate_raw_records(
        raw_jobs
    )

    # ========================================================
    # 10. Schema drift
    # ========================================================

    schema_drift = (
        check_schema_drift(
            raw_jobs
        )
    )

    schema_drift_detected = bool(
        schema_drift
    )

    # ========================================================
    # 11. Transformation Raw -> Bronze
    # ========================================================

    bronze_records = [
        flatten_job(
            job=job,
            raw_source_file=(
                raw_source_file
            ),
            raw_source_object_key=(
                raw_object_key
            ),
            batch_id=(
                batch_id
            ),
            processing_run_id=(
                resolved_processing_run_id
            ),
        )
        for job in raw_jobs
    ]

    bronze_df = pd.DataFrame(
        bronze_records
    )

    # ========================================================
    # 12. Garantie stricte de non-perte
    # ========================================================

    validate_transformation_integrity(
        raw_jobs=raw_jobs,
        bronze_df=bronze_df,
    )

    logger.info(
        "Garantie de non-perte validée : "
        f"{len(raw_jobs)} Raw -> "
        f"{len(bronze_df)} Bronze"
    )

    # ========================================================
    # 13. Construction de la clé Bronze v2
    # ========================================================

    bronze_object_key = (
        build_bronze_object_key(
            source=source,
            batch_id=batch_id,
            ingestion_date=(
                ingestion_date
            ),
            processing_run_id=(
                resolved_processing_run_id
            ),
            filename="offres.parquet",
        )
    )

    parsed_bronze_key = (
        parse_data_lake_object_key(
            bronze_object_key
        )
    )

    if (
        parsed_bronze_key[
            "path_version"
        ]
        != BRONZE_PATH_VERSION
    ):

        raise RuntimeError(
            "Le chemin Bronze généré "
            "n'utilise pas processing_v2."
        )

    # ========================================================
    # 14. Sérialisation Parquet en mémoire
    # ========================================================

    parquet_buffer = BytesIO()

    bronze_df.to_parquet(
        parquet_buffer,
        index=False,
        engine="pyarrow",
    )

    bronze_bytes = (
        parquet_buffer.getvalue()
    )

    if not bronze_bytes:

        raise RuntimeError(
            "La sérialisation Bronze "
            "a produit un fichier vide."
        )

    # ========================================================
    # 15. Métadonnées Bronze
    # ========================================================

    bronze_metadata = {
        # ----------------------------------------------------
        # Identité
        # ----------------------------------------------------
        "layer": "bronze",
        "source": source,

        # ----------------------------------------------------
        # Dataset
        # ----------------------------------------------------
        "batch-id": batch_id,
        "ingestion-date-utc": (
            ingestion_date
        ),

        # ----------------------------------------------------
        # Contrat
        # ----------------------------------------------------
        "schema-version": (
            BRONZE_SCHEMA_VERSION
        ),
        "path-version": (
            BRONZE_PATH_VERSION
        ),

        # ----------------------------------------------------
        # Volume
        # ----------------------------------------------------
        "record-count": str(
            len(
                bronze_df
            )
        ),
        "column-count": str(
            bronze_df.shape[
                1
            ]
        ),

        # ----------------------------------------------------
        # Lineage parent générique
        # ----------------------------------------------------
        "parent-layer": "raw",
        "parent-object-key": (
            raw_object_key
        ),
        "parent-sha256": (
            raw_sha256_calculated
        ),

        # ----------------------------------------------------
        # Compatibilité explicite Raw
        # ----------------------------------------------------
        "raw-object-key": (
            raw_object_key
        ),
        "raw-sha256": (
            raw_sha256_calculated
        ),

        # ----------------------------------------------------
        # Schema drift
        # ----------------------------------------------------
        "schema-drift-detected": (
            str(
                schema_drift_detected
            )
            .lower()
        ),
        "schema-drift-field-count": str(
            len(
                schema_drift
            )
        ),
    }

    # --------------------------------------------------------
    # Provenance d'exécution / Git
    # --------------------------------------------------------

    bronze_metadata.update(
        context.to_r2_metadata()
    )

    # ========================================================
    # 16. Protection contre un écrasement
    # ========================================================

    if storage.object_exists(
        bronze_object_key
    ):

        raise RuntimeError(
            "L'objet Bronze existe déjà. "
            "Un processing_run_id doit identifier "
            "une exécution unique : "
            f"{bronze_object_key}"
        )

    # ========================================================
    # 17. Upload R2
    # ========================================================

    upload_result = (
        storage.upload_bytes(
            object_key=(
                bronze_object_key
            ),
            data=bronze_bytes,
            content_type=(
                "application/vnd.apache.parquet"
            ),
            metadata=(
                bronze_metadata
            ),
            overwrite=False,
        )
    )

    # ========================================================
    # 18. Validation post-upload
    # ========================================================

    if not storage.object_exists(
        bronze_object_key
    ):

        raise RuntimeError(
            "Le Bronze n'est pas accessible "
            "dans R2 après upload."
        )

    bronze_info_after_upload = (
        storage.get_object_info(
            bronze_object_key
        )
    )

    uploaded_metadata = (
        bronze_info_after_upload.get(
            "metadata",
            {},
        )
    )

    if (
        uploaded_metadata.get(
            "processing-run-id"
        )
        != resolved_processing_run_id
    ):

        raise RuntimeError(
            "processing_run_id absent "
            "ou incohérent après upload."
        )

    if (
        uploaded_metadata.get(
            "schema-version"
        )
        != BRONZE_SCHEMA_VERSION
    ):

        raise RuntimeError(
            "schema_version Bronze absent "
            "ou incohérent après upload."
        )

    # ========================================================
    # 19. Résumé terminal
    # ========================================================

    print()
    print("=" * 70)
    print(
        "TRANSFORMATION RAW -> BRONZE TERMINÉE"
    )
    print("=" * 70)
    print()

    print(
        f"Batch ID              : "
        f"{batch_id}"
    )

    print(
        f"Processing Run ID     : "
        f"{resolved_processing_run_id}"
    )

    print(
        f"Schema version        : "
        f"{BRONZE_SCHEMA_VERSION}"
    )

    print(
        f"Path version          : "
        f"{BRONZE_PATH_VERSION}"
    )

    print(
        f"Lignes Raw            : "
        f"{len(raw_jobs)}"
    )

    print(
        f"Lignes Bronze         : "
        f"{len(bronze_df)}"
    )

    print(
        f"IDs uniques           : "
        f"{bronze_df['id'].nunique(dropna=True)}"
    )

    print(
        f"Colonnes Bronze       : "
        f"{bronze_df.shape[1]}"
    )

    print(
        f"Schema drift détecté  : "
        f"{schema_drift_detected}"
    )

    print(
        f"Git branch            : "
        f"{context.git_branch}"
    )

    print(
        f"Git commit            : "
        f"{context.git_commit_sha}"
    )

    print(
        f"Git worktree dirty    : "
        f"{context.git_worktree_dirty}"
    )

    print(
        f"Objet Raw             : "
        f"{raw_object_key}"
    )

    print(
        f"Objet Bronze          : "
        f"{bronze_object_key}"
    )

    print(
        f"Taille Bronze         : "
        f"{upload_result['size_bytes']} octets"
    )

    print(
        f"SHA-256 Bronze        : "
        f"{upload_result['sha256']}"
    )

    print()

    # ========================================================
    # 20. Résultat pipeline
    # ========================================================

    return {
        "batch_id": (
            batch_id
        ),

        "processing_run_id": (
            resolved_processing_run_id
        ),

        "source": (
            source
        ),

        "ingestion_date": (
            ingestion_date
        ),

        "schema_version": (
            BRONZE_SCHEMA_VERSION
        ),

        "path_version": (
            BRONZE_PATH_VERSION
        ),

        "raw_object_key": (
            raw_object_key
        ),

        "raw_sha256": (
            raw_sha256_calculated
        ),

        "bronze_object_key": (
            bronze_object_key
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

        "nombre_colonnes_bronze": (
            bronze_df.shape[
                1
            ]
        ),

        "schema_drift_detected": (
            schema_drift_detected
        ),

        "schema_drift": (
            schema_drift
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
    Point d'entrée CLI.

    En développement :

        --processing-run-id peut être omis.
        Un nouvel UUID sera créé.

    Avec Airflow :

        --processing-run-id sera fourni explicitement
        afin de propager le même contexte d'exécution.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Transformation d'un snapshot Raw "
            "France Travail stocké dans Cloudflare R2 "
            "vers Bronze Parquet R2."
        )
    )

    parser.add_argument(
        "--raw-object-key",
        required=True,
        help=(
            "Clé de l'objet Raw "
            "dans Cloudflare R2."
        ),
    )

    parser.add_argument(
        "--processing-run-id",
        required=False,
        default=None,
        help=(
            "UUID de l'exécution logique du pipeline. "
            "S'il est absent, un nouvel UUID est généré."
        ),
    )

    args = parser.parse_args()

    run_bronze_transformation(
        raw_object_key=(
            args.raw_object_key
        ),
        processing_run_id=(
            args.processing_run_id
        ),
    )


if __name__ == "__main__":
    main()