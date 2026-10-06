import re
from datetime import date, datetime, timezone
from uuid import UUID


# ============================================================
# Configuration
# ============================================================

SUPPORTED_DATA_LAYERS = {
    "raw",
    "bronze",
    "silver",
}


# ============================================================
# Validation / normalisation
# ============================================================

def normalize_source(
    source: str,
) -> str:
    """
    Normalise le nom d'une source pour son utilisation
    dans les chemins du Data Lake.

    Exemple :
        "France Travail" -> "france_travail"
    """

    if not isinstance(source, str):
        raise TypeError(
            "source doit être une chaîne de caractères."
        )

    normalized = (
        source
        .strip()
        .lower()
    )

    normalized = re.sub(
        r"[^a-z0-9]+",
        "_",
        normalized,
    )

    normalized = normalized.strip("_")

    if not normalized:
        raise ValueError(
            "Le nom de source est vide après normalisation."
        )

    return normalized


def normalize_ingestion_date(
    ingestion_date: date | str,
) -> str:
    """
    Normalise et valide une date d'ingestion.

    Format retourné :
        YYYY-MM-DD
    """

    if isinstance(
        ingestion_date,
        datetime,
    ):
        return (
            ingestion_date
            .date()
            .isoformat()
        )

    if isinstance(
        ingestion_date,
        date,
    ):
        return ingestion_date.isoformat()

    if not isinstance(
        ingestion_date,
        str,
    ):
        raise TypeError(
            "ingestion_date doit être une date "
            "ou une chaîne YYYY-MM-DD."
        )

    try:

        parsed_date = date.fromisoformat(
            ingestion_date
        )

    except ValueError as exc:

        raise ValueError(
            "Date d'ingestion invalide : "
            f"{ingestion_date}. "
            "Format attendu : YYYY-MM-DD."
        ) from exc

    return parsed_date.isoformat()


def normalize_uuid(
    value: UUID | str,
    field_name: str,
) -> str:
    """
    Valide un UUID et retourne sa représentation canonique.

    Utilisé pour :
    - batch_id ;
    - processing_run_id.
    """

    try:

        parsed_uuid = UUID(
            str(value)
        )

    except (
        ValueError,
        TypeError,
        AttributeError,
    ) as exc:

        raise ValueError(
            f"{field_name} n'est pas un UUID valide : "
            f"{value}"
        ) from exc

    return str(
        parsed_uuid
    )


def validate_filename(
    filename: str,
) -> str:
    """
    Vérifie qu'un nom de fichier est valide pour une clé R2.

    Le filename doit être un simple nom de fichier,
    sans sous-répertoire.
    """

    if not isinstance(
        filename,
        str,
    ):
        raise TypeError(
            "filename doit être une chaîne."
        )

    filename = filename.strip()

    if not filename:
        raise ValueError(
            "filename ne peut pas être vide."
        )

    if (
        "/" in filename
        or "\\" in filename
    ):
        raise ValueError(
            "filename ne doit pas contenir "
            "de séparateur de répertoire."
        )

    if filename in {
        ".",
        "..",
    }:
        raise ValueError(
            "filename invalide."
        )

    return filename


def parse_partition(
    partition: str,
    expected_name: str,
) -> str:
    """
    Extrait la valeur d'une partition.

    Exemple :
        batch_id=abc
            ->
        abc
    """

    expected_prefix = (
        f"{expected_name}="
    )

    if not partition.startswith(
        expected_prefix
    ):
        raise ValueError(
            "Partition Data Lake invalide : "
            f"{partition}. "
            f"Attendu : {expected_prefix}<valeur>"
        )

    value = partition[
        len(expected_prefix):
    ]

    if not value:
        raise ValueError(
            "Valeur de partition vide : "
            f"{partition}"
        )

    return value


# ============================================================
# Construction RAW
# ============================================================

def build_raw_object_key(
    source: str,
    batch_id: UUID | str,
    ingestion_datetime: datetime | None = None,
    filename: str = "offres.json",
) -> str:
    """
    Construit la clé R2 d'un snapshot Raw.

    Raw représente directement un batch d'ingestion.

    Il n'y a pas de processing_run_id dans Raw :
    batch_id suffit à identifier de manière unique
    le snapshot récupéré depuis France Travail.

    Exemple :

    raw/france_travail/
        ingestion_date=2026-10-05/
        batch_id=<uuid>/
        offres.json
    """

    normalized_source = (
        normalize_source(
            source
        )
    )

    normalized_batch_id = (
        normalize_uuid(
            batch_id,
            "batch_id",
        )
    )

    normalized_filename = (
        validate_filename(
            filename
        )
    )

    if ingestion_datetime is None:

        ingestion_datetime = (
            datetime.now(
                timezone.utc
            )
        )

    if not isinstance(
        ingestion_datetime,
        datetime,
    ):
        raise TypeError(
            "ingestion_datetime doit être "
            "un objet datetime."
        )

    if (
        ingestion_datetime.tzinfo
        is None
    ):

        ingestion_datetime = (
            ingestion_datetime.replace(
                tzinfo=timezone.utc
            )
        )

    ingestion_datetime_utc = (
        ingestion_datetime
        .astimezone(
            timezone.utc
        )
    )

    ingestion_date = (
        ingestion_datetime_utc
        .date()
        .isoformat()
    )

    return (
        f"raw/{normalized_source}/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={normalized_batch_id}/"
        f"{normalized_filename}"
    )


# ============================================================
# Construction BRONZE
# ============================================================

def build_bronze_object_key(
    source: str,
    batch_id: UUID | str,
    ingestion_date: date | str,
    processing_run_id: UUID | str,
    filename: str = "offres.parquet",
) -> str:
    """
    Construit la clé R2 d'un artefact Bronze.

    processing_run_id identifie précisément
    l'exécution Raw -> Bronze.

    Exemple :

    bronze/france_travail/
        ingestion_date=2026-10-05/
        batch_id=<uuid>/
        processing_run_id=<uuid>/
        offres.parquet
    """

    normalized_source = (
        normalize_source(
            source
        )
    )

    normalized_batch_id = (
        normalize_uuid(
            batch_id,
            "batch_id",
        )
    )

    normalized_processing_run_id = (
        normalize_uuid(
            processing_run_id,
            "processing_run_id",
        )
    )

    normalized_ingestion_date = (
        normalize_ingestion_date(
            ingestion_date
        )
    )

    normalized_filename = (
        validate_filename(
            filename
        )
    )

    return (
        f"bronze/{normalized_source}/"
        f"ingestion_date={normalized_ingestion_date}/"
        f"batch_id={normalized_batch_id}/"
        f"processing_run_id="
        f"{normalized_processing_run_id}/"
        f"{normalized_filename}"
    )


# ============================================================
# Construction SILVER
# ============================================================

def build_silver_object_key(
    source: str,
    batch_id: UUID | str,
    ingestion_date: date | str,
    processing_run_id: UUID | str,
    filename: str = "offres.parquet",
) -> str:
    """
    Construit la clé R2 d'un artefact Silver.

    processing_run_id identifie précisément
    l'exécution Bronze -> Silver.

    Exemple :

    silver/france_travail/
        ingestion_date=2026-10-05/
        batch_id=<uuid>/
        processing_run_id=<uuid>/
        offres.parquet
    """

    normalized_source = (
        normalize_source(
            source
        )
    )

    normalized_batch_id = (
        normalize_uuid(
            batch_id,
            "batch_id",
        )
    )

    normalized_processing_run_id = (
        normalize_uuid(
            processing_run_id,
            "processing_run_id",
        )
    )

    normalized_ingestion_date = (
        normalize_ingestion_date(
            ingestion_date
        )
    )

    normalized_filename = (
        validate_filename(
            filename
        )
    )

    return (
        f"silver/{normalized_source}/"
        f"ingestion_date={normalized_ingestion_date}/"
        f"batch_id={normalized_batch_id}/"
        f"processing_run_id="
        f"{normalized_processing_run_id}/"
        f"{normalized_filename}"
    )


# ============================================================
# Parsing des clés Data Lake
# ============================================================

def parse_data_lake_object_key(
    object_key: str,
) -> dict[str, str | None]:
    """
    Analyse une clé Raw, Bronze ou Silver.

    Supporte deux versions.

    ----------------------------------------------------------
    Version actuelle
    ----------------------------------------------------------

    Raw :

        raw/source/
        ingestion_date=YYYY-MM-DD/
        batch_id=<uuid>/
        fichier

    Bronze / Silver :

        layer/source/
        ingestion_date=YYYY-MM-DD/
        batch_id=<uuid>/
        processing_run_id=<uuid>/
        fichier

    ----------------------------------------------------------
    Version historique
    ----------------------------------------------------------

    Les anciens objets Bronze/Silver créés avant
    l'introduction de processing_run_id restent lisibles :

        layer/source/
        ingestion_date=YYYY-MM-DD/
        batch_id=<uuid>/
        fichier

    Dans ce cas :

        processing_run_id = None
        path_version = legacy_v1
    """

    if not isinstance(
        object_key,
        str,
    ):
        raise TypeError(
            "object_key doit être une chaîne."
        )

    object_key = (
        object_key.strip()
    )

    if not object_key:
        raise ValueError(
            "object_key ne peut pas être vide."
        )

    if (
        object_key.startswith("/")
        or object_key.endswith("/")
    ):
        raise ValueError(
            "La clé Data Lake ne doit pas "
            "commencer ou finir par '/'."
        )

    parts = object_key.split(
        "/"
    )

    if any(
        part == ""
        for part in parts
    ):
        raise ValueError(
            "La clé Data Lake contient "
            "un segment vide."
        )

    # --------------------------------------------------------
    # Structure minimale
    # --------------------------------------------------------

    if len(parts) < 5:
        raise ValueError(
            "Clé Data Lake invalide : "
            f"{object_key}"
        )

    layer = parts[0]

    if layer not in SUPPORTED_DATA_LAYERS:
        raise ValueError(
            "Couche Data Lake inconnue : "
            f"{layer}"
        )

    source = parts[1]

    normalized_source = (
        normalize_source(
            source
        )
    )

    if source != normalized_source:
        raise ValueError(
            "Le nom de source dans la clé "
            "n'est pas normalisé : "
            f"{source}"
        )

    # --------------------------------------------------------
    # ingestion_date
    # --------------------------------------------------------

    ingestion_date_raw = (
        parse_partition(
            parts[2],
            "ingestion_date",
        )
    )

    ingestion_date = (
        normalize_ingestion_date(
            ingestion_date_raw
        )
    )

    # --------------------------------------------------------
    # batch_id
    # --------------------------------------------------------

    batch_id_raw = (
        parse_partition(
            parts[3],
            "batch_id",
        )
    )

    batch_id = (
        normalize_uuid(
            batch_id_raw,
            "batch_id",
        )
    )

    # ========================================================
    # RAW
    # ========================================================

    if layer == "raw":

        if len(parts) != 5:
            raise ValueError(
                "Structure Raw invalide. "
                "Format attendu : "
                "raw/source/"
                "ingestion_date=YYYY-MM-DD/"
                "batch_id=<uuid>/"
                "fichier"
            )

        filename = (
            validate_filename(
                parts[4]
            )
        )

        return {
            "layer": layer,
            "source": source,
            "ingestion_date": (
                ingestion_date
            ),
            "batch_id": (
                batch_id
            ),
            "processing_run_id": None,
            "filename": filename,
            "path_version": "raw_v1",
        }

    # ========================================================
    # BRONZE / SILVER - architecture actuelle
    # ========================================================

    if len(parts) == 6:

        processing_run_id_raw = (
            parse_partition(
                parts[4],
                "processing_run_id",
            )
        )

        processing_run_id = (
            normalize_uuid(
                processing_run_id_raw,
                "processing_run_id",
            )
        )

        filename = (
            validate_filename(
                parts[5]
            )
        )

        return {
            "layer": layer,
            "source": source,
            "ingestion_date": (
                ingestion_date
            ),
            "batch_id": (
                batch_id
            ),
            "processing_run_id": (
                processing_run_id
            ),
            "filename": filename,
            "path_version": "processing_v2",
        }

    # ========================================================
    # BRONZE / SILVER - historique
    # ========================================================
    #
    # Compatibilité avec les objets déjà présents dans R2 :
    #
    # bronze/.../batch_id=<uuid>/offres.parquet
    # silver/.../batch_id=<uuid>/offres.parquet
    #
    # On ne les supprime pas.
    #
    # ========================================================

    if len(parts) == 5:

        filename = (
            validate_filename(
                parts[4]
            )
        )

        return {
            "layer": layer,
            "source": source,
            "ingestion_date": (
                ingestion_date
            ),
            "batch_id": (
                batch_id
            ),
            "processing_run_id": None,
            "filename": filename,
            "path_version": "legacy_v1",
        }

    raise ValueError(
        "Structure de clé Data Lake "
        "non reconnue : "
        f"{object_key}"
    )