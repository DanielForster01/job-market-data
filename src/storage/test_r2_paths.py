from datetime import datetime, timezone

from src.storage.r2_paths import (
    build_bronze_object_key,
    build_raw_object_key,
    build_silver_object_key,
    normalize_ingestion_date,
    normalize_source,
    normalize_uuid,
    parse_data_lake_object_key,
)


# ============================================================
# Données de test déterministes
# ============================================================

BATCH_ID = (
    "ead60f51-e84c-4078-a840-40039a81cc7b"
)

PROCESSING_RUN_ID = (
    "11111111-2222-4333-8444-555555555555"
)

INGESTION_DATE = (
    "2026-10-05"
)

INGESTION_DATETIME = datetime(
    2026,
    10,
    5,
    19,
    33,
    4,
    tzinfo=timezone.utc,
)


# ============================================================
# Helper
# ============================================================

def expect_value_error(
    function,
    *args,
    **kwargs,
) -> None:
    """
    Vérifie qu'un appel produit bien ValueError.
    """

    try:

        function(
            *args,
            **kwargs,
        )

    except ValueError:
        return

    raise AssertionError(
        "ValueError attendu mais non déclenché."
    )


# ============================================================
# Tests normalisation
# ============================================================

def test_normalization() -> None:

    assert (
        normalize_source(
            "France Travail"
        )
        == "france_travail"
    )

    assert (
        normalize_ingestion_date(
            "2026-10-05"
        )
        == "2026-10-05"
    )

    assert (
        normalize_uuid(
            BATCH_ID,
            "batch_id",
        )
        == BATCH_ID
    )

    print(
        "[OK] Normalisation"
    )


# ============================================================
# Test RAW
# ============================================================

def test_raw_path() -> None:

    expected = (
        "raw/france_travail/"
        "ingestion_date=2026-10-05/"
        f"batch_id={BATCH_ID}/"
        "offres.json"
    )

    actual = (
        build_raw_object_key(
            source="France Travail",
            batch_id=BATCH_ID,
            ingestion_datetime=(
                INGESTION_DATETIME
            ),
        )
    )

    assert actual == expected

    parsed = (
        parse_data_lake_object_key(
            actual
        )
    )

    assert parsed[
        "layer"
    ] == "raw"

    assert parsed[
        "source"
    ] == "france_travail"

    assert parsed[
        "batch_id"
    ] == BATCH_ID

    assert (
        parsed[
            "processing_run_id"
        ]
        is None
    )

    assert (
        parsed[
            "path_version"
        ]
        == "raw_v1"
    )

    print(
        "[OK] Convention Raw"
    )


# ============================================================
# Test BRONZE nouvelle architecture
# ============================================================

def test_bronze_path_v2() -> None:

    expected = (
        "bronze/france_travail/"
        "ingestion_date=2026-10-05/"
        f"batch_id={BATCH_ID}/"
        f"processing_run_id="
        f"{PROCESSING_RUN_ID}/"
        "offres.parquet"
    )

    actual = (
        build_bronze_object_key(
            source="france_travail",
            batch_id=BATCH_ID,
            ingestion_date=(
                INGESTION_DATE
            ),
            processing_run_id=(
                PROCESSING_RUN_ID
            ),
        )
    )

    assert actual == expected

    parsed = (
        parse_data_lake_object_key(
            actual
        )
    )

    assert parsed[
        "layer"
    ] == "bronze"

    assert parsed[
        "batch_id"
    ] == BATCH_ID

    assert (
        parsed[
            "processing_run_id"
        ]
        == PROCESSING_RUN_ID
    )

    assert (
        parsed[
            "path_version"
        ]
        == "processing_v2"
    )

    print(
        "[OK] Convention Bronze v2"
    )


# ============================================================
# Test SILVER nouvelle architecture
# ============================================================

def test_silver_path_v2() -> None:

    expected = (
        "silver/france_travail/"
        "ingestion_date=2026-10-05/"
        f"batch_id={BATCH_ID}/"
        f"processing_run_id="
        f"{PROCESSING_RUN_ID}/"
        "offres.parquet"
    )

    actual = (
        build_silver_object_key(
            source="france_travail",
            batch_id=BATCH_ID,
            ingestion_date=(
                INGESTION_DATE
            ),
            processing_run_id=(
                PROCESSING_RUN_ID
            ),
        )
    )

    assert actual == expected

    parsed = (
        parse_data_lake_object_key(
            actual
        )
    )

    assert parsed[
        "layer"
    ] == "silver"

    assert parsed[
        "batch_id"
    ] == BATCH_ID

    assert (
        parsed[
            "processing_run_id"
        ]
        == PROCESSING_RUN_ID
    )

    assert (
        parsed[
            "path_version"
        ]
        == "processing_v2"
    )

    print(
        "[OK] Convention Silver v2"
    )


# ============================================================
# Compatibilité historique BRONZE
# ============================================================

def test_legacy_bronze_path() -> None:

    legacy_key = (
        "bronze/france_travail/"
        "ingestion_date=2026-10-05/"
        f"batch_id={BATCH_ID}/"
        "offres.parquet"
    )

    parsed = (
        parse_data_lake_object_key(
            legacy_key
        )
    )

    assert parsed[
        "layer"
    ] == "bronze"

    assert parsed[
        "batch_id"
    ] == BATCH_ID

    assert (
        parsed[
            "processing_run_id"
        ]
        is None
    )

    assert (
        parsed[
            "path_version"
        ]
        == "legacy_v1"
    )

    print(
        "[OK] Compatibilité Bronze historique"
    )


# ============================================================
# Compatibilité historique SILVER
# ============================================================

def test_legacy_silver_path() -> None:

    legacy_key = (
        "silver/france_travail/"
        "ingestion_date=2026-10-05/"
        f"batch_id={BATCH_ID}/"
        "offres.parquet"
    )

    parsed = (
        parse_data_lake_object_key(
            legacy_key
        )
    )

    assert parsed[
        "layer"
    ] == "silver"

    assert parsed[
        "batch_id"
    ] == BATCH_ID

    assert (
        parsed[
            "processing_run_id"
        ]
        is None
    )

    assert (
        parsed[
            "path_version"
        ]
        == "legacy_v1"
    )

    print(
        "[OK] Compatibilité Silver historique"
    )


# ============================================================
# Tests d'erreurs
# ============================================================

def test_invalid_paths() -> None:

    expect_value_error(
        normalize_ingestion_date,
        "05-10-2026",
    )

    expect_value_error(
        normalize_uuid,
        "not-a-uuid",
        "batch_id",
    )

    expect_value_error(
        parse_data_lake_object_key,
        (
            "gold/france_travail/"
            "ingestion_date=2026-10-05/"
            f"batch_id={BATCH_ID}/"
            "offres.parquet"
        ),
    )

    expect_value_error(
        parse_data_lake_object_key,
        (
            "bronze/france_travail/"
            "wrong_partition=2026-10-05/"
            f"batch_id={BATCH_ID}/"
            "offres.parquet"
        ),
    )

    expect_value_error(
        build_bronze_object_key,
        source="france_travail",
        batch_id=BATCH_ID,
        ingestion_date=(
            INGESTION_DATE
        ),
        processing_run_id=(
            "invalid-run-id"
        ),
    )

    print(
        "[OK] Rejet des chemins invalides"
    )


# ============================================================
# Exécution
# ============================================================

def main() -> None:

    print()
    print("=" * 70)
    print(
        "TEST DU CONTRAT DE CHEMINS R2"
    )
    print("=" * 70)
    print()

    test_normalization()
    test_raw_path()
    test_bronze_path_v2()
    test_silver_path_v2()

    test_legacy_bronze_path()
    test_legacy_silver_path()

    test_invalid_paths()

    print()
    print("-" * 70)
    print(
        "CONTRAT DE CHEMINS R2 : VALIDE"
    )
    print("-" * 70)
    print()


if __name__ == "__main__":
    main()