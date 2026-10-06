from src.storage.r2_storage import R2Storage


TEST_KEY = "tests/r2_storage/test.json"

TEST_PAYLOAD = {
    "projet": "job-market-data",
    "source": "test",
    "message": "Validation abstraction R2Storage",
    "nombre_offres": 971,
}


def main() -> None:

    storage = R2Storage()

    print("Test R2Storage")
    print()

    # Nettoyage préventif uniquement pour le test
    if storage.object_exists(TEST_KEY):
        storage.delete_object(TEST_KEY)

    # --------------------------------------------------
    # Écriture
    # --------------------------------------------------

    resultat = storage.upload_json(
        object_key=TEST_KEY,
        payload=TEST_PAYLOAD,
        metadata={
            "layer": "test",
            "project": "job-market-data",
        },
    )

    print("[OK] Upload JSON")
    print(f"Bucket      : {resultat['bucket']}")
    print(f"Object key  : {resultat['object_key']}")
    print(f"Taille      : {resultat['size_bytes']} octets")
    print(f"SHA-256     : {resultat['sha256']}")
    print()

    # --------------------------------------------------
    # Existence
    # --------------------------------------------------

    if not storage.object_exists(TEST_KEY):
        raise RuntimeError(
            "L'objet devrait exister après l'upload."
        )

    print("[OK] Vérification existence")

    # --------------------------------------------------
    # Lecture
    # --------------------------------------------------

    payload_relu = storage.download_json(
        TEST_KEY
    )

    if payload_relu != TEST_PAYLOAD:
        raise RuntimeError(
            "Le JSON relu ne correspond pas "
            "au JSON envoyé."
        )

    print("[OK] Lecture JSON")
    print("[OK] Intégrité fonctionnelle du contenu")
    print()

    # --------------------------------------------------
    # Test d'immutabilité
    # --------------------------------------------------

    try:
        storage.upload_json(
            object_key=TEST_KEY,
            payload=TEST_PAYLOAD,
        )

        raise RuntimeError(
            "Le test d'immutabilité aurait dû échouer."
        )

    except FileExistsError:
        print(
            "[OK] Protection contre "
            "l'écrasement validée"
        )

    # --------------------------------------------------
    # Suppression du fichier de test
    # --------------------------------------------------

    storage.delete_object(
        TEST_KEY
    )

    if storage.object_exists(TEST_KEY):
        raise RuntimeError(
            "L'objet existe encore après suppression."
        )

    print("[OK] Suppression du fichier de test")

    print()
    print(
        "Abstraction R2Storage validée avec succès."
    )


if __name__ == "__main__":
    main()
