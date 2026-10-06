from botocore.exceptions import BotoCoreError, ClientError

from src.storage.r2_client import (
    get_r2_bucket_name,
    get_r2_client,
)


TEST_KEY = "tests/connexion_r2.txt"
TEST_CONTENT = (
    "Connexion Cloudflare R2 valide pour le projet job-market-data."
)


def main() -> None:
    """
    Valide le cycle minimal d'accès au data lake R2 :

    1. connexion ;
    2. écriture d'un objet ;
    3. vérification de l'objet ;
    4. lecture ;
    5. suppression ;
    6. vérification de la suppression.
    """

    bucket = get_r2_bucket_name()
    client = get_r2_client()

    print("Test de connexion Cloudflare R2")
    print(f"Bucket : {bucket}")
    print()

    try:
        # --------------------------------------------------
        # 1. Écriture
        # --------------------------------------------------

        client.put_object(
            Bucket=bucket,
            Key=TEST_KEY,
            Body=TEST_CONTENT.encode("utf-8"),
            ContentType="text/plain",
        )

        print(f"[OK] Objet créé : {TEST_KEY}")

        # --------------------------------------------------
        # 2. Vérification metadata
        # --------------------------------------------------

        metadata = client.head_object(
            Bucket=bucket,
            Key=TEST_KEY,
        )

        print(
            "[OK] Objet accessible "
            f"({metadata['ContentLength']} octets)"
        )

        # --------------------------------------------------
        # 3. Lecture
        # --------------------------------------------------

        response = client.get_object(
            Bucket=bucket,
            Key=TEST_KEY,
        )

        contenu = (
            response["Body"]
            .read()
            .decode("utf-8")
        )

        if contenu != TEST_CONTENT:
            raise RuntimeError(
                "Le contenu relu depuis R2 "
                "ne correspond pas au contenu écrit."
            )

        print("[OK] Lecture et intégrité du contenu validées")

        # --------------------------------------------------
        # 4. Suppression
        # --------------------------------------------------

        client.delete_object(
            Bucket=bucket,
            Key=TEST_KEY,
        )

        print(f"[OK] Objet supprimé : {TEST_KEY}")

        print()
        print("Connexion R2 validée avec succès.")

    except (ClientError, BotoCoreError) as exc:
        print()
        print("[ERREUR] Échec de communication avec R2")
        raise exc


if __name__ == "__main__":
    main()
