import os

import boto3
from botocore.client import BaseClient
from dotenv import load_dotenv


load_dotenv()


def get_r2_client() -> BaseClient:
    """
    Crée et retourne un client S3 configuré pour Cloudflare R2.

    Les credentials et paramètres de connexion sont chargés
    exclusivement depuis les variables d'environnement.
    """

    endpoint_url = os.getenv("R2_ENDPOINT_URL")
    access_key_id = os.getenv("R2_ACCESS_KEY_ID")
    secret_access_key = os.getenv("R2_SECRET_ACCESS_KEY")
    region = os.getenv("R2_REGION", "auto")

    variables_manquantes = []

    if not endpoint_url:
        variables_manquantes.append("R2_ENDPOINT_URL")

    if not access_key_id:
        variables_manquantes.append("R2_ACCESS_KEY_ID")

    if not secret_access_key:
        variables_manquantes.append("R2_SECRET_ACCESS_KEY")

    if variables_manquantes:
        raise RuntimeError(
            "Variables R2 manquantes : "
            + ", ".join(variables_manquantes)
        )

    return boto3.client(
        service_name="s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region,
    )


def get_r2_bucket_name() -> str:
    """
    Retourne le nom du bucket R2 configuré.
    """

    bucket_name = os.getenv("R2_BUCKET_NAME")

    if not bucket_name:
        raise RuntimeError(
            "Variable R2_BUCKET_NAME manquante."
        )

    return bucket_name
