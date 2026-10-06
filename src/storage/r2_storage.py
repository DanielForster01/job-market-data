import hashlib
import json
from typing import Any, Optional

from botocore.exceptions import ClientError

from src.storage.r2_client import (
    get_r2_bucket_name,
    get_r2_client,
)


class R2Storage:
    """
    Abstraction de stockage objet pour Cloudflare R2.

    Cette classe centralise les opérations techniques liées au Data Lake :
    - vérification d'existence ;
    - lecture des métadonnées ;
    - écriture de bytes ;
    - écriture JSON ;
    - lecture de bytes ;
    - lecture JSON ;
    - suppression.

    Les couches métier du projet ne doivent pas manipuler directement
    boto3 autant que possible.
    """

    def __init__(self) -> None:
        self.client = get_r2_client()
        self.bucket_name = get_r2_bucket_name()

    def object_exists(
        self,
        object_key: str,
    ) -> bool:
        """
        Vérifie si un objet existe dans le bucket.
        """

        try:
            self.client.head_object(
                Bucket=self.bucket_name,
                Key=object_key,
            )

            return True

        except ClientError as exc:

            error_code = (
                exc.response
                .get("Error", {})
                .get("Code")
            )

            if error_code in (
                "404",
                "NoSuchKey",
                "NotFound",
            ):
                return False

            raise

    def get_object_info(
        self,
        object_key: str,
    ) -> dict[str, Any]:
        """
        Retourne les informations techniques et métadonnées
        associées à un objet R2.
        """

        response = self.client.head_object(
            Bucket=self.bucket_name,
            Key=object_key,
        )

        return {
            "bucket": self.bucket_name,
            "object_key": object_key,
            "size_bytes": response.get(
                "ContentLength"
            ),
            "content_type": response.get(
                "ContentType"
            ),
            "etag": (
                response.get("ETag", "")
                .replace('"', "")
            ),
            "last_modified": response.get(
                "LastModified"
            ),
            "metadata": response.get(
                "Metadata",
                {},
            ),
        }

    def upload_bytes(
        self,
        object_key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[
            dict[str, str]
        ] = None,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        """
        Écrit un objet binaire dans R2.

        Par défaut, refuse d'écraser un objet existant afin
        de protéger le caractère immuable de la couche Raw.
        """

        if (
            not overwrite
            and self.object_exists(
                object_key
            )
        ):
            raise FileExistsError(
                f"L'objet R2 existe déjà : "
                f"{object_key}"
            )

        sha256 = hashlib.sha256(
            data
        ).hexdigest()

        object_metadata = {
            "sha256": sha256,
        }

        if metadata:
            object_metadata.update(
                metadata
            )

        response = self.client.put_object(
            Bucket=self.bucket_name,
            Key=object_key,
            Body=data,
            ContentType=content_type,
            Metadata=object_metadata,
        )

        return {
            "bucket": self.bucket_name,
            "object_key": object_key,
            "size_bytes": len(data),
            "sha256": sha256,
            "etag": response.get("ETag"),
        }

    def upload_json(
        self,
        object_key: str,
        payload: Any,
        metadata: Optional[
            dict[str, str]
        ] = None,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        """
        Sérialise puis écrit un objet JSON dans R2.
        """

        contenu_json = json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            default=str,
        )

        return self.upload_bytes(
            object_key=object_key,
            data=contenu_json.encode(
                "utf-8"
            ),
            content_type="application/json",
            metadata=metadata,
            overwrite=overwrite,
        )

    def download_bytes(
        self,
        object_key: str,
    ) -> bytes:
        """
        Télécharge un objet R2 sous forme binaire.

        Cette méthode est utile notamment pour les contrôles
        d'intégrité SHA-256.
        """

        response = self.client.get_object(
            Bucket=self.bucket_name,
            Key=object_key,
        )

        return response[
            "Body"
        ].read()

    def download_json(
        self,
        object_key: str,
    ) -> Any:
        """
        Télécharge puis désérialise un objet JSON.
        """

        contenu = self.download_bytes(
            object_key
        )

        return json.loads(
            contenu.decode("utf-8")
        )

    def delete_object(
        self,
        object_key: str,
    ) -> None:
        """
        Supprime un objet R2.

        Cette méthode existe pour les tests et objets techniques.
        Elle ne doit normalement pas être utilisée sur la couche Raw.
        """

        self.client.delete_object(
            Bucket=self.bucket_name,
            Key=object_key,
        )