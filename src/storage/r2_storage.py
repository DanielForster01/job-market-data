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

    Cette classe centralise les opérations techniques
    liées au Data Lake :
    - vérification d'existence ;
    - lecture des métadonnées ;
    - écriture de bytes ;
    - écriture JSON ;
    - lecture de bytes ;
    - lecture JSON ;
    - suppression.

    Par défaut, les écritures utilisent une sémantique
    create-only :

        une clé absente peut être créée ;
        une clé existante ne peut pas être remplacée.

    Cette propriété protège notamment l'immutabilité
    de la couche Raw.

    Les couches métier du projet ne doivent pas manipuler
    directement boto3 autant que possible.
    """

    def __init__(self) -> None:
        self.client = get_r2_client()
        self.bucket_name = get_r2_bucket_name()

    # ========================================================
    # Existence
    # ========================================================

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

            http_status = (
                exc.response
                .get("ResponseMetadata", {})
                .get("HTTPStatusCode")
            )

            if (
                error_code
                in (
                    "404",
                    "NoSuchKey",
                    "NotFound",
                )
                or http_status == 404
            ):
                return False

            raise

    # ========================================================
    # Métadonnées
    # ========================================================

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
                response.get(
                    "ETag",
                    "",
                )
                .replace(
                    '"',
                    "",
                )
            ),
            "last_modified": response.get(
                "LastModified"
            ),
            "metadata": response.get(
                "Metadata",
                {},
            ),
        }

    # ========================================================
    # Upload binaire
    # ========================================================

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

        Comportement par défaut :
        create-only / immutable.

        Si overwrite=False :
        - l'écriture utilise If-None-Match='*' ;
        - R2 crée l'objet uniquement si la clé
          n'existe pas ;
        - l'opération est atomique côté stockage ;
        - une clé existante provoque FileExistsError.

        Si overwrite=True :
        - l'appel autorise explicitement le remplacement.

        overwrite=True ne doit jamais être utilisé pour
        la couche Raw.
        """

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

        put_parameters = {
            "Bucket": self.bucket_name,
            "Key": object_key,
            "Body": data,
            "ContentType": content_type,
            "Metadata": object_metadata,
        }

        # ----------------------------------------------------
        # Protection atomique create-only
        # ----------------------------------------------------

        if not overwrite:
            put_parameters[
                "IfNoneMatch"
            ] = "*"

        try:

            response = self.client.put_object(
                **put_parameters
            )

        except ClientError as exc:

            error_code = (
                exc.response
                .get("Error", {})
                .get("Code")
            )

            http_status = (
                exc.response
                .get("ResponseMetadata", {})
                .get("HTTPStatusCode")
            )

            # If-None-Match='*' échoue si l'objet
            # existe déjà.
            if (
                not overwrite
                and (
                    error_code
                    in (
                        "412",
                        "PreconditionFailed",
                    )
                    or http_status == 412
                )
            ):

                raise FileExistsError(
                    "Écriture R2 refusée : "
                    "l'objet existe déjà et "
                    "l'écrasement est interdit. "
                    f"Clé={object_key}"
                ) from exc

            raise

        return {
            "bucket": self.bucket_name,
            "object_key": object_key,
            "size_bytes": len(data),
            "sha256": sha256,
            "etag": (
                response.get(
                    "ETag",
                    "",
                )
                .replace(
                    '"',
                    "",
                )
            ),
        }

    # ========================================================
    # Upload JSON
    # ========================================================

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

        Par défaut, l'écriture est create-only.
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

    # ========================================================
    # Download binaire
    # ========================================================

    def download_bytes(
        self,
        object_key: str,
    ) -> bytes:
        """
        Télécharge un objet R2 sous forme binaire.

        Cette méthode est notamment utilisée pour
        les contrôles d'intégrité SHA-256.
        """

        response = self.client.get_object(
            Bucket=self.bucket_name,
            Key=object_key,
        )

        return response[
            "Body"
        ].read()

    # ========================================================
    # Download JSON
    # ========================================================

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
            contenu.decode(
                "utf-8"
            )
        )

    # ========================================================
    # Suppression
    # ========================================================

    def delete_object(
        self,
        object_key: str,
    ) -> None:
        """
        Supprime un objet R2.

        Cette méthode existe uniquement pour les tests
        et objets techniques.

        Elle ne doit normalement jamais être utilisée
        sur la couche Raw métier.
        """

        self.client.delete_object(
            Bucket=self.bucket_name,
            Key=object_key,
        )