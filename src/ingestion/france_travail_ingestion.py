import os
from datetime import datetime, timezone
from uuid import uuid4

import requests
from dotenv import load_dotenv

from src.storage.r2_paths import build_raw_object_key
from src.storage.r2_storage import R2Storage
from src.utils.logger import logger


# ============================================================
# Configuration
# ============================================================

load_dotenv()

CLIENT_ID = os.getenv("FRANCE_TRAVAIL_CLIENT_ID")
CLIENT_SECRET = os.getenv("FRANCE_TRAVAIL_CLIENT_SECRET")


TOKEN_URL = (
    "https://entreprise.francetravail.fr/connexion/oauth2/access_token"
    "?realm=/partenaire"
)

SEARCH_URL = (
    "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
)


# ============================================================
# Requêtes d'acquisition
# ============================================================
#
# IMPORTANT :
#
# Ces valeurs ne constituent PAS une nomenclature de métiers.
#
# Ce sont uniquement des requêtes envoyées à l'API France Travail
# pour maximiser la couverture des offres Data.
#
# La classification métier officielle du projet est réalisée plus
# tard dans la couche Intermediate dbt avec famille_metier_data.
#
# "python data" est donc conservé comme requête de couverture
# technique tant qu'elle permet de récupérer au moins une offre
# pertinente qui ne serait pas captée par les autres requêtes.
#
# ============================================================

SEARCH_QUERIES = [
    "data engineer",
    "data analyst",
    "data scientist",
    "business intelligence",
    "BI analyst",
    "analytics engineer",
    "python data",
    "consultant data",
]


def get_access_token() -> str:
    """
    Récupère un token OAuth2 permettant d'appeler
    l'API France Travail.
    """

    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError(
            "Les variables FRANCE_TRAVAIL_CLIENT_ID et "
            "FRANCE_TRAVAIL_CLIENT_SECRET doivent être définies dans .env"
        )

    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "api_offresdemploiv2 o2dsoffre",
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.post(
        TOKEN_URL,
        data=payload,
        headers=headers,
        timeout=30,
    )

    response.raise_for_status()

    token_data = response.json()

    return token_data["access_token"]


def search_jobs_all_pages(
    keyword: str,
    token: str,
) -> list[dict]:
    """
    Récupère toutes les pages d'offres France Travail
    correspondant à une requête de recherche.
    """

    all_results = []

    start = 0
    page_size = 150

    while True:

        range_value = (
            f"{start}-{start + page_size - 1}"
        )

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

        params = {
            "motsCles": keyword,
            "range": range_value,
        }

        response = requests.get(
            SEARCH_URL,
            headers=headers,
            params=params,
            timeout=30,
        )

        # Aucun résultat supplémentaire
        if response.status_code == 204:
            break

        response.raise_for_status()

        data = response.json()

        results = data.get(
            "resultats",
            [],
        )

        all_results.extend(
            results
        )

        content_range = response.headers.get(
            "Content-Range",
            "",
        )

        if (
            not content_range
            or len(results) < page_size
        ):
            break

        start += page_size

    return all_results


def run_ingestion() -> dict:
    """
    Lance l'ingestion des offres Data depuis l'API France Travail.

    Objectifs :
    - maximiser la couverture des offres ;
    - éviter les doublons entre les différentes requêtes ;
    - conserver la traçabilité de toutes les requêtes ayant
      permis de retrouver une offre ;
    - ne perdre aucune offre disposant d'un identifiant ;
    - stocker le batch Raw dans Cloudflare R2 ;
    - ne conserver aucun fichier Raw permanent sur le disque local.

    search_keyword :
        première requête ayant permis de découvrir l'offre.

    search_keywords :
        ensemble des requêtes ayant permis de retrouver l'offre.

    La classification métier réelle est réalisée plus tard dans dbt.
    """

    logger.info(
        "Début de l'ingestion France Travail"
    )

    # ========================================================
    # 1. Identité technique du batch
    # ========================================================

    batch_id = uuid4()

    ingestion_datetime = datetime.now(
        timezone.utc
    )

    # Format conservé temporairement pour rester compatible
    # avec les transformations Bronze / Silver existantes.
    ingestion_timestamp = (
        ingestion_datetime.strftime(
            "%Y-%m-%d_%H-%M-%S"
        )
    )

    logger.info(
        f"Batch ID : {batch_id}"
    )

    logger.info(
        "Horodatage ingestion UTC : "
        f"{ingestion_datetime.isoformat()}"
    )

    try:

        # ====================================================
        # 2. Authentification
        # ====================================================

        token = get_access_token()

        # ====================================================
        # 3. Structures de collecte
        # ====================================================
        #
        # La clé est l'identifiant France Travail.
        #
        # Cette structure garantit qu'une offre ne sera stockée
        # qu'une seule fois dans le Raw, même si elle est
        # retrouvée via plusieurs requêtes.
        #
        # ====================================================

        offres_par_id: dict[str, dict] = {}

        ignored_without_id = 0

        # Statistiques détaillées par requête.
        statistiques_requetes: dict[
            str,
            dict[str, int],
        ] = {}

        # ====================================================
        # 4. Exécution des requêtes d'acquisition
        # ====================================================

        for keyword in SEARCH_QUERIES:

            logger.info(
                "Recherche des offres pour "
                f"la requête : {keyword}"
            )

            jobs = search_jobs_all_pages(
                keyword=keyword,
                token=token,
            )

            logger.info(
                f"{len(jobs)} résultats récupérés "
                f"pour : {keyword}"
            )

            nouvelles = 0
            deja_couvertes = 0
            doublons_dans_requete = 0
            sans_id_requete = 0

            # Évite de compter deux fois un même ID si l'API
            # le renvoie plusieurs fois pour une seule requête.
            ids_vus_dans_requete = set()

            for job in jobs:

                job_id = job.get(
                    "id"
                )

                # --------------------------------------------
                # Offre sans identifiant
                # --------------------------------------------

                if not job_id:

                    ignored_without_id += 1
                    sans_id_requete += 1

                    logger.warning(
                        "Offre sans identifiant ignorée "
                        f"pour la requête : {keyword}"
                    )

                    continue

                # --------------------------------------------
                # Doublon à l'intérieur d'une même requête
                # --------------------------------------------

                if job_id in ids_vus_dans_requete:

                    doublons_dans_requete += 1

                    continue

                ids_vus_dans_requete.add(
                    job_id
                )

                # --------------------------------------------
                # Première apparition de cette offre
                # dans le batch complet
                # --------------------------------------------

                if job_id not in offres_par_id:

                    job_enrichi = dict(
                        job
                    )

                    # Première requête ayant découvert l'offre.
                    # Conservée pour compatibilité avec les
                    # transformations existantes.
                    job_enrichi[
                        "search_keyword"
                    ] = keyword

                    # Liste de toutes les requêtes ayant permis
                    # de retrouver cette offre.
                    job_enrichi[
                        "search_keywords"
                    ] = [
                        keyword
                    ]

                    # Métadonnée technique d'ingestion.
                    job_enrichi[
                        "ingestion_timestamp"
                    ] = ingestion_timestamp

                    offres_par_id[
                        job_id
                    ] = job_enrichi

                    nouvelles += 1

                # --------------------------------------------
                # Offre déjà récupérée via une autre requête
                # --------------------------------------------

                else:

                    search_keywords = (
                        offres_par_id[
                            job_id
                        ][
                            "search_keywords"
                        ]
                    )

                    if (
                        keyword
                        not in search_keywords
                    ):

                        search_keywords.append(
                            keyword
                        )

                    deja_couvertes += 1

            # --------------------------------------------
            # Statistiques de la requête
            # --------------------------------------------

            statistiques_requetes[
                keyword
            ] = {
                "resultats_api": len(
                    jobs
                ),
                "nouvelles": nouvelles,
                "deja_couvertes": (
                    deja_couvertes
                ),
                "doublons_dans_requete": (
                    doublons_dans_requete
                ),
                "sans_id": sans_id_requete,
            }

            logger.info(
                f"{keyword} : "
                f"{nouvelles} nouvelles, "
                f"{deja_couvertes} déjà couvertes, "
                f"{doublons_dans_requete} doublons internes, "
                f"{sans_id_requete} sans ID"
            )

        # ====================================================
        # 5. Construction du dataset Raw final
        # ====================================================

        all_jobs = list(
            offres_par_id.values()
        )

        if not all_jobs:

            raise RuntimeError(
                "Aucune offre récupérée. "
                "Le batch Raw ne sera pas créé."
            )

        # ====================================================
        # 6. Analyse de contribution des requêtes
        # ====================================================

        logger.info(
            "Contribution des requêtes d'acquisition"
        )

        contribution_requetes = {}

        for query in SEARCH_QUERIES:

            nombre_couvertes = sum(
                1
                for offre in all_jobs
                if query
                in offre.get(
                    "search_keywords",
                    [],
                )
            )

            nombre_exclusives = sum(
                1
                for offre in all_jobs
                if offre.get(
                    "search_keywords"
                )
                == [query]
            )

            contribution_requetes[
                query
            ] = {
                "nombre_couvertes": (
                    nombre_couvertes
                ),
                "nombre_exclusives": (
                    nombre_exclusives
                ),
            }

            logger.info(
                f"{query} : "
                f"{nombre_couvertes} offres couvertes, "
                f"{nombre_exclusives} offres exclusives"
            )

        # ====================================================
        # 7. Contrôles avant stockage
        # ====================================================

        logger.info(
            "Nombre total d'offres uniques : "
            f"{len(all_jobs)}"
        )

        logger.info(
            "Nombre total d'offres sans identifiant ignorées : "
            f"{ignored_without_id}"
        )

        # Double sécurité :
        # toutes les offres conservées doivent posséder un ID.
        offres_sans_id_final = [
            offre
            for offre in all_jobs
            if not offre.get("id")
        ]

        if offres_sans_id_final:

            raise RuntimeError(
                "Des offres sans identifiant sont présentes "
                "dans le dataset Raw final."
            )

        # Contrôle d'unicité.
        ids_finaux = [
            offre["id"]
            for offre in all_jobs
        ]

        if len(ids_finaux) != len(
            set(ids_finaux)
        ):

            raise RuntimeError(
                "Des doublons d'identifiants subsistent "
                "dans le dataset Raw final."
            )

        # Contrôle search_keywords.
        offres_search_keywords_invalides = [
            offre
            for offre in all_jobs
            if (
                not isinstance(
                    offre.get(
                        "search_keywords"
                    ),
                    list,
                )
                or not offre.get(
                    "search_keywords"
                )
            )
        ]

        if (
            offres_search_keywords_invalides
        ):

            raise RuntimeError(
                "Certaines offres ne possèdent pas "
                "de search_keywords valide."
            )

        # search_keyword doit forcément appartenir
        # à search_keywords.
        offres_search_keyword_incoherentes = [
            offre
            for offre in all_jobs
            if offre.get(
                "search_keyword"
            )
            not in offre.get(
                "search_keywords",
                [],
            )
        ]

        if (
            offres_search_keyword_incoherentes
        ):

            raise RuntimeError(
                "Certaines offres possèdent un "
                "search_keyword incohérent avec "
                "search_keywords."
            )

        logger.info(
            "Contrôles pré-stockage validés"
        )

        # ====================================================
        # 8. Construction de la clé Raw R2
        # ====================================================

        object_key = build_raw_object_key(
            source="france_travail",
            batch_id=batch_id,
            ingestion_datetime=(
                ingestion_datetime
            ),
            filename="offres.json",
        )

        # ====================================================
        # 9. Initialisation du stockage
        # ====================================================

        storage = R2Storage()

        # ====================================================
        # 10. Upload du Raw dans R2
        # ====================================================

        resultat_upload = (
            storage.upload_json(
                object_key=object_key,
                payload=all_jobs,
                metadata={
                    "layer": "raw",
                    "source": (
                        "france_travail"
                    ),
                    "batch-id": str(
                        batch_id
                    ),
                    "ingestion-date-utc": (
                        ingestion_datetime
                        .date()
                        .isoformat()
                    ),
                    "record-count": str(
                        len(all_jobs)
                    ),
                    "ignored-without-id": str(
                        ignored_without_id
                    ),
                    "search-query-count": str(
                        len(
                            SEARCH_QUERIES
                        )
                    ),
                },
                overwrite=False,
            )
        )

        # ====================================================
        # 11. Validation post-upload
        # ====================================================

        if not storage.object_exists(
            object_key
        ):

            raise RuntimeError(
                "Le fichier Raw n'est pas "
                "accessible après son upload R2."
            )

        logger.info(
            "Batch Raw sauvegardé dans R2 : "
            f"{object_key}"
        )

        logger.info(
            "SHA-256 du fichier Raw : "
            f"{resultat_upload['sha256']}"
        )

        # ====================================================
        # 12. Résumé terminal
        # ====================================================

        print()

        print(
            "=" * 70
        )

        print(
            "INGESTION FRANCE TRAVAIL TERMINÉE"
        )

        print(
            "=" * 70
        )

        print()

        print(
            f"Batch ID       : {batch_id}"
        )

        print(
            f"Nombre offres  : {len(all_jobs)}"
        )

        print(
            f"Offres sans ID : {ignored_without_id}"
        )

        print(
            f"Requêtes       : {len(SEARCH_QUERIES)}"
        )

        print(
            f"Bucket R2      : "
            f"{resultat_upload['bucket']}"
        )

        print(
            f"Objet Raw      : {object_key}"
        )

        print(
            f"Taille         : "
            f"{resultat_upload['size_bytes']} octets"
        )

        print(
            f"SHA-256        : "
            f"{resultat_upload['sha256']}"
        )

        print()

        print(
            "CONTRIBUTION DES REQUÊTES"
        )

        print(
            "-" * 70
        )

        for query in SEARCH_QUERIES:

            stats = (
                contribution_requetes[
                    query
                ]
            )

            print(
                f"{query:<25} "
                f"couvertes={stats['nombre_couvertes']:<5} "
                f"exclusives={stats['nombre_exclusives']}"
            )

        print()

        # ====================================================
        # 13. Métadonnées retournées au pipeline
        # ====================================================

        return {
            "batch_id": str(
                batch_id
            ),
            "ingestion_datetime_utc": (
                ingestion_datetime
                .isoformat()
            ),
            "nombre_offres": len(
                all_jobs
            ),
            "ignored_without_id": (
                ignored_without_id
            ),
            "nombre_requetes": len(
                SEARCH_QUERIES
            ),
            "statistiques_requetes": (
                statistiques_requetes
            ),
            "contribution_requetes": (
                contribution_requetes
            ),
            "bucket": (
                resultat_upload[
                    "bucket"
                ]
            ),
            "object_key": object_key,
            "size_bytes": (
                resultat_upload[
                    "size_bytes"
                ]
            ),
            "sha256": (
                resultat_upload[
                    "sha256"
                ]
            ),
        }

    # ========================================================
    # Gestion des erreurs HTTP
    # ========================================================

    except requests.exceptions.HTTPError as http_error:

        logger.exception(
            "Erreur HTTP lors de l'appel API : "
            f"{http_error}"
        )

        raise

    # ========================================================
    # Gestion des erreurs réseau
    # ========================================================

    except requests.exceptions.RequestException as request_error:

        logger.exception(
            "Erreur réseau lors de l'appel API : "
            f"{request_error}"
        )

        raise

    # ========================================================
    # Gestion des autres erreurs
    # ========================================================

    except Exception as error:

        logger.exception(
            f"Erreur inattendue : {error}"
        )

        raise

    finally:

        logger.info(
            "Fin de l'ingestion France Travail"
        )


if __name__ == "__main__":
    run_ingestion()