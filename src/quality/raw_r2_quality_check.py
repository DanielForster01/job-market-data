import argparse
import hashlib
import json
import re
from typing import Any

from src.storage.r2_storage import R2Storage


# ============================================================
# Référentiel des requêtes d'acquisition autorisées
# ============================================================
#
# Ces valeurs doivent rester cohérentes avec celles utilisées
# dans france_travail_ingestion.py.
#
# Elles représentent des requêtes d'acquisition, PAS des familles
# métier.
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


def extract_batch_id_from_key(
    object_key: str,
) -> str | None:
    """
    Extrait le batch_id depuis une clé R2 du type :

    raw/france_travail/
    ingestion_date=YYYY-MM-DD/
    batch_id=<uuid>/
    offres.json
    """

    match = re.search(
        r"batch_id=([^/]+)",
        object_key,
    )

    if match:
        return match.group(1)

    return None


def validate_raw_object(
    object_key: str,
) -> dict[str, Any]:
    """
    Valide un batch Raw France Travail stocké dans Cloudflare R2.

    Contrôles réalisés :
    - existence de l'objet ;
    - cohérence de la taille ;
    - intégrité SHA-256 ;
    - validité JSON ;
    - cohérence du volume ;
    - cohérence du batch_id ;
    - absence d'ID manquant ;
    - unicité des IDs ;
    - présence de search_keyword ;
    - présence et validité de search_keywords ;
    - cohérence search_keyword / search_keywords ;
    - validité des requêtes d'acquisition ;
    - présence de ingestion_timestamp ;
    - cohérence des métadonnées techniques R2.
    """

    storage = R2Storage()

    print()
    print("=" * 70)
    print("CONTRÔLE QUALITÉ RAW R2")
    print("=" * 70)
    print()

    # ========================================================
    # 1. Existence de l'objet
    # ========================================================

    if not storage.object_exists(
        object_key
    ):
        raise FileNotFoundError(
            f"Objet R2 introuvable : "
            f"{object_key}"
        )

    print(
        "[OK] Objet présent dans R2"
    )

    # ========================================================
    # 2. Métadonnées R2
    # ========================================================

    object_info = storage.get_object_info(
        object_key
    )

    metadata = object_info[
        "metadata"
    ]

    print(
        "[OK] Métadonnées R2 accessibles"
    )

    # ========================================================
    # 3. Téléchargement binaire
    # ========================================================

    raw_bytes = storage.download_bytes(
        object_key
    )

    taille_reelle = len(
        raw_bytes
    )

    taille_r2 = object_info[
        "size_bytes"
    ]

    if taille_reelle != taille_r2:
        raise RuntimeError(
            "La taille téléchargée ne correspond "
            "pas à la taille déclarée par R2."
        )

    print(
        f"[OK] Taille objet : "
        f"{taille_reelle} octets"
    )

    # ========================================================
    # 4. Contrôle SHA-256
    # ========================================================

    sha256_calcule = hashlib.sha256(
        raw_bytes
    ).hexdigest()

    sha256_metadata = metadata.get(
        "sha256"
    )

    if not sha256_metadata:
        raise RuntimeError(
            "SHA-256 absent des métadonnées R2."
        )

    if (
        sha256_calcule
        != sha256_metadata
    ):
        raise RuntimeError(
            "Échec du contrôle d'intégrité SHA-256."
        )

    print(
        "[OK] Intégrité SHA-256 validée"
    )

    # ========================================================
    # 5. Parsing JSON
    # ========================================================

    try:

        payload = json.loads(
            raw_bytes.decode(
                "utf-8"
            )
        )

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:

        raise RuntimeError(
            "Le fichier Raw n'est pas "
            "un JSON UTF-8 valide."
        ) from exc

    if not isinstance(
        payload,
        list,
    ):
        raise RuntimeError(
            "La racine du fichier Raw doit "
            "être une liste d'offres."
        )

    print(
        "[OK] JSON valide"
    )

    # ========================================================
    # 6. Contrôle du volume
    # ========================================================

    nombre_offres = len(
        payload
    )

    record_count_metadata = (
        metadata.get(
            "record-count"
        )
    )

    if record_count_metadata is None:
        raise RuntimeError(
            "record-count absent des "
            "métadonnées R2."
        )

    try:

        record_count_metadata_int = int(
            record_count_metadata
        )

    except ValueError as exc:

        raise RuntimeError(
            "record-count n'est pas "
            "un entier valide."
        ) from exc

    if (
        record_count_metadata_int
        != nombre_offres
    ):
        raise RuntimeError(
            "Le nombre d'offres du JSON "
            "ne correspond pas au record-count "
            "stocké dans R2."
        )

    print(
        "[OK] Volume cohérent : "
        f"{nombre_offres} offres"
    )

    # ========================================================
    # 7. Contrôle du Batch ID
    # ========================================================

    batch_id_path = (
        extract_batch_id_from_key(
            object_key
        )
    )

    batch_id_metadata = metadata.get(
        "batch-id"
    )

    if not batch_id_path:
        raise RuntimeError(
            "Impossible d'extraire le batch_id "
            "depuis la clé R2."
        )

    if not batch_id_metadata:
        raise RuntimeError(
            "batch-id absent des "
            "métadonnées R2."
        )

    if (
        batch_id_path
        != batch_id_metadata
    ):
        raise RuntimeError(
            "Le batch_id du chemin R2 "
            "ne correspond pas au batch-id "
            "des métadonnées."
        )

    print(
        "[OK] Batch ID cohérent : "
        f"{batch_id_path}"
    )

    # ========================================================
    # 8. Contrôle du nombre de requêtes d'acquisition
    # ========================================================

    search_query_count_metadata = (
        metadata.get(
            "search-query-count"
        )
    )

    if search_query_count_metadata is None:
        raise RuntimeError(
            "search-query-count absent "
            "des métadonnées R2."
        )

    try:

        search_query_count_metadata_int = int(
            search_query_count_metadata
        )

    except ValueError as exc:

        raise RuntimeError(
            "search-query-count n'est pas "
            "un entier valide."
        ) from exc

    if (
        search_query_count_metadata_int
        != len(SEARCH_QUERIES)
    ):
        raise RuntimeError(
            "Le nombre de requêtes déclaré "
            "dans R2 ne correspond pas au "
            "référentiel SEARCH_QUERIES."
        )

    print(
        "[OK] Nombre de requêtes "
        f"d'acquisition cohérent : "
        f"{len(SEARCH_QUERIES)}"
    )

    # ========================================================
    # 9. Contrôle de la structure des offres
    # ========================================================

    ids = []

    offres_sans_id = 0

    sans_search_keyword = 0

    sans_search_keywords = 0

    search_keywords_non_liste = 0

    search_keywords_vides = 0

    search_keyword_incoherent = 0

    search_keywords_inconnus = 0

    sans_ingestion_timestamp = 0

    entrees_non_dict = 0

    valeurs_inconnues = set()

    for offre in payload:

        # ----------------------------------------------------
        # Type de l'entrée
        # ----------------------------------------------------

        if not isinstance(
            offre,
            dict,
        ):

            entrees_non_dict += 1

            continue

        # ----------------------------------------------------
        # ID
        # ----------------------------------------------------

        job_id = offre.get(
            "id"
        )

        if not job_id:

            offres_sans_id += 1

        else:

            ids.append(
                job_id
            )

        # ----------------------------------------------------
        # search_keyword
        # ----------------------------------------------------

        search_keyword = offre.get(
            "search_keyword"
        )

        if not search_keyword:

            sans_search_keyword += 1

        # ----------------------------------------------------
        # search_keywords
        # ----------------------------------------------------

        search_keywords = offre.get(
            "search_keywords"
        )

        if search_keywords is None:

            sans_search_keywords += 1

            continue

        if not isinstance(
            search_keywords,
            list,
        ):

            search_keywords_non_liste += 1

            continue

        if not search_keywords:

            search_keywords_vides += 1

            continue

        # ----------------------------------------------------
        # Cohérence search_keyword / search_keywords
        # ----------------------------------------------------

        if (
            search_keyword
            and search_keyword
            not in search_keywords
        ):

            search_keyword_incoherent += 1

        # ----------------------------------------------------
        # Valeurs autorisées dans search_keywords
        # ----------------------------------------------------

        keywords_inconnus_offre = [
            query
            for query in search_keywords
            if query not in SEARCH_QUERIES
        ]

        if keywords_inconnus_offre:

            search_keywords_inconnus += 1

            valeurs_inconnues.update(
                keywords_inconnus_offre
            )

        # ----------------------------------------------------
        # ingestion_timestamp
        # ----------------------------------------------------

        if not offre.get(
            "ingestion_timestamp"
        ):

            sans_ingestion_timestamp += 1

    # ========================================================
    # 10. Contrôle structure JSON
    # ========================================================

    if entrees_non_dict > 0:
        raise RuntimeError(
            f"{entrees_non_dict} entrée(s) Raw "
            "ne sont pas des objets JSON."
        )

    print(
        "[OK] Toutes les entrées Raw "
        "sont des objets JSON"
    )

    # ========================================================
    # 11. Contrôle des IDs
    # ========================================================

    nombre_ids_uniques = len(
        set(ids)
    )

    nombre_doublons = (
        len(ids)
        - nombre_ids_uniques
    )

    if offres_sans_id != 0:
        raise RuntimeError(
            f"{offres_sans_id} offres sans ID "
            "trouvées dans le Raw."
        )

    if nombre_doublons != 0:
        raise RuntimeError(
            f"{nombre_doublons} doublons d'ID "
            "trouvés dans le Raw."
        )

    print(
        "[OK] Aucun ID manquant"
    )

    print(
        "[OK] Aucun doublon d'ID"
    )

    # ========================================================
    # 12. Contrôle search_keyword
    # ========================================================

    if sans_search_keyword > 0:
        raise RuntimeError(
            f"{sans_search_keyword} offres "
            "sans search_keyword."
        )

    print(
        "[OK] search_keyword présent "
        "sur toutes les offres"
    )

    # ========================================================
    # 13. Contrôle search_keywords
    # ========================================================

    if sans_search_keywords > 0:
        raise RuntimeError(
            f"{sans_search_keywords} offres "
            "sans search_keywords."
        )

    if search_keywords_non_liste > 0:
        raise RuntimeError(
            f"{search_keywords_non_liste} offres "
            "avec search_keywords qui n'est "
            "pas une liste."
        )

    if search_keywords_vides > 0:
        raise RuntimeError(
            f"{search_keywords_vides} offres "
            "avec search_keywords vide."
        )

    print(
        "[OK] search_keywords présent "
        "sur toutes les offres"
    )

    print(
        "[OK] search_keywords est une liste "
        "non vide pour toutes les offres"
    )

    # ========================================================
    # 14. Cohérence search_keyword / search_keywords
    # ========================================================

    if search_keyword_incoherent > 0:
        raise RuntimeError(
            f"{search_keyword_incoherent} offres "
            "ont un search_keyword absent "
            "de search_keywords."
        )

    print(
        "[OK] search_keyword appartient "
        "à search_keywords pour toutes les offres"
    )

    # ========================================================
    # 15. Contrôle des valeurs de search_keywords
    # ========================================================

    if search_keywords_inconnus > 0:

        valeurs_triees = sorted(
            valeurs_inconnues
        )

        raise RuntimeError(
            f"{search_keywords_inconnus} offres "
            "contiennent des requêtes inconnues "
            f"dans search_keywords : "
            f"{valeurs_triees}"
        )

    print(
        "[OK] Toutes les valeurs de "
        "search_keywords sont autorisées"
    )

    # ========================================================
    # 16. Contrôle ingestion_timestamp
    # ========================================================

    if (
        sans_ingestion_timestamp
        > 0
    ):
        raise RuntimeError(
            f"{sans_ingestion_timestamp} offres "
            "sans ingestion_timestamp."
        )

    print(
        "[OK] ingestion_timestamp présent "
        "sur toutes les offres"
    )

    # ========================================================
    # 17. Contrôle unicité du timestamp d'ingestion
    # ========================================================

    ingestion_timestamps = {
        offre.get(
            "ingestion_timestamp"
        )
        for offre in payload
        if isinstance(
            offre,
            dict,
        )
        and offre.get(
            "ingestion_timestamp"
        )
    }

    if len(
        ingestion_timestamps
    ) != 1:
        raise RuntimeError(
            "Le batch Raw contient plusieurs "
            "ingestion_timestamp différents : "
            f"{ingestion_timestamps}"
        )

    ingestion_timestamp_unique = next(
        iter(
            ingestion_timestamps
        )
    )

    print(
        "[OK] ingestion_timestamp unique "
        "pour tout le batch : "
        f"{ingestion_timestamp_unique}"
    )

    # ========================================================
    # 18. ignored_without_id
    # ========================================================

    ignored_without_id_metadata = (
        metadata.get(
            "ignored-without-id",
            "0",
        )
    )

    try:

        ignored_without_id = int(
            ignored_without_id_metadata
        )

    except ValueError as exc:

        raise RuntimeError(
            "ignored-without-id n'est pas "
            "un entier valide."
        ) from exc

    if ignored_without_id < 0:
        raise RuntimeError(
            "ignored-without-id ne peut pas "
            "être négatif."
        )

    # ========================================================
    # 19. Analyse de couverture des requêtes
    # ========================================================

    couverture_requetes = {}

    for query in SEARCH_QUERIES:

        nombre_couvertes = sum(
            1
            for offre in payload
            if query
            in offre.get(
                "search_keywords",
                [],
            )
        )

        nombre_exclusives = sum(
            1
            for offre in payload
            if offre.get(
                "search_keywords"
            )
            == [query]
        )

        couverture_requetes[
            query
        ] = {
            "couvertes": (
                nombre_couvertes
            ),
            "exclusives": (
                nombre_exclusives
            ),
        }

    # ========================================================
    # 20. Résumé
    # ========================================================

    print()
    print("-" * 70)
    print("RÉSUMÉ")
    print("-" * 70)

    print(
        f"Batch ID             : "
        f"{batch_id_path}"
    )

    print(
        f"Nombre offres        : "
        f"{nombre_offres}"
    )

    print(
        f"IDs uniques          : "
        f"{nombre_ids_uniques}"
    )

    print(
        f"Offres sans ID Raw   : "
        f"{offres_sans_id}"
    )

    print(
        f"Ignorées avant Raw   : "
        f"{ignored_without_id}"
    )

    print(
        f"Requêtes acquisition : "
        f"{len(SEARCH_QUERIES)}"
    )

    print(
        f"Timestamp ingestion  : "
        f"{ingestion_timestamp_unique}"
    )

    print(
        f"Taille               : "
        f"{taille_reelle} octets"
    )

    print(
        f"SHA-256              : "
        f"{sha256_calcule}"
    )

    print()
    print(
        "COUVERTURE DES REQUÊTES"
    )
    print("-" * 70)

    for query in SEARCH_QUERIES:

        stats = (
            couverture_requetes[
                query
            ]
        )

        print(
            f"{query:<25} "
            f"couvertes={stats['couvertes']:<5} "
            f"exclusives={stats['exclusives']}"
        )

    print()
    print(
        "STATUT RAW : VALIDE"
    )
    print()

    # ========================================================
    # 21. Résultat retourné
    # ========================================================

    return {
        "statut": "VALIDE",
        "batch_id": batch_id_path,
        "nombre_offres": (
            nombre_offres
        ),
        "nombre_ids_uniques": (
            nombre_ids_uniques
        ),
        "offres_sans_id_raw": (
            offres_sans_id
        ),
        "ignored_without_id": (
            ignored_without_id
        ),
        "nombre_doublons": (
            nombre_doublons
        ),
        "nombre_requetes": len(
            SEARCH_QUERIES
        ),
        "ingestion_timestamp": (
            ingestion_timestamp_unique
        ),
        "couverture_requetes": (
            couverture_requetes
        ),
        "size_bytes": (
            taille_reelle
        ),
        "sha256": (
            sha256_calcule
        ),
    }


def main() -> None:
    """
    Point d'entrée du contrôle qualité Raw R2.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Contrôle qualité d'un batch Raw "
            "France Travail stocké dans R2."
        )
    )

    parser.add_argument(
        "--object-key",
        required=True,
        help=(
            "Clé de l'objet Raw dans "
            "le bucket R2."
        ),
    )

    args = parser.parse_args()

    validate_raw_object(
        object_key=args.object_key
    )


if __name__ == "__main__":
    main()