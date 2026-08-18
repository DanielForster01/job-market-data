import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.logger import logger


ROOT_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT_DIR / "data" / "raw"
BRONZE_DIR = ROOT_DIR / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)


# Sous-clés connues et validées pour chaque champ dict simple.
# Ce schéma a été établi en scannant l'ensemble du dataset (voir
# discover_flat_dict_fields ci-dessous). À revalider périodiquement,
# l'API pouvant faire évoluer sa structure sans préavis.
FLAT_DICT_FIELDS = {
    "lieuTravail": ["libelle", "latitude", "longitude", "codePostal", "commune"],
    "entreprise": ["nom", "description", "entrepriseAdaptee"],
    "salaire": ["libelle"],
    "origineOffre": ["origine", "urlOrigine"],
    "contexteTravail": ["horaires"],
}

# Champs à structure variable ou peu fiable -> conservés en JSON string
JSON_STRING_FIELDS = [
    "competences",
    "formations",
    "langues",
    "qualitesProfessionnelles",
    "contact",
    "agence",
    "permis",
]


def to_json_string(value: Any) -> str | None:
    """
    Convertit une valeur complexe en chaîne JSON.
    Conserve les listes et dictionnaires vides au lieu de les
    transformer en None, pour ne pas perdre l'information de
    "champ présent mais vide" vs "champ absent".
    """

    if value is None:
        return None

    return json.dumps(value, ensure_ascii=False)


def discover_flat_dict_fields(
    raw_jobs: list[dict], candidate_keys: list[str]
) -> dict[str, list[str]]:
    """
    Scanne l'ensemble des offres pour découvrir toutes les sous-clés
    réellement présentes pour chaque champ dict candidat.

    À utiliser en exploration pour valider/mettre à jour FLAT_DICT_FIELDS,
    pas dans le flux de transformation courant.
    """

    discovered = defaultdict(set)

    for job in raw_jobs:
        for key in candidate_keys:
            value = job.get(key)
            if isinstance(value, dict):
                discovered[key].update(value.keys())

    return {key: sorted(sub_keys) for key, sub_keys in discovered.items()}


def check_schema_drift(raw_jobs: list[dict]) -> None:
    """
    Compare les sous-clés réellement présentes dans le dataset avec
    celles définies dans FLAT_DICT_FIELDS, et logue un avertissement
    si des sous-clés inconnues apparaissent (schema drift).

    Ne bloque pas le pipeline : les données restent accessibles via
    raw_record même si une sous-clé n'est pas encore aplatie.
    """

    actual_fields = discover_flat_dict_fields(
        raw_jobs, candidate_keys=list(FLAT_DICT_FIELDS.keys())
    )

    for key, expected_sub_keys in FLAT_DICT_FIELDS.items():
        actual_sub_keys = set(actual_fields.get(key, []))
        expected_set = set(expected_sub_keys)

        unknown_sub_keys = actual_sub_keys - expected_set

        if unknown_sub_keys:
            logger.warning(
                f"Schema drift détecté sur '{key}' : sous-clés non "
                f"aplaties trouvées {sorted(unknown_sub_keys)}. "
                f"Elles restent accessibles via raw_record. "
                f"Pensez à mettre à jour FLAT_DICT_FIELDS."
            )


def flatten_job(job: dict, raw_source_file: str) -> dict:
    """
    Transforme une offre brute France Travail en ligne Bronze.

    La couche Bronze :
    - aplatit les dictionnaires simples et connus (FLAT_DICT_FIELDS) ;
    - conserve les structures complexes/variables en JSON string ;
    - ne nettoie pas les valeurs métier (pas de règle qualité ici) ;
    - garantit un schéma de colonnes stable ligne à ligne ;
    - conserve le record brut complet pour audit et traçabilité.
    """

    flat: dict[str, Any] = {}

    # 1. Initialiser les sous-colonnes attendues pour garantir un schéma stable
    for parent_key, sub_fields in FLAT_DICT_FIELDS.items():
        for sub_key in sub_fields:
            flat[f"{parent_key}_{sub_key}"] = None

    # 2. Initialiser les champs JSON complexes
    for field in JSON_STRING_FIELDS:
        flat[field] = None

    # 3. Parcourir les champs réellement présents dans l'offre
    for key, value in job.items():

        if key in FLAT_DICT_FIELDS:
            sub_dict = value if isinstance(value, dict) else {}

            for sub_key in FLAT_DICT_FIELDS[key]:
                sub_value = sub_dict.get(sub_key)
                # Certaines sous-clés supposées "simples" (ex: horaires)
                # sont en réalité des listes côté API. On les convertit
                # en chaîne pour garantir un type scalaire, condition
                # nécessaire pour tout chargement SQL/Parquet en aval.
                if isinstance(sub_value, list):
                    sub_value = "; ".join(
                        str(item).strip() for item in sub_value if item is not None
                    ) or None

                flat[f"{key}_{sub_key}"] = sub_value

        elif key in JSON_STRING_FIELDS:
            flat[key] = to_json_string(value)

        elif isinstance(value, (dict, list)):
            # Sécurité : champ complexe non prévu -> conservé en JSON string
            # plutôt que perdu silencieusement.
            flat[key] = to_json_string(value)

        else:
            # Champ simple : str, int, bool, float, None
            flat[key] = value

    # 4. Métadonnées techniques de traçabilité
    flat["raw_source_file"] = raw_source_file
    flat["raw_record"] = json.dumps(job, ensure_ascii=False)

    return flat


def get_latest_raw_file() -> Path:
    """
    Retourne le fichier JSON raw le plus récent du dossier data/raw,
    trié par date de modification (plus robuste qu'un tri par nom).
    """

    raw_files = sorted(
        RAW_DIR.glob("france_travail_jobs_raw_*.json"),
        key=lambda file: file.stat().st_mtime,
        reverse=True,
    )

    if not raw_files:
        raise FileNotFoundError("Aucun fichier raw trouvé dans data/raw/")

    return raw_files[0]


def run_bronze_transformation(raw_file: Path) -> Path:
    """
    Lit un fichier JSON raw et produit une table Bronze au format Parquet.
    """

    try:
        logger.info(f"Lecture du fichier brut : {raw_file}")

        with open(raw_file, "r", encoding="utf-8") as file:
            raw_jobs = json.load(file)

        if not isinstance(raw_jobs, list):
            raise ValueError("Le fichier raw doit contenir une liste d'offres.")

        if not raw_jobs:
            raise ValueError("Le fichier raw est vide.")

        logger.info(f"{len(raw_jobs)} offres à transformer")

        check_schema_drift(raw_jobs)

        flat_jobs = [
            flatten_job(job=job, raw_source_file=raw_file.name)
            for job in raw_jobs
        ]

        df = pd.DataFrame(flat_jobs)

        logger.info(
            f"DataFrame Bronze créé : {df.shape[0]} lignes, {df.shape[1]} colonnes"
        )

        output_file = BRONZE_DIR / raw_file.name.replace(
            "_raw_", "_bronze_"
        ).replace(".json", ".parquet")

        df.to_parquet(output_file, index=False)

        logger.info(f"Fichier Bronze sauvegardé : {output_file}")

        print(
            f"Transformation Bronze terminée : "
            f"{df.shape[0]} lignes, {df.shape[1]} colonnes"
        )
        print(f"Fichier créé : {output_file}")

        return output_file

    except (json.JSONDecodeError, ValueError) as data_error:
        logger.exception(f"Erreur de données dans le fichier raw : {data_error}")
        raise

    except Exception as error:
        logger.exception(f"Erreur inattendue lors de la transformation Bronze : {error}")
        raise


if __name__ == "__main__":
    latest_file = get_latest_raw_file()
    run_bronze_transformation(latest_file)