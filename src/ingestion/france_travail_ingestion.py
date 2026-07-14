import json
import os
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

from src.utils.logger import logger


ROOT_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

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


def get_access_token() -> str:
    """
    Récupère un token OAuth2 pour appeler l'API France Travail.
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
        "Content-Type": "application/x-www-form-urlencoded"
    }

    response = requests.post(TOKEN_URL, data=payload, headers=headers, timeout=30)
    response.raise_for_status()

    token_data = response.json()
    return token_data["access_token"]


def search_jobs_all_pages(keyword: str, token: str) -> list[dict]:
    all_results = []
    start = 0
    page_size = 150

    while True:
        range_value = f"{start}-{start + page_size - 1}"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        params = {"motsCles": keyword, "range": range_value}

        response = requests.get(SEARCH_URL, headers=headers, params=params, timeout=30)

        if response.status_code == 204:  # aucun résultat
            break

        response.raise_for_status()
        data = response.json()
        results = data.get("resultats", [])
        all_results.extend(results)

        content_range = response.headers.get("Content-Range", "")
        if not content_range or len(results) < page_size:
            break

        start += page_size

    return all_results


def run_ingestion() -> None:
    """
    Lance l'ingestion des offres Data depuis l'API France Travail.
    """
 
    logger.info("Début de l'ingestion France Travail")
 
    keywords = [
        "data engineer",
        "data analyst",
        "business intelligence",
        "BI analyst",
        "analytics engineer",
        "python data",
        "consultant data",
    ]
 
    ingestion_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
 
    try:
        token = get_access_token()
 
        all_jobs = []
        seen_ids = set()
 
        for keyword in keywords:
            logger.info(f"Recherche des offres pour le mot-clé : {keyword}")
 
            jobs = search_jobs_all_pages(keyword=keyword, token=token)
 
            logger.info(f"{len(jobs)} offres récupérées pour : {keyword}")
 
            new_count = 0
            for job in jobs:
                job_id = job.get("id")
 
                if job_id in seen_ids:
                    continue
 
                seen_ids.add(job_id)
                job["search_keyword"] = keyword
                job["ingestion_timestamp"] = ingestion_date
                all_jobs.append(job)
                new_count += 1
 
            logger.info(f"{new_count} offres uniques ajoutées pour : {keyword}")
 
        output_file = RAW_DIR / f"france_travail_jobs_raw_{ingestion_date}.json"
 
        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(all_jobs, file, ensure_ascii=False, indent=2)
 
        logger.info(f"{len(all_jobs)} offres sauvegardées dans {output_file}")
 
        print(f"Ingestion terminée : {len(all_jobs)} offres récupérées.")
        print(f"Fichier créé : {output_file}")
 
    except requests.exceptions.HTTPError as http_error:
        logger.exception(f"Erreur HTTP lors de l'appel API : {http_error}")
        raise
 
    except requests.exceptions.RequestException as request_error:
        logger.exception(f"Erreur réseau lors de l'appel API : {request_error}")
        raise
 
    except Exception as error:
        logger.exception(f"Erreur inattendue : {error}")
        raise
 
    finally:
        logger.info("Fin de l'ingestion France Travail")
 
 
if __name__ == "__main__":
    run_ingestion()
