from pathlib import Path

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]

OUTPUT_FILE = (
    ROOT_DIR
    / "dbt_project"
    / "seeds"
    / "referentiel_departements.csv"
)


URL_DEPARTEMENTS = (
    "https://www.insee.fr/fr/statistiques/fichier/"
    "8740222/v_departement_2026.csv"
)

URL_REGIONS = (
    "https://www.insee.fr/fr/statistiques/fichier/"
    "8740222/v_region_2026.csv"
)


def charger_departements() -> pd.DataFrame:
    print("Téléchargement du référentiel des départements INSEE 2026...")

    df = pd.read_csv(
        URL_DEPARTEMENTS,
        dtype=str,
        encoding="utf-8",
    )

    colonnes_attendues = {
        "DEP",
        "REG",
        "LIBELLE",
    }

    colonnes_manquantes = colonnes_attendues - set(df.columns)

    if colonnes_manquantes:
        raise ValueError(
            f"Colonnes manquantes dans le fichier départements : "
            f"{sorted(colonnes_manquantes)}"
        )

    return df


def charger_regions() -> pd.DataFrame:
    print("Téléchargement du référentiel des régions INSEE 2026...")

    df = pd.read_csv(
        URL_REGIONS,
        dtype=str,
        encoding="utf-8",
    )

    colonnes_attendues = {
        "REG",
        "LIBELLE",
    }

    colonnes_manquantes = colonnes_attendues - set(df.columns)

    if colonnes_manquantes:
        raise ValueError(
            f"Colonnes manquantes dans le fichier régions : "
            f"{sorted(colonnes_manquantes)}"
        )

    return df


def construire_referentiel(
    departements: pd.DataFrame,
    regions: pd.DataFrame,
) -> pd.DataFrame:

    departements = departements[
        [
            "DEP",
            "REG",
            "LIBELLE",
        ]
    ].copy()

    departements = departements.rename(
        columns={
            "DEP": "code_departement",
            "REG": "code_region",
            "LIBELLE": "nom_departement",
        }
    )

    regions = regions[
        [
            "REG",
            "LIBELLE",
        ]
    ].copy()

    regions = regions.rename(
        columns={
            "REG": "code_region",
            "LIBELLE": "nom_region",
        }
    )

    referentiel = departements.merge(
        regions,
        on="code_region",
        how="left",
        validate="many_to_one",
    )

    referentiel = referentiel[
        [
            "code_departement",
            "nom_departement",
            "code_region",
            "nom_region",
        ]
    ]

    # Nettoyage
    for colonne in referentiel.columns:
        referentiel[colonne] = referentiel[colonne].str.strip()

    # Contrôles qualité
    if referentiel["code_departement"].isna().any():
        raise ValueError(
            "Des codes département sont manquants."
        )

    if referentiel["code_departement"].duplicated().any():
        doublons = referentiel.loc[
            referentiel["code_departement"].duplicated(
                keep=False
            ),
            "code_departement",
        ].tolist()

        raise ValueError(
            f"Codes département dupliqués : {doublons}"
        )

    if referentiel["nom_departement"].isna().any():
        raise ValueError(
            "Certains départements n'ont pas de libellé."
        )

    if referentiel["code_region"].isna().any():
        raise ValueError(
            "Certains départements n'ont pas de région."
        )

    if referentiel["nom_region"].isna().any():
        raise ValueError(
            "Certains départements n'ont pas de nom de région."
        )

    return referentiel.sort_values(
        by="code_departement"
    ).reset_index(drop=True)


def sauvegarder_referentiel(
    referentiel: pd.DataFrame,
) -> None:

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    referentiel.to_csv(
        OUTPUT_FILE,
        index=False,
        encoding="utf-8",
    )

    print()
    print("Référentiel géographique créé avec succès.")
    print(f"Fichier : {OUTPUT_FILE}")
    print(f"Nombre de départements : {len(referentiel)}")


def main() -> None:

    departements = charger_departements()
    regions = charger_regions()

    referentiel = construire_referentiel(
        departements,
        regions,
    )

    sauvegarder_referentiel(
        referentiel,
    )

    print()
    print("Aperçu :")
    print(referentiel.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
