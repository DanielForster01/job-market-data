# Rapport qualité Bronze

## Fichier analysé

`france_travail_jobs_bronze_2026-07-14_00-20-49.parquet`

## Résumé

| Indicateur | Valeur |
|---|---:|
| Nombre de lignes | 971 |
| Nombre de colonnes | 53 |
| Identifiants uniques | 971 |
| Doublons sur `id` | 0 |
| Offres avec salaire renseigné | 249 |
| Offres sans salaire renseigné | 722 |
| Taux de salaire renseigné | 25.64 % |
| Entreprises renseignées | 468 |
| Entreprises non renseignées | 503 |
| Taux entreprise renseignée | 48.2 % |

## Complétude des colonnes critiques

| Colonne | Disponible | Manquant | Complétude |
|---|---:|---:|---:|
| `id` | 971 | 0 | 100.0 % |
| `intitule` | 971 | 0 | 100.0 % |
| `description` | 971 | 0 | 100.0 % |
| `dateCreation` | 971 | 0 | 100.0 % |
| `dateActualisation` | 971 | 0 | 100.0 % |
| `lieuTravail_libelle` | 971 | 0 | 100.0 % |
| `entreprise_nom` | 468 | 503 | 48.2 % |
| `typeContrat` | 971 | 0 | 100.0 % |
| `typeContratLibelle` | 971 | 0 | 100.0 % |
| `salaire_libelle` | 249 | 722 | 25.64 % |
| `search_keyword` | 971 | 0 | 100.0 % |
| `ingestion_timestamp` | 971 | 0 | 100.0 % |
| `raw_source_file` | 971 | 0 | 100.0 % |
| `raw_record` | 971 | 0 | 100.0 % |


## Validité des dates

| Colonne | Dates valides | Dates invalides | Valeurs manquantes | Taux de validité |
|---|---:|---:|---:|---:|
| `dateCreation` | 971 | 0 | 0 | 100.0 % |
| `dateActualisation` | 971 | 0 | 0 | 100.0 % |
| `ingestion_timestamp` | 0 | 971 | 0 | 0.0 % |


## Validité des champs JSON

| Colonne | JSON valides | JSON invalides | Valeurs manquantes | Taux de validité |
|---|---:|---:|---:|---:|
| `competences` | 132 | 0 | 839 | 100.0 % |
| `formations` | 61 | 0 | 910 | 100.0 % |
| `langues` | 52 | 0 | 919 | 100.0 % |
| `qualitesProfessionnelles` | 84 | 0 | 887 | 100.0 % |
| `contact` | 631 | 0 | 340 | 100.0 % |
| `agence` | 158 | 0 | 813 | 100.0 % |
| `permis` | 5 | 0 | 966 | 100.0 % |
| `raw_record` | 971 | 0 | 0 | 100.0 % |


## Observations importantes

- La couche Bronze conserve le même niveau de granularité que la source : une ligne correspond à une offre récupérée.
- Les champs `entreprise_nom` et `salaire_libelle` présentent une forte proportion de valeurs manquantes.
- Les salaires manquants ne sont pas une erreur de pipeline : ils reflètent une limite de complétude de la source.
- Les champs complexes comme `competences`, `formations`, `langues` et `permis` sont conservés au format JSON string.
- Les règles de nettoyage seront appliquées uniquement dans la couche Silver.

## Décisions pour la couche Silver

- Conserver une ligne par offre unique.
- Typer les dates.
- Standardiser les localisations.
- Créer un champ `company_name_clean`.
- Créer un indicateur `has_salary`.
- Extraire les bornes de salaire lorsque le format le permet.
- Normaliser les types de contrat.
- Préparer des tables analytiques pour la couche Gold.
