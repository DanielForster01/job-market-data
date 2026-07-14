# Validation de la couche Bronze

## Objectif

Cette validation a pour but de vérifier que la transformation Raw vers Bronze n'a pas entraîné de perte de données et que la couche Bronze est techniquement exploitable pour les prochaines étapes du pipeline.

## Fichiers analysés

- Fichier Raw : `france_travail_jobs_raw_2026-07-14_00-20-49.json`
- Fichier Bronze : `france_travail_jobs_bronze_2026-07-14_00-20-49.parquet`

## Résultats de volumétrie

| Contrôle | Résultat |
|---|---:|
| Nombre de lignes Raw | 971 |
| Nombre de lignes Bronze | 971 |
| Écart Raw / Bronze | 0 |

La transformation n'a supprimé aucune ligne.

## Colonnes critiques

Toutes les colonnes critiques sont présentes dans la couche Bronze :

- `id`
- `intitule`
- `description`
- `dateCreation`
- `dateActualisation`
- `lieuTravail_libelle`
- `entreprise_nom`
- `typeContrat`
- `typeContratLibelle`
- `salaire_libelle`
- `search_keyword`
- `ingestion_timestamp`
- `raw_source_file`
- `raw_record`

## Doublons

| Contrôle | Résultat |
|---|---:|
| Identifiants uniques | 971 |
| Doublons sur `id` | 0 |

Aucun doublon fonctionnel n'a été détecté sur l'identifiant d'offre.

## Valeurs manquantes sur les colonnes critiques

| Colonne | Valeurs manquantes | Commentaire |
|---|---:|---|
| `id` | 0 | Identifiant fiable |
| `intitule` | 0 | Titre toujours renseigné |
| `description` | 0 | Description toujours renseignée |
| `dateCreation` | 0 | Date de création disponible |
| `dateActualisation` | 0 | Date d'actualisation disponible |
| `lieuTravail_libelle` | 0 | Localisation disponible |
| `entreprise_nom` | 503 | Nom d'entreprise souvent absent |
| `typeContrat` | 0 | Code contrat disponible |
| `typeContratLibelle` | 0 | Libellé contrat disponible |
| `salaire_libelle` | 722 | Salaire majoritairement non renseigné |
| `search_keyword` | 0 | Mot-clé d'ingestion disponible |
| `ingestion_timestamp` | 0 | Horodatage d'ingestion disponible |
| `raw_source_file` | 0 | Fichier source tracé |
| `raw_record` | 0 | Record brut conservé |

## Validation de `raw_record`

Un test de parsing JSON a été réalisé sur plusieurs lignes. Les enregistrements testés sont valides.

## Conclusion

La couche Bronze est techniquement valide. Elle conserve le même nombre de lignes que la couche Raw, dispose des colonnes critiques nécessaires, ne présente pas de doublons sur l'identifiant d'offre et conserve la traçabilité grâce à `raw_source_file` et `raw_record`.

Les valeurs manquantes sur `entreprise_nom` et `salaire_libelle` ne constituent pas une erreur de transformation. Elles reflètent les limites de complétude de la source France Travail et seront prises en compte dans la couche Silver.