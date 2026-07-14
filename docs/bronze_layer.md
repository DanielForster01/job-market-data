# Couche Bronze

## Objectif

La couche Bronze contient une version structurée des données brutes issues de l’API France Travail.

Elle ne réalise pas de nettoyage métier avancé. Son rôle est de rendre le JSON brut exploitable sous forme tabulaire tout en conservant la traçabilité.

## Source

- Source Raw : `france_travail_jobs_raw_2026-07-14_00-20-49.json`
- Fichier Bronze généré : `france_travail_jobs_bronze_2026-07-14_00-20-49.parquet`
- Nombre de lignes : 971
- Nombre de colonnes : 53

## Transformations appliquées

Les transformations appliquées sont uniquement techniques :

- aplatissement des dictionnaires simples ;
- conservation des listes et dictionnaires complexes au format JSON string ;
- ajout du fichier source dans `raw_source_file` ;
- ajout de l’enregistrement brut complet dans `raw_record`.

## Champs aplatis

- `lieuTravail` → `lieuTravail_libelle`, `lieuTravail_latitude`, `lieuTravail_longitude`, `lieuTravail_codePostal`, `lieuTravail_commune`
- `entreprise` → `entreprise_nom`, `entreprise_entrepriseAdaptee`
- `salaire` → `salaire_libelle`
- `origineOffre` → `origineOffre_origine`, `origineOffre_urlOrigine`
- `contexteTravail` → `contexteTravail_horaires`

## Champs complexes conservés

- `competences`
- `formations`
- `langues`
- `qualitesProfessionnelles`
- `contact`
- `agence`
- `permis`

Ces champs sont conservés en JSON string car leur structure peut varier et ils pourront être exploités plus tard dans des tables dédiées.

## Décisions importantes

La couche Bronze ne supprime pas les doublons.

Les doublons fonctionnels sur `id` seront analysés et traités dans la couche Silver, car ils peuvent provenir du fait qu’une même offre est récupérée via plusieurs mots-clés de recherche.

## Contrôles à réaliser

- Vérifier que le nombre de lignes Raw et Bronze est identique.
- Vérifier la présence des colonnes critiques.
- Vérifier la validité de `raw_record`.
- Mesurer les doublons sur `id`.
- Mesurer les valeurs manquantes sur les colonnes critiques.