# Chargement Silver vers PostgreSQL

## Objectif

Charger la couche Silver validée dans PostgreSQL afin de disposer d'une table source fiable pour les futurs modèles dbt.

## Source

- Format : Parquet
- Dossier : `data/silver/`
- Fichier utilisé : dernier fichier `france_travail_jobs_silver_*.parquet`

## Cible PostgreSQL

- Base : `job_market`
- Schéma : `silver`
- Table : `france_travail_offres`

## Stratégie de chargement

La première version utilise une stratégie full refresh :

- remplacement complet de la table ;
- chargement du dernier fichier Silver ;
- ajout d'une clé primaire sur `id_offre` ;
- création d'index sur quelques colonnes analytiques ;
- enregistrement du chargement dans `audit.pipeline_runs`.

## Contrôles réalisés avant chargement

- fichier Silver non vide ;
- colonnes obligatoires présentes ;
- `id_offre` non nul ;
- `id_offre` unique.

## Contrôles réalisés après chargement

- nombre de lignes PostgreSQL identique au fichier Silver ;
- aucun `id_offre` manquant ;
- aucun doublon sur `id_offre`.

## Résultat attendu

- 971 lignes dans `silver.france_travail_offres` ;
- 0 doublon sur `id_offre` ;
- 0 `id_offre` manquant ;
- statut `SUCCESS` dans `audit.pipeline_runs`.

## Limites actuelles

Cette version n'est pas encore incrémentale.

Elle ne gère pas encore :

- les mises à jour partielles ;
- l'historisation ;
- les suppressions logiques ;
- la détection des offres modifiées.

Ces évolutions seront traitées après stabilisation de PostgreSQL, dbt et Airflow.