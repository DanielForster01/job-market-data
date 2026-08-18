# Validation Silver PostgreSQL

## Statut global

**Statut : VALIDE**

## Source contrôlée

- Fichier Silver Parquet : `france_travail_jobs_silver_2026-08-16_21-21-28.parquet`
- Table PostgreSQL : `silver.france_travail_offres`
- Date du rapport : `2026-08-16T22:19:39.599055+00:00`

## Résumé des volumes

| Indicateur | Parquet | PostgreSQL |
|---|---:|---:|
| Nombre de lignes | 971 | 971 |
| Nombre de colonnes | 61 | 64 |
| ID distincts | 971 | 971 |
| ID manquants | 0 | 0 |
| Doublons ID | 0 | 0 |

## Contrôles de validation

| Contrôle | Résultat |
|---|---|
| nombre_lignes_identique | ✅ |
| id_offre_manquants_parquet_zero | ✅ |
| id_offre_manquants_postgresql_zero | ✅ |
| doublons_id_offre_parquet_zero | ✅ |
| doublons_id_offre_postgresql_zero | ✅ |
| ids_identiques | ✅ |
| schema_colonnes_valide | ✅ |
| batch_unique | ✅ |
| audit_latest_success | ✅ |

## Comparaison des identifiants

- IDs identiques : `True`
- IDs absents dans PostgreSQL : `0`
- IDs en trop dans PostgreSQL : `0`

## Comparaison des colonnes

- Colonnes Parquet : `61`
- Colonnes PostgreSQL : `64`
- Colonnes absentes dans PostgreSQL : `[]`
- Colonnes en plus dans PostgreSQL : `['batch_id', 'date_chargement', 'fichier_source_silver']`
- Colonnes en plus non attendues : `[]`

## Batch chargé

- Fichier source Silver : `france_travail_jobs_silver_2026-08-16_21-21-28.parquet`
- Batch ID : `7903a03b-4118-4251-be45-746129f72cbb`
- Nombre de lignes : `971`
- Première date de chargement : `2026-08-16T21:40:55.763781+00:00`
- Dernière date de chargement : `2026-08-16T21:40:55.763781+00:00`

## Dernier run d'audit

- Run ID : `3`
- Pipeline : `load_silver_to_postgres`
- Statut : `SUCCESS`
- Début : `2026-08-16T21:40:55.483380+00:00`
- Fin : `2026-08-16T21:40:57.129281+00:00`
- Message : `Chargement Silver terminé avec succès. Table=silver.france_travail_offres, fichier=france_travail_jobs_silver_2026-08-16_21-21-28.parquet, lignes=971, colonnes=64, batch_id=7903a03b-4118-4251-be45-746129f72cbb`
