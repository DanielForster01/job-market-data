# Validation Silver R2 → PostgreSQL

## Statut global

**Statut : VALIDE**

## Source Silver R2

- Objet : `silver/france_travail/ingestion_date=2026-10-05/batch_id=ead60f51-e84c-4078-a840-40039a81cc7b/processing_run_id=d3d7f430-ccae-45e3-a997-6c45913049a0/offres.parquet`
- Batch ID : `ead60f51-e84c-4078-a840-40039a81cc7b`
- Processing Run ID : `d3d7f430-ccae-45e3-a997-6c45913049a0`
- SHA-256 : `b84b3011759dd51b8a6d2123cfad68b7b87fdbca9e4835401991e1fcf6ccffae`
- Schema version : `2.0.0`
- Lignes : `889`
- Colonnes : `70`

## PostgreSQL

- Table : `silver.france_travail_offres`
- Lignes : `889`
- IDs distincts : `889`
- Batch ID : `ead60f51-e84c-4078-a840-40039a81cc7b`
- Processing Run ID : `d3d7f430-ccae-45e3-a997-6c45913049a0`

## Comparaison du schéma

- Colonnes Silver R2 : `70`
- Colonnes PostgreSQL : `72`
- Colonnes attendues PostgreSQL : `72`
- Colonnes absentes : `[]`
- Colonnes inattendues : `[]`

## Comparaison du contenu

- Contenu identique : `True`
- Lignes comparées : `889`
- Lignes différentes : `0`

## Contrôles

| Contrôle | Résultat |
|---|---|
| silver_non_vide | ✅ |
| nombre_lignes_identique | ✅ |
| ids_distincts_identiques | ✅ |
| ids_postgresql_non_nuls | ✅ |
| ids_postgresql_non_dupliques | ✅ |
| ensembles_ids_identiques | ✅ |
| schema_postgresql_valide | ✅ |
| batch_unique_postgresql | ✅ |
| batch_id_identique | ✅ |
| processing_run_unique_postgresql | ✅ |
| processing_run_id_identique | ✅ |
| silver_schema_version_unique | ✅ |
| silver_schema_version_identique | ✅ |
| source_silver_unique_postgresql | ✅ |
| source_silver_identique | ✅ |
| date_chargement_non_nulle | ✅ |
| date_chargement_unique | ✅ |
| contenu_silver_postgresql_identique | ✅ |
| audit_present | ✅ |
| audit_success | ✅ |
| audit_batch_id_identique | ✅ |
| audit_processing_run_identique | ✅ |
| audit_source_object_identique | ✅ |
| audit_sha256_identique | ✅ |
| audit_volume_source_identique | ✅ |
| audit_volume_charge_identique | ✅ |
| audit_schema_version_identique | ✅ |
| audit_git_commit_present | ✅ |
| audit_git_branch_presente | ✅ |
| audit_git_worktree_clean | ✅ |
| table_intermediaire_absente | ✅ |

## Audit du chargement

- Run ID : `6`
- Statut : `SUCCESS`
- Git commit : `da03b94f80636ed72f9a3300731100950725df0a`
- Git branch : `main`
- Git worktree dirty : `False`
- Source rows : `889`
- Loaded rows : `889`
