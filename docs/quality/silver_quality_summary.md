# Rapport qualité — Couche Silver

## 1. Informations générales

- Généré le : `2026-07-16T00:18:27.082766+00:00`
- Statut : **VALIDE**
- Fichier Bronze analysé : `france_travail_jobs_bronze_2026-07-14_00-20-49.parquet`
- Fichier Silver analysé : `france_travail_jobs_silver_2026-07-15_23-45-36.parquet`

---

## 2. Volumétrie

| Indicateur | Valeur |
|---|---:|
| Lignes Bronze | 971 |
| Lignes Silver | 971 |
| Écart Bronze - Silver | 0 |
| Colonnes Bronze | 54 |
| Colonnes Silver | 61 |

---

## 3. Identifiant métier

| Indicateur | Valeur |
|---|---:|
| Colonne `id_offre` présente | True |
| `id_offre` manquants | 0 |
| `id_offre` uniques | 971 |
| Doublons `id_offre` | 0 |

---

## 4. Colonnes

### Colonnes critiques absentes

```text
[]
```

### Colonnes calculées absentes

```text
[]
```

### Valeurs manquantes sur les colonnes critiques

```json
{
  "id_offre": 0,
  "intitule_offre": 0,
  "description_offre": 0,
  "date_creation": 0,
  "date_actualisation": 0,
  "libelle_lieu_travail": 0,
  "code_type_contrat": 0,
  "libelle_type_contrat": 0,
  "mot_cle_recherche": 0,
  "fichier_source": 0,
  "enregistrement_brut": 0
}
```

---

## 5. Dates

```json
{
  "date_creation": {
    "presente": true,
    "type": "datetime64[ns, UTC]",
    "valeurs_manquantes": 0,
    "date_min": "2025-05-21 03:49:47+00:00",
    "date_max": "2026-07-13 15:09:31.777000+00:00"
  },
  "date_actualisation": {
    "presente": true,
    "type": "datetime64[ns, UTC]",
    "valeurs_manquantes": 0,
    "date_min": "2026-04-15 05:11:54+00:00",
    "date_max": "2026-07-13 23:01:15+00:00"
  },
  "date_ingestion": {
    "presente": true,
    "type": "datetime64[ns, UTC]",
    "valeurs_manquantes": 0,
    "date_min": "2026-07-14 00:20:49+00:00",
    "date_max": "2026-07-14 00:20:49+00:00"
  },
  "date_traitement_silver": {
    "presente": true,
    "type": "datetime64[us, UTC]",
    "valeurs_manquantes": 0,
    "date_min": "2026-07-15 23:45:36.286140+00:00",
    "date_max": "2026-07-15 23:45:36.286140+00:00"
  },
  "date_actualisation_coherente": {
    "True": 971
  }
}
```

---

## 6. Informations entreprise

| Indicateur | Nombre | Taux |
|---|---:|---:|
| Nom entreprise renseigné | 468 | 48.2 % |
| Description entreprise renseignée | 643 | 66.22 % |
| Nom ou description disponible | 854 | 87.95 % |

---

## 7. Salaire

| Indicateur | Nombre | Taux |
|---|---:|---:|
| Salaire renseigné | 249 | 25.64 % |

### Répartition de periode_salaire

| Valeur | Nombre |
|---|---:|
| `NULL` | 722 |
| `Annuel` | 222 |
| `Mensuel` | 26 |
| `Horaire` | 1 |

---

## 8. Localisation

| Indicateur | Nombre | Taux |
|---|---:|---:|
| Coordonnées renseignées | 798 | 82.18 % |
| Code postal renseigné | 810 | 83.42 % |
| Code commune renseigné | 892 | 91.86 % |

---

## 9. Contrats

### code_type_contrat

| Valeur | Nombre |
|---|---:|
| `CDI` | 691 |
| `CDD` | 153 |
| `MIS` | 116 |
| `LIB` | 11 |

### nature_contrat

| Valeur | Nombre |
|---|---:|
| `Contrat travail` | 857 |
| `Cont. professionnalisation` | 56 |
| `Contrat apprentissage` | 45 |
| `Emploi non salarié` | 11 |
| `CDI de chantier ou d'opération` | 2 |

---

## 10. Validité JSON

```json
{
  "competences_brutes": {
    "presente": true,
    "valeurs_non_vides": 132,
    "json_valides": 132,
    "json_invalides": 0,
    "exemples_invalides": []
  },
  "formations_brutes": {
    "presente": true,
    "valeurs_non_vides": 61,
    "json_valides": 61,
    "json_invalides": 0,
    "exemples_invalides": []
  },
  "langues_brutes": {
    "presente": true,
    "valeurs_non_vides": 52,
    "json_valides": 52,
    "json_invalides": 0,
    "exemples_invalides": []
  },
  "qualites_professionnelles_brutes": {
    "presente": true,
    "valeurs_non_vides": 84,
    "json_valides": 84,
    "json_invalides": 0,
    "exemples_invalides": []
  },
  "contact_brut": {
    "presente": true,
    "valeurs_non_vides": 631,
    "json_valides": 631,
    "json_invalides": 0,
    "exemples_invalides": []
  },
  "agence_brute": {
    "presente": true,
    "valeurs_non_vides": 158,
    "json_valides": 158,
    "json_invalides": 0,
    "exemples_invalides": []
  },
  "permis_bruts": {
    "presente": true,
    "valeurs_non_vides": 5,
    "json_valides": 5,
    "json_invalides": 0,
    "exemples_invalides": []
  },
  "enregistrement_brut": {
    "presente": true,
    "valeurs_non_vides": 971,
    "json_valides": 971,
    "json_invalides": 0,
    "exemples_invalides": []
  }
}
```

---

## 11. Top valeurs manquantes

| Colonne | Manquant | Présent | Taux manquant |
|---|---:|---:|---:|
| `permis_bruts` | 966 | 5 | 99.49 % |
| `commentaire_experience` | 966 | 5 | 99.49 % |
| `langues_brutes` | 919 | 52 | 94.64 % |
| `formations_brutes` | 910 | 61 | 93.72 % |
| `code_deplacement` | 898 | 73 | 92.48 % |
| `complement_exercice` | 898 | 73 | 92.48 % |
| `libelle_deplacement` | 897 | 74 | 92.38 % |
| `qualites_professionnelles_brutes` | 887 | 84 | 91.35 % |
| `competences_brutes` | 839 | 132 | 86.41 % |
| `offre_difficile_a_pourvoir` | 815 | 156 | 83.93 % |
| `agence_brute` | 813 | 158 | 83.73 % |
| `tranche_effectif_etablissement` | 773 | 198 | 79.61 % |
| `libelle_duree_travail_converti` | 760 | 211 | 78.27 % |
| `salaire_libelle` | 722 | 249 | 74.36 % |
| `periode_salaire` | 722 | 249 | 74.36 % |
| `code_qualification` | 691 | 280 | 71.16 % |
| `libelle_qualification` | 691 | 280 | 71.16 % |
| `accessible_travailleur_handicape` | 557 | 414 | 57.36 % |
| `code_naf` | 552 | 419 | 56.85 % |
| `code_secteur_activite` | 552 | 419 | 56.85 % |
| `libelle_secteur_activite` | 552 | 419 | 56.85 % |
| `nom_entreprise` | 503 | 468 | 51.8 % |
| `horaires_travail` | 341 | 630 | 35.12 % |
| `libelle_duree_travail` | 341 | 630 | 35.12 % |
| `contact_brut` | 340 | 631 | 35.02 % |

---

## 12. Points à contrôler

[]

---

## 13. Conclusion

La couche Silver est considérée comme techniquement valide si :

- `id_offre` est présent, unique et non nul ;
- les colonnes critiques sont présentes ;
- les colonnes calculées Silver sont présentes ;
- les dates sont correctement typées ;
- les champs JSON conservés sont valides ;
- les valeurs manquantes correspondent aux limites de la source et non à une erreur de transformation.
