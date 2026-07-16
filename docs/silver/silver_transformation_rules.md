# Règles de transformation Bronze vers Silver

## 1. Objectif de la couche Silver

La couche Silver a pour objectif de transformer les données issues de la couche Bronze en un jeu de données propre, typé, cohérent, standardisé et exploitable pour les prochaines étapes du projet.

La couche Silver ne produit pas encore d’indicateurs métier agrégés. Elle prépare les données pour la couche Gold, qui sera dédiée à l’analyse, aux indicateurs, aux agrégations et au dashboard.

La couche Silver doit donc permettre de passer d’une donnée structurée mais encore brute à une donnée fiable, contrôlée et compréhensible.

---

## 2. Rappel de l’architecture du projet

Le projet suit une architecture en couches :

```text
API France Travail
        ↓
Raw
        ↓
Bronze
        ↓
Silver
        ↓
Gold
        ↓
Dashboard / Restitution
```

### Raw

La couche Raw conserve la réponse JSON brute de l’API France Travail, sans modification.

### Bronze

La couche Bronze transforme le JSON brut en table Parquet structurée. Elle aplatit certains champs imbriqués simples, conserve les champs complexes au format JSON string, et ajoute des éléments de traçabilité.

### Silver

La couche Silver nettoie, renomme, type, standardise et contrôle les données. Elle ne réalise pas encore d’agrégation métier.

### Gold

La couche Gold sera utilisée pour construire les tables analytiques, les indicateurs métier et les jeux de données prêts pour Power BI.

---

## 3. Source de la transformation Silver

La transformation Silver utilise le dernier fichier Parquet disponible dans le dossier :

```text
data/bronze/
```

Le fichier Bronze attendu suit le format :

```text
france_travail_jobs_bronze_YYYY-MM-DD_HH-MM-SS.parquet
```

Exemple de fichier utilisé :

```text
france_travail_jobs_bronze_2026-07-14_00-20-49.parquet
```

La couche Bronze contient actuellement :

```text
971 lignes
54 colonnes
```

---

## 4. Sortie attendue

La transformation Silver doit produire un fichier Parquet dans :

```text
data/silver/
```

Le fichier Silver suivra le format :

```text
france_travail_jobs_silver_YYYY-MM-DD_HH-MM-SS.parquet
```

Le timestamp du fichier Silver doit correspondre au timestamp de traitement ou être dérivé du fichier Bronze utilisé.

---

## 5. Convention de nommage des colonnes Silver

Le projet utilise une convention de nommage en français, car :

* la source de données est française ;
* les données proviennent de France Travail ;
* le contexte métier est français ;
* les futurs utilisateurs ou recruteurs ciblés sont en France ;
* la restitution finale sera probablement en français.

La convention retenue est :

```text
snake_case
sans accents
sans espaces
noms explicites
noms métier en français
```

Exemples :

```text
id_offre
intitule_offre
date_creation
nom_entreprise
salaire_renseigne
```

Les noms comme `dateCréation`, `DateCreation`, `lieu travail` ou `companyName` sont évités.

---

## 6. Granularité de la couche Silver

La granularité de la couche Silver est :

```text
1 ligne = 1 offre d’emploi France Travail
```

L’identifiant métier principal est :

```text
id_offre
```

Il provient de la colonne Bronze :

```text
id
```

La couche Silver doit conserver une ligne par identifiant d’offre unique.

---

## 7. Règles de gestion des doublons

### 7.1 Doublons stricts sur l’identifiant

Si plusieurs lignes possèdent le même `id_offre`, la couche Silver doit conserver uniquement la version la plus récente, en s’appuyant sur :

```text
date_actualisation
```

Règle :

```text
Pour un même id_offre, conserver la ligne avec la date_actualisation la plus récente.
```

Le nombre de lignes supprimées doit être documenté dans le rapport qualité Silver.

### 7.2 Quasi-doublons

L’analyse Bronze a révélé l’existence de quasi-doublons : certaines offres ont des intitulés, entreprises et lieux identiques ou très proches, mais des identifiants différents.

Ces quasi-doublons ne doivent pas être supprimés en Silver.

Raison :

* ils peuvent correspondre à plusieurs publications réelles ;
* ils peuvent représenter plusieurs postes similaires ;
* ils peuvent avoir des descriptions différentes ;
* ils peuvent être publiés à des dates différentes ;
* ils possèdent des identifiants France Travail distincts.

La couche Silver conserve donc toutes les offres tant que `id_offre` est unique.

Une future couche analytique pourra créer une clé de rapprochement, par exemple :

```text
cle_rapprochement_offre = intitule_offre + nom_entreprise + libelle_lieu_travail
```

Cette clé servira à analyser les offres similaires, mais pas à supprimer automatiquement les lignes.

---

## 8. Règles de renommage des colonnes

| Colonne Bronze                | Colonne Silver                     | Décision                                   |
| ----------------------------- | ---------------------------------- | ------------------------------------------ |
| `id`                          | `id_offre`                         | Identifiant unique de l’offre              |
| `intitule`                    | `intitule_offre`                   | Titre de l’offre                           |
| `description`                 | `description_offre`                | Description complète de l’offre            |
| `dateCreation`                | `date_creation`                    | Date de création de l’offre                |
| `dateActualisation`           | `date_actualisation`               | Date de dernière actualisation             |
| `lieuTravail_libelle`         | `libelle_lieu_travail`             | Localisation textuelle                     |
| `lieuTravail_latitude`        | `latitude`                         | Latitude du lieu de travail                |
| `lieuTravail_longitude`       | `longitude`                        | Longitude du lieu de travail               |
| `lieuTravail_codePostal`      | `code_postal`                      | Code postal                                |
| `lieuTravail_commune`         | `code_commune`                     | Code commune INSEE                         |
| `entreprise_nom`              | `nom_entreprise`                   | Nom de l’entreprise                        |
| `entreprise_description`      | `description_entreprise`           | Description de l’entreprise                |
| `entrepriseAdaptee`           | `est_entreprise_adaptee`           | Indique si l’entreprise est adaptée        |
| `employeurHandiEngage`        | `est_employeur_handi_engage`       | Indique si l’employeur est engagé handicap |
| `typeContrat`                 | `code_type_contrat`                | Code du type de contrat                    |
| `typeContratLibelle`          | `libelle_type_contrat`             | Libellé du type de contrat                 |
| `natureContrat`               | `nature_contrat`                   | Nature du contrat                          |
| `experienceExige`             | `experience_exigee`                | Indicateur d’expérience exigée             |
| `experienceLibelle`           | `libelle_experience`               | Libellé de l’expérience                    |
| `romeCode`                    | `code_rome`                        | Code ROME                                  |
| `romeLibelle`                 | `libelle_rome`                     | Libellé ROME                               |
| `appellationlibelle`          | `libelle_appellation`              | Appellation métier                         |
| `salaire_libelle`             | `salaire_libelle`                  | Texte salaire original nettoyé             |
| `alternance`                  | `est_alternance`                   | Indique si l’offre concerne une alternance |
| `nombrePostes`                | `nombre_postes`                    | Nombre de postes proposés                  |
| `dureeTravailLibelle`         | `libelle_duree_travail`            | Durée de travail                           |
| `dureeTravailLibelleConverti` | `libelle_duree_travail_converti`   | Durée de travail convertie                 |
| `qualificationCode`           | `code_qualification`               | Code qualification                         |
| `qualificationLibelle`        | `libelle_qualification`            | Libellé qualification                      |
| `codeNAF`                     | `code_naf`                         | Code NAF                                   |
| `secteurActivite`             | `code_secteur_activite`            | Code secteur d’activité                    |
| `secteurActiviteLibelle`      | `libelle_secteur_activite`         | Libellé secteur d’activité                 |
| `trancheEffectifEtab`         | `tranche_effectif_etablissement`   | Tranche d’effectif établissement           |
| `offresManqueCandidats`       | `offre_difficile_a_pourvoir`       | Indique si l’offre manque de candidats     |
| `accessibleTH`                | `accessible_travailleur_handicape` | Accessibilité travailleur handicapé        |
| `deplacementCode`             | `code_deplacement`                 | Code déplacement                           |
| `deplacementLibelle`          | `libelle_deplacement`              | Libellé déplacement                        |
| `experienceCommentaire`       | `commentaire_experience`           | Commentaire expérience                     |
| `complementExercice`          | `complement_exercice`              | Complément d’exercice                      |
| `origineOffre_origine`        | `origine_offre`                    | Origine de l’offre                         |
| `origineOffre_urlOrigine`     | `url_origine_offre`                | URL de l’offre d’origine                   |
| `contexteTravail_horaires`    | `horaires_travail`                 | Horaires de travail                        |
| `search_keyword`              | `mot_cle_recherche`                | Mot-clé utilisé lors de l’ingestion        |
| `ingestion_timestamp`         | `date_ingestion`                   | Date d’ingestion                           |
| `raw_source_file`             | `fichier_source`                   | Fichier Bronze source                      |
| `raw_record`                  | `enregistrement_brut`              | Enregistrement JSON brut                   |
| `competences`                 | `competences_brutes`               | Champ complexe conservé                    |
| `formations`                  | `formations_brutes`                | Champ complexe conservé                    |
| `langues`                     | `langues_brutes`                   | Champ complexe conservé                    |
| `qualitesProfessionnelles`    | `qualites_professionnelles_brutes` | Champ complexe conservé                    |
| `contact`                     | `contact_brut`                     | Champ complexe conservé                    |
| `agence`                      | `agence_brute`                     | Champ complexe conservé                    |
| `permis`                      | `permis_bruts`                     | Champ complexe conservé                    |

---

## 9. Colonnes exclues de Silver

Certaines colonnes Bronze ne doivent pas être conservées dans Silver lorsqu’elles sont redondantes ou moins fiables.

| Colonne Bronze                 | Décision                             | Justification                       |
| ------------------------------ | ------------------------------------ | ----------------------------------- |
| `entreprise_entrepriseAdaptee` | Exclure                              | Redondante avec `entrepriseAdaptee` |
| `raw_record`                   | Conserver sous `enregistrement_brut` | Utile pour l’audit                  |
| `raw_source_file`              | Conserver sous `fichier_source`      | Utile pour la traçabilité           |

Après investigation, `entrepriseAdaptee` et `entreprise_entrepriseAdaptee` donnent la même information lorsqu’elles sont toutes les deux renseignées.

Cependant, `entrepriseAdaptee` est disponible pour 100 % des lignes, tandis que `entreprise_entrepriseAdaptee` est disponible sur environ 50,88 % des lignes.

La couche Silver conserve donc uniquement :

```text
entrepriseAdaptee → est_entreprise_adaptee
```

---

## 10. Règles de typage

| Colonne Silver               | Type attendu    |
| ---------------------------- | --------------- |
| `id_offre`                   | string          |
| `intitule_offre`             | string          |
| `description_offre`          | string          |
| `date_creation`              | datetime UTC    |
| `date_actualisation`         | datetime UTC    |
| `libelle_lieu_travail`       | string nullable |
| `latitude`                   | float nullable  |
| `longitude`                  | float nullable  |
| `code_postal`                | string nullable |
| `code_commune`               | string nullable |
| `nom_entreprise`             | string nullable |
| `description_entreprise`     | string nullable |
| `est_entreprise_adaptee`     | boolean         |
| `est_employeur_handi_engage` | boolean         |
| `code_type_contrat`          | string nullable |
| `libelle_type_contrat`       | string nullable |
| `nature_contrat`             | string nullable |
| `experience_exigee`          | string nullable |
| `libelle_experience`         | string nullable |
| `code_rome`                  | string nullable |
| `libelle_rome`               | string nullable |
| `libelle_appellation`        | string nullable |
| `salaire_libelle`            | string nullable |
| `est_alternance`             | boolean         |
| `nombre_postes`              | integer         |
| `mot_cle_recherche`          | string nullable |
| `date_ingestion`             | datetime UTC    |
| `fichier_source`             | string          |
| `enregistrement_brut`        | string          |
| `date_traitement_silver`     | datetime UTC    |

Pour les booléens avec valeurs manquantes, le type attendu est un booléen nullable.

---

## 11. Nettoyage des chaînes de caractères

Les colonnes textuelles doivent être nettoyées selon les règles suivantes :

* suppression des espaces en début et fin de chaîne ;
* remplacement des chaînes vides par une valeur manquante ;
* conservation du texte original autant que possible ;
* absence de traduction ;
* absence de reformulation ;
* absence de correction automatique des noms d’entreprise ;
* conservation de la casse des intitulés, entreprises, descriptions et lieux ;
* passage en minuscules uniquement de `mot_cle_recherche`.

Exemple :

```text
"  Data Analyst  " → "Data Analyst"
"" → valeur manquante
"DATA ANALYST (H/F)" → "DATA ANALYST (H/F)"
```

La couche Silver ne modifie pas le sens métier de la donnée.

---

## 12. Traitement des dates

Les colonnes suivantes doivent être converties en datetime UTC :

| Colonne Bronze        | Colonne Silver       | Format observé        |
| --------------------- | -------------------- | --------------------- |
| `dateCreation`        | `date_creation`      | ISO UTC               |
| `dateActualisation`   | `date_actualisation` | ISO UTC               |
| `ingestion_timestamp` | `date_ingestion`     | `YYYY-MM-DD_HH-MM-SS` |

Règles :

* les dates valides sont converties en datetime UTC ;
* les dates invalides deviennent nulles ;
* les lignes ne sont pas supprimées automatiquement en cas de date invalide ;
* le nombre de dates invalides doit être mesuré dans le rapport qualité Silver.

Une colonne calculée doit être ajoutée :

```text
date_actualisation_coherente
```

Règle :

```text
True si date_actualisation >= date_creation
False sinon
```

---

## 13. Traitement des informations entreprise

L’analyse Bronze a montré que le nom de l’entreprise est absent dans une part importante des offres.

Résultats observés :

```text
nom entreprise présent : 468 / 971 = 48,2 %
description entreprise présente : 643 / 971 = 66,22 %
nom OU description disponible : 854 / 971 = 87,95 %
aucune information entreprise : 117 / 971 = 12,05 %
```

Décisions :

* `nom_entreprise` est conservé lorsqu’il est disponible ;
* `description_entreprise` est conservée lorsqu’elle est disponible ;
* `description_entreprise` ne remplace jamais automatiquement `nom_entreprise` ;
* aucun nom d’entreprise n’est inventé ;
* les valeurs manquantes restent manquantes.

Colonnes calculées à créer :

```text
entreprise_renseignee
description_entreprise_renseignee
information_entreprise_disponible
```

Règles :

```text
entreprise_renseignee = True si nom_entreprise est renseigné
description_entreprise_renseignee = True si description_entreprise est renseignée
information_entreprise_disponible = True si nom_entreprise ou description_entreprise est renseigné
```

---

## 14. Traitement du salaire

L’analyse Bronze a montré que le salaire est renseigné pour :

```text
249 offres sur 971
soit 25,64 %
```

Le champ `salaire_libelle` contient plusieurs formats distincts.

Formats observés :

```text
Annuel de # Euros à # Euros
Annuel de # Euros à # Euros sur # mois
Mensuel de # Euros à # Euros sur # mois
Annuel de # Euros sur # mois
Mensuel de # Euros sur # mois
Mensuel de # Euros à # Euros
Annuel de # Euros
Horaire de # Euros à # Euros sur # mois
```

Décisions Silver :

* conserver le texte original nettoyé dans `salaire_libelle` ;
* créer un indicateur `salaire_renseigne` ;
* extraire uniquement la période du salaire dans `periode_salaire` si elle est identifiable ;
* ne pas encore calculer de salaire moyen ;
* ne pas imputer les salaires manquants ;
* ne pas comparer directement des salaires annuels, mensuels et horaires ;
* ne pas corriger automatiquement les valeurs suspectes.

Colonnes prévues :

```text
salaire_renseigne
periode_salaire
```

Règles :

```text
salaire_renseigne = True si salaire_libelle est renseigné
periode_salaire = Annuel / Mensuel / Horaire si identifiable
```

Les colonnes suivantes ne seront pas créées dans cette première version Silver :

```text
salaire_min
salaire_max
salaire_moyen
salaire_annuel_estime
salaire_suspect
```

Ces colonnes feront l’objet d’une étape dédiée après analyse plus approfondie des formats de salaire.

---

## 15. Traitement de la localisation

Les colonnes de localisation issues de Bronze sont :

```text
lieuTravail_libelle
lieuTravail_latitude
lieuTravail_longitude
lieuTravail_codePostal
lieuTravail_commune
```

Règles Silver :

* conserver le libellé original nettoyé dans `libelle_lieu_travail` ;
* conserver `latitude` et `longitude` si disponibles ;
* conserver `code_postal` sous forme de chaîne ;
* conserver `code_commune` sous forme de chaîne ;
* ne pas encore extraire le nom de ville depuis `libelle_lieu_travail` ;
* ne pas encore créer de dimension géographique.

Colonne calculée :

```text
coordonnees_renseignees
```

Règle :

```text
coordonnees_renseignees = True si latitude et longitude sont renseignées
```

L’extraction avancée de la ville, du département ou de la région sera traitée dans une étape ultérieure.

---

## 16. Traitement des contrats

Les colonnes liées au contrat sont :

```text
typeContrat
typeContratLibelle
natureContrat
```

Les valeurs observées de `typeContrat` sont notamment :

```text
CDI
CDD
MIS
LIB
```

Règles Silver :

* conserver le code contrat dans `code_type_contrat` ;
* conserver le libellé complet dans `libelle_type_contrat` ;
* conserver la nature du contrat dans `nature_contrat` ;
* ne pas regrouper les contrats à ce stade ;
* ne pas créer encore de dimension contrat.

Une normalisation avancée pourra être réalisée plus tard dans Gold.

---

## 17. Traitement des champs complexes

Certains champs sont très peu renseignés ou ont une structure complexe :

```text
competences
formations
langues
qualitesProfessionnelles
contact
agence
permis
```

Ces champs sont conservés en Silver sous forme de chaînes JSON renommées :

```text
competences_brutes
formations_brutes
langues_brutes
qualites_professionnelles_brutes
contact_brut
agence_brute
permis_bruts
```

Règles :

* conserver les valeurs JSON string telles qu’elles existent en Bronze ;
* ne pas les exploser en plusieurs lignes dans cette étape ;
* ne pas créer de table de compétences en Silver pour l’instant ;
* vérifier leur validité JSON dans le rapport qualité Silver.

Une étape dédiée pourra transformer ces champs en tables séparées plus tard.

---

## 18. Colonnes calculées à créer en Silver

| Colonne                             | Règle                                            |
| ----------------------------------- | ------------------------------------------------ |
| `salaire_renseigne`                 | True si `salaire_libelle` est renseigné          |
| `periode_salaire`                   | Annuel, Mensuel ou Horaire si identifiable       |
| `entreprise_renseignee`             | True si `nom_entreprise` est renseigné           |
| `description_entreprise_renseignee` | True si `description_entreprise` est renseignée  |
| `information_entreprise_disponible` | True si nom ou description entreprise disponible |
| `coordonnees_renseignees`           | True si latitude et longitude disponibles        |
| `date_actualisation_coherente`      | True si `date_actualisation >= date_creation`    |
| `date_traitement_silver`            | Date et heure de traitement Silver               |

---

## 19. Traitement des valeurs manquantes

Aucune valeur métier ne doit être inventée.

Règles :

* ne pas remplacer un nom d’entreprise manquant ;
* ne pas remplacer un salaire manquant ;
* ne pas remplacer une localisation manquante par une valeur arbitraire ;
* ne pas imputer les salaires ;
* ne pas inventer de date ;
* ne pas créer de faux code postal ou code commune.

Les valeurs manquantes restent nulles.

Les indicateurs booléens permettent de savoir si une information est disponible ou non.

---

## 20. Contrôles qualité attendus sur Silver

Un rapport qualité Silver devra être généré après la transformation.

Il devra vérifier :

### Volumétrie

* nombre de lignes Bronze ;
* nombre de lignes Silver ;
* écart Bronze / Silver ;
* nombre de doublons supprimés sur `id_offre`.

### Clé métier

* présence de `id_offre` ;
* absence de valeurs manquantes sur `id_offre` ;
* unicité de `id_offre`.

### Dates

* validité de `date_creation` ;
* validité de `date_actualisation` ;
* validité de `date_ingestion`;
* cohérence `date_actualisation >= date_creation`.

### Complétude

Mesurer les valeurs manquantes sur les colonnes critiques :

```text
id_offre
intitule_offre
description_offre
date_creation
date_actualisation
libelle_lieu_travail
code_type_contrat
libelle_type_contrat
mot_cle_recherche
fichier_source
enregistrement_brut
```

### Entreprise

Mesurer :

```text
taux de nom_entreprise renseigné
taux de description_entreprise renseignée
taux d’information entreprise disponible
```

### Salaire

Mesurer :

```text
taux de salaire renseigné
répartition de periode_salaire
```

### JSON

Vérifier la validité JSON des colonnes :

```text
competences_brutes
formations_brutes
langues_brutes
qualites_professionnelles_brutes
contact_brut
agence_brute
permis_bruts
enregistrement_brut
```

---

## 21. Transformations exclues de Silver

Les transformations suivantes sont volontairement exclues de la première version Silver :

* calcul de KPI ;
* agrégation par ville ;
* agrégation par métier ;
* agrégation par contrat ;
* création de dashboard ;
* calcul du salaire moyen ;
* estimation du salaire annuel ;
* correction automatique des salaires extrêmes ;
* extraction avancée des compétences depuis la description ;
* extraction automatique du nom d’entreprise depuis la description ;
* création d’une table de compétences ;
* création d’une table de dimensions ;
* orchestration Airflow ;
* chargement PostgreSQL ;
* transformation dbt ;
* création de la couche Gold.

Ces étapes viendront uniquement lorsque Silver sera stable, testée et validée.

---

## 22. Critères de validation de la couche Silver

La couche Silver sera considérée comme validée lorsque :

* les règles de transformation sont documentées ;
* le script `bronze_to_silver.py` est créé ;
* le script est exécutable depuis la racine du projet ;
* le fichier Silver Parquet est généré ;
* le fichier Silver peut être relu correctement ;
* le nombre de lignes est cohérent avec Bronze ;
* `id_offre` est unique ;
* les colonnes critiques sont présentes ;
* les dates sont correctement converties ;
* les colonnes calculées sont présentes ;
* le rapport qualité Silver est généré ;
* les décisions sont documentées dans Git ;
* les fichiers de données restent exclus du dépôt Git.

---

## 23. Storytelling entretien

La couche Silver a été conçue après analyse de la couche Bronze et non développée directement.

Les principales décisions ont été prises à partir des observations suivantes :

* les salaires sont souvent manquants ;
* les formats de salaire mélangent annuel, mensuel et horaire ;
* les noms d’entreprise sont souvent absents ;
* les descriptions d’entreprise apportent une information complémentaire importante ;
* certaines offres sont quasi-dupliquées mais possèdent des identifiants distincts ;
* certaines colonnes sont redondantes, comme `entrepriseAdaptee` et `entreprise_entrepriseAdaptee`.

La couche Silver applique donc un nettoyage prudent :

* elle renomme les colonnes en français ;
* elle type les dates ;
* elle conserve les valeurs manquantes lorsqu’elles reflètent la source ;
* elle crée des indicateurs de disponibilité ;
* elle ne supprime pas les quasi-doublons ;
* elle ne produit pas encore d’agrégations métier.

Cette démarche permet de construire une base fiable, traçable et maintenable avant de passer à la couche Gold.
