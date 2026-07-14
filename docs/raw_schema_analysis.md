# Analyse du schéma Raw - API France Travail

## Fichier analysé

`data/raw/france_travail_jobs_raw_2026-07-14_00-16-56.json`

## Volumétrie

- Nombre total d'offres : 971
- Nombre de clés top-level distinctes : 45

## Champs présents dans 100 % des offres

- id
- intitule
- description
- dateCreation
- dateActualisation
- lieuTravail
- romeCode
- romeLibelle
- appellationlibelle
- entreprise
- typeContrat
- typeContratLibelle
- natureContrat
- experienceExige
- experienceLibelle
- salaire
- alternance
- nombrePostes
- origineOffre
- contexteTravail
- entrepriseAdaptee
- employeurHandiEngage
- search_keyword
- ingestion_timestamp

## Champs optionnels

| Champ | Présence | Commentaire |
|---|---:|---|
| contact | 65.0 % | Information de contact partiellement disponible |
| dureeTravailLibelle | 64.9 % | Temps de travail disponible sur une partie des offres |
| secteurActiviteLibelle | 43.2 % | Secteur d'activité incomplet |
| qualificationLibelle | 28.8 % | Qualification peu renseignée |
| competences | 13.6 % | Trop incomplet pour analyser seul les compétences |
| formations | 6.3 % | Très peu renseigné |
| langues | 5.4 % | Très peu renseigné |
| permis | 0.5 % | Quasi absent |

## Champs imbriqués identifiés

| Champ parent | Type | Sous-clés observées |
|---|---|---|
| lieuTravail | dict | libelle, latitude, longitude, codePostal, commune |
| entreprise | dict | nom, entrepriseAdaptee |
| salaire | dict | libelle |
| origineOffre | dict | origine, urlOrigine |
| contexteTravail | dict | horaires |
| competences | list[dict] | à analyser |
| formations | list[dict] | à analyser |
| langues | list[dict] | à analyser |
| permis | list[dict] | à analyser |

## Décisions pour la couche Bronze

La couche Bronze sera une version tabulaire structurée du fichier Raw.

Elle doit :

- conserver une ligne par offre récupérée ;
- conserver les métadonnées d'ingestion ;
- aplatir les objets simples comme `lieuTravail`, `entreprise`, `salaire` et `origineOffre` ;
- conserver les listes complexes au format JSON texte ;
- conserver le record brut complet dans une colonne `raw_record` pour audit ;
- ne pas appliquer de nettoyage métier avancé.

## Points d'attention

- Une même offre peut apparaître plusieurs fois via plusieurs mots-clés de recherche.
- Le champ `competences` est peu renseigné.
- Le champ `salaire` est présent mais son contenu doit être analysé.
- Les champs de localisation doivent être standardisés plus tard en Silver.