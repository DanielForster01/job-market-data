# Sources de données

## Source principale : API Offres d’emploi France Travail

L’API Offres d’emploi de France Travail permet de récupérer des offres d’emploi actives collectées par France Travail et ses partenaires.

### Usage dans le projet

Nous utilisons cette API pour collecter des offres liées aux métiers Data :

- Data Engineer
- Data Analyst
- BI Analyst
- Analytics Engineer
- Python Data

### Données attendues

Les champs attendus sont notamment :

- identifiant de l’offre
- intitulé du poste
- entreprise
- localisation
- type de contrat
- date de création
- date d’actualisation
- description
- compétences
- salaire si disponible

### Couche Bronze

Les données sont d’abord stockées au format JSON brut dans `data/raw/`, sans transformation.