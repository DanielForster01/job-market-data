{{ config(materialized='table') }}

-- ============================================================
-- Modèle : int_france_travail__salaires_normalises
-- ============================================================
--
-- Objectif :
--   - parser les salaires renseignés dans salaire_libelle ;
--   - extraire les bornes min / max ;
--   - annualiser les salaires mensuels ;
--   - ne pas annualiser les salaires horaires sans hypothèse
--     métier supplémentaire ;
--   - accepter les commentaires libres ajoutés après le salaire ;
--   - détecter les valeurs atypiques ;
--   - conserver les anomalies plutôt que les corriger
--     silencieusement ;
--   - identifier explicitement les salaires exploitables
--     pour les futurs KPI.
--
-- Formats pris en charge :
--
--   Annuel de X Euros
--   Annuel de X Euros à Y Euros
--   Annuel de X Euros sur Z mois
--   Annuel de X Euros à Y Euros sur Z mois
--
--   Mensuel de X Euros
--   Mensuel de X Euros à Y Euros
--   Mensuel de X Euros sur Z mois
--   Mensuel de X Euros à Y Euros sur Z mois
--
--   Horaire de X Euros
--   Horaire de X Euros à Y Euros
--
-- Les mêmes formats peuvent être suivis d'un commentaire libre :
--
--   Annuel de X Euros à Y Euros - Selon profil
--   Mensuel de X Euros - Prime mensuelle
--   Horaire de X Euros à Y Euros - Primes, CSE
--
-- Le commentaire n'est PAS utilisé pour modifier automatiquement
-- le montant du salaire.
--
-- Le salaire_libelle original est conservé intégralement pour
-- assurer la traçabilité.
--
-- Les salaires horaires ne sont volontairement pas annualisés :
-- une conversion nécessiterait une hypothèse sur le nombre
-- d'heures travaillées annuellement.
--
-- Le seuil de 200 000 EUR est actuellement utilisé comme contrôle
-- qualité pour identifier les valeurs annualisées atypiques.
--
-- Une valeur suspecte est conservée dans le modèle mais exclue
-- des KPI nécessitant un salaire exploitable.
-- ============================================================


with parametres as (

    select
        200000::numeric as seuil_salaire_annuel_suspect

),


offres as (

    select
        id_offre,
        intitule_offre,

        famille_metier_data,
        categorie_contrat,
        categorie_temps_travail,

        code_departement,

        salaire_libelle,
        salaire_renseigne,

        periode_salaire,
        periode_salaire_normalisee,

        -- Traçabilité du snapshot
        batch_id,
        processing_run_id,
        silver_schema_version,
        fichier_source_silver,
        date_chargement

    from {{ ref('int_france_travail__offres_enrichies') }}

    where salaire_renseigne is true

),


matches as (

    select
        offres.*,

        -- ========================================================
        -- Parsing du noyau structuré du salaire
        --
        -- Le suffixe :
        --
        --     - commentaire libre
        --
        -- est volontairement accepté mais n'intervient pas dans
        -- le calcul du salaire.
        -- ========================================================

        regexp_match(
            trim(salaire_libelle),

            '^(Annuel|Mensuel|Horaire)\s+de\s+([0-9]+\.?[0-9]*)\s+Euros(?:\s+à\s+([0-9]+\.?[0-9]*)\s+Euros)?(?:\s+sur\s+([0-9]+\.?[0-9]*)\s+mois)?(?:\s+-\s+.*)?$'

        ) as regex_match

    from offres

),


extraits as (

    select
        id_offre,
        intitule_offre,

        famille_metier_data,
        categorie_contrat,
        categorie_temps_travail,

        code_departement,

        salaire_libelle,
        salaire_renseigne,

        periode_salaire,
        periode_salaire_normalisee,

        -- Traçabilité du snapshot
        batch_id,
        processing_run_id,
        silver_schema_version,
        fichier_source_silver,
        date_chargement,

        -- Résultat du parsing
        regex_match,

        case
            when regex_match is not null
                then true

            else false
        end as salaire_parse_ok,

        regex_match[1] as periode_extraite,

        nullif(
            regex_match[2],
            ''
        )::numeric as salaire_min_brut,

        nullif(
            regex_match[3],
            ''
        )::numeric as salaire_max_brut_extrait,

        nullif(
            regex_match[4],
            ''
        )::numeric as duree_mois_brute

    from matches

),


salaires_prepares as (

    select
        *,

        -- ========================================================
        -- Pour un montant unique :
        --
        -- salaire_min_brut = salaire_max_brut
        -- ========================================================

        case
            when salaire_min_brut is not null
                then coalesce(
                    salaire_max_brut_extrait,
                    salaire_min_brut
                )

            else null
        end as salaire_max_brut,


        case
            when salaire_parse_ok is false
                then null

            when salaire_max_brut_extrait is null
                then 'Montant unique'

            else 'Fourchette'
        end as format_salaire,


        -- ========================================================
        -- Cohérence de la période
        --
        -- On compare la période extraite du texte à la période
        -- déjà normalisée dans l'intermediate principal.
        -- ========================================================

        case
            when salaire_parse_ok is true
             and periode_extraite = periode_salaire_normalisee
                then true

            else false
        end as periode_salaire_coherente

    from extraits

),


annualises as (

    select
        salaires_prepares.*,

        -- ========================================================
        -- Salaire minimum annualisé
        -- ========================================================

        case
            when periode_extraite = 'Annuel'
                then salaire_min_brut

            when periode_extraite = 'Mensuel'
                then salaire_min_brut * 12

            -- Pas d'annualisation du salaire horaire
            else null
        end as salaire_annuel_min,


        -- ========================================================
        -- Salaire maximum annualisé
        -- ========================================================

        case
            when periode_extraite = 'Annuel'
                then salaire_max_brut

            when periode_extraite = 'Mensuel'
                then salaire_max_brut * 12

            else null
        end as salaire_annuel_max

    from salaires_prepares

),


calculs as (

    select
        annualises.*,

        -- ========================================================
        -- Milieu de la fourchette annualisée
        --
        -- Ce n'est PAS un salaire moyen du marché.
        -- Il s'agit de la valeur centrale de l'offre.
        -- ========================================================

        case
            when salaire_annuel_min is not null
             and salaire_annuel_max is not null
                then round(
                    (
                        salaire_annuel_min
                        + salaire_annuel_max
                    ) / 2.0,
                    2
                )

            else null
        end as salaire_annuel_central,


        -- ========================================================
        -- Valeur centrale avant annualisation
        -- ========================================================

        case
            when salaire_min_brut is not null
             and salaire_max_brut is not null
                then round(
                    (
                        salaire_min_brut
                        + salaire_max_brut
                    ) / 2.0,
                    2
                )

            else null
        end as salaire_central_brut

    from annualises

),


qualification as (

    select
        calculs.*,

        parametres.seuil_salaire_annuel_suspect,


        -- ========================================================
        -- Détection des salaires atypiques
        --
        -- Le contrôle est réalisé APRÈS annualisation.
        --
        -- Exemple :
        --
        --   Mensuel 45 000 EUR
        --       -> 540 000 EUR annualisés
        --       -> valeur suspecte
        --
        -- La valeur n'est jamais corrigée automatiquement.
        -- ========================================================

        case
            when salaire_annuel_max
                 > parametres.seuil_salaire_annuel_suspect
                then true

            else false
        end as salaire_suspect

    from calculs

    cross join parametres

),


finalises as (

    select
        *,

        -- ========================================================
        -- Salaire exploitable pour les KPI
        -- ========================================================

        case
            when salaire_parse_ok is false
                then false

            when periode_salaire_coherente is false
                then false

            when periode_extraite = 'Horaire'
                then false

            when salaire_suspect is true
                then false

            when salaire_annuel_min is null
                then false

            when salaire_annuel_max is null
                then false

            when salaire_annuel_min <= 0
                then false

            when salaire_annuel_max < salaire_annuel_min
                then false

            else true
        end as salaire_exploitable,


        -- ========================================================
        -- Motif d'exclusion des KPI
        -- ========================================================

        case
            when salaire_parse_ok is false
                then 'Format salarial non reconnu'

            when periode_salaire_coherente is false
                then 'Incohérence entre période extraite et période normalisée'

            when periode_extraite = 'Horaire'
                then 'Salaire horaire non annualisé'

            when salaire_suspect is true
                then 'Valeur annualisée atypique supérieure à 200000 EUR'

            when salaire_annuel_min is null
              or salaire_annuel_max is null
                then 'Annualisation impossible'

            when salaire_annuel_min <= 0
                then 'Salaire inférieur ou égal à zéro'

            when salaire_annuel_max < salaire_annuel_min
                then 'Salaire maximum inférieur au salaire minimum'

            else null
        end as motif_non_exploitation,


        -- ========================================================
        -- Méthode de normalisation
        -- ========================================================

        case
            when salaire_parse_ok is false
                then 'Non parsé'

            when periode_extraite = 'Horaire'
                then 'Non annualisé'

            when periode_extraite = 'Annuel'
                then 'Déjà annuel'

            when periode_extraite = 'Mensuel'
                then 'Mensuel x 12'

            else null
        end as methode_normalisation_salaire

    from qualification

)


select
    id_offre,
    intitule_offre,

    famille_metier_data,
    categorie_contrat,
    categorie_temps_travail,

    code_departement,

    -- Donnée salariale source
    salaire_libelle,
    periode_salaire,
    periode_salaire_normalisee,

    -- Parsing
    salaire_parse_ok,
    periode_extraite,
    periode_salaire_coherente,

    salaire_min_brut,
    salaire_max_brut,
    salaire_central_brut,

    format_salaire,
    duree_mois_brute,

    -- Annualisation
    salaire_annuel_min,
    salaire_annuel_max,
    salaire_annuel_central,

    methode_normalisation_salaire,

    -- Qualité
    seuil_salaire_annuel_suspect,
    salaire_suspect,
    salaire_exploitable,
    motif_non_exploitation,

    -- Traçabilité
    batch_id,
    processing_run_id,
    silver_schema_version,
    fichier_source_silver,
    date_chargement

from finalises