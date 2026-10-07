{{ config(materialized='table') }}

-- ============================================================
-- Modèle : int_france_travail__offres_enrichies
-- ============================================================
--
-- Objectif :
--   - enrichir le snapshot Silver courant des offres ;
--   - produire les dimensions analytiques nécessaires aux marts ;
--   - normaliser les informations de localisation ;
--   - classifier les contrats, métiers et niveaux de qualité ;
--   - détecter les mentions Cloud / DevOps ;
--   - conserver la traçabilité complète du pipeline v2.
--
-- Architecture :
--
--   R2 Silver historique
--       -> PostgreSQL Silver snapshot courant
--       -> dbt staging
--       -> intermediate.int_france_travail__offres_enrichies
--
-- Important :
--   les mots-clés d'acquisition ne déterminent pas directement
--   la famille métier.
--
--   mot_cle_recherche / mots_cles_recherche
--       = COMMENT l'offre a été trouvée
--
--   intitule + description + ROME
--       = CE QU'EST l'offre
--
-- ============================================================


with offres_staging as (

    select *
    from {{ ref('stg_france_travail__offres') }}

),


offres_preparees as (

    select
        *,

        lower(
            coalesce(intitule_offre, '')
            || ' '
            || coalesce(description_offre, '')
            || ' '
            || coalesce(libelle_rome, '')
            || ' '
            || coalesce(libelle_appellation, '')
        ) as texte_analyse_offre

    from offres_staging

),


offres_enrichies as (

    select

        -- ============================================================
        -- IDENTITÉ DE L'OFFRE
        -- ============================================================

        id_offre,
        intitule_offre,
        description_offre,


        -- ============================================================
        -- DATES
        -- ============================================================

        date_creation,
        date_actualisation,
        date_actualisation_coherente,

        date_ingestion,
        date_traitement_silver,
        date_chargement,

        coalesce(
            date_ingestion::date,
            date_chargement::date
        ) as date_reference_analyse,


        case
            when date_creation is not null
             and coalesce(
                    date_ingestion::date,
                    date_chargement::date
                 ) is not null
                then greatest(
                    0,
                    coalesce(
                        date_ingestion::date,
                        date_chargement::date
                    )
                    - date_creation::date
                )

            else null
        end as anciennete_offre_jours,


        case
            when date_creation is not null
             and date_actualisation is not null
                then greatest(
                    0,
                    date_actualisation::date
                    - date_creation::date
                )

            else null
        end as delai_actualisation_jours,


        case
            when date_creation is not null
             and coalesce(
                    date_ingestion::date,
                    date_chargement::date
                 ) is not null
             and date_creation::date
                 >= (
                    coalesce(
                        date_ingestion::date,
                        date_chargement::date
                    ) - 30
                 )
                then true

            else false
        end as est_offre_recente_30j,


        -- ============================================================
        -- LOCALISATION
        -- ============================================================

        libelle_lieu_travail,
        latitude,
        longitude,
        code_postal,
        code_commune,


        -- ============================================================
        -- CODE DÉPARTEMENT NORMALISÉ
        --
        -- Objectif :
        --   ne conserver ici que de véritables départements français.
        --
        -- Les collectivités non départementales comme la
        -- Nouvelle-Calédonie (988) restent présentes dans les données,
        -- mais code_departement reste NULL.
        --
        -- Les pseudo-codes comme 99999 sont également exclus.
        -- ============================================================

        case

            -- --------------------------------------------------------
            -- CORSE : priorité au code commune INSEE
            -- --------------------------------------------------------

            when upper(trim(code_commune::text)) like '2A%'
                then '2A'

            when upper(trim(code_commune::text)) like '2B%'
                then '2B'


            -- --------------------------------------------------------
            -- DÉPARTEMENTS ET RÉGIONS D'OUTRE-MER
            --
            -- 971 : Guadeloupe
            -- 972 : Martinique
            -- 973 : Guyane
            -- 974 : La Réunion
            -- 976 : Mayotte
            -- --------------------------------------------------------

            when trim(code_commune::text) ~ '^97[1-4]'
                then left(
                    trim(code_commune::text),
                    3
                )

            when trim(code_commune::text) ~ '^976'
                then '976'


            -- --------------------------------------------------------
            -- FRANCE MÉTROPOLITAINE
            --
            -- Départements 01 à 95.
            -- La Corse est déjà traitée ci-dessus.
            -- --------------------------------------------------------

            when trim(code_commune::text)
                ~ '^(0[1-9]|[1-8][0-9]|9[0-5])[0-9]{3}$'
                then left(
                    trim(code_commune::text),
                    2
                )


            -- --------------------------------------------------------
            -- FALLBACK DROM À PARTIR DU CODE POSTAL
            -- --------------------------------------------------------

            when trim(code_postal::text) ~ '^97[1-4]'
                then left(
                    trim(code_postal::text),
                    3
                )

            when trim(code_postal::text) ~ '^976'
                then '976'


            -- --------------------------------------------------------
            -- FALLBACK MÉTROPOLE À PARTIR DU CODE POSTAL
            --
            -- Cette règle exclut notamment 99999.
            -- --------------------------------------------------------

            when trim(code_postal::text)
                ~ '^(0[1-9]|[1-8][0-9]|9[0-5])[0-9]{3}$'
                then left(
                    trim(code_postal::text),
                    2
                )


            -- --------------------------------------------------------
            -- FALLBACK CORSE À PARTIR DU LIBELLÉ
            -- --------------------------------------------------------

            when upper(
                trim(libelle_lieu_travail)
            ) ~ '^2A\s*-'
                then '2A'

            when upper(
                trim(libelle_lieu_travail)
            ) ~ '^2B\s*-'
                then '2B'


            -- --------------------------------------------------------
            -- FALLBACK DROM À PARTIR DU LIBELLÉ
            -- --------------------------------------------------------

            when trim(
                libelle_lieu_travail
            ) ~ '^97[1-4]\s*-'
                then substring(
                    trim(libelle_lieu_travail)
                    from '^([0-9]{3})'
                )

            when trim(
                libelle_lieu_travail
            ) ~ '^976\s*-'
                then '976'


            -- --------------------------------------------------------
            -- FALLBACK MÉTROPOLE À PARTIR DU LIBELLÉ
            -- --------------------------------------------------------

            when trim(
                libelle_lieu_travail
            ) ~ '^(0[1-9]|[1-8][0-9]|9[0-5])\s*-'
                then substring(
                    trim(libelle_lieu_travail)
                    from '^([0-9]{2})'
                )


            -- --------------------------------------------------------
            -- Collectivité non départementale,
            -- localisation nationale ou localisation non exploitable.
            -- --------------------------------------------------------

            else null

        end as code_departement,


        coordonnees_renseignees,
        localisation_renseignee,


        case
            when coordonnees_renseignees is true
                then 'Coordonnées renseignées'

            else 'Coordonnées non renseignées'
        end as statut_coordonnees,


        case
            when localisation_renseignee is true
                then 'Localisation renseignée'

            else 'Localisation non renseignée'
        end as statut_localisation,


        -- ============================================================
        -- ENTREPRISE
        -- ============================================================

        nom_entreprise,
        description_entreprise,

        entreprise_renseignee,
        description_entreprise_renseignee,
        information_entreprise_disponible,

        est_entreprise_adaptee,
        est_employeur_handi_engage,


        case
            when information_entreprise_disponible is true
                then 'Information entreprise disponible'

            else 'Information entreprise absente'
        end as statut_information_entreprise,


        -- ============================================================
        -- CONTRAT
        -- ============================================================

        code_type_contrat,
        libelle_type_contrat,
        nature_contrat,


        case

            when est_alternance is true
                then 'Alternance'

            when code_type_contrat in (
                'CDI',
                'CDI-I'
            )
                then 'CDI'

            when code_type_contrat in (
                'CDD',
                'CDD-I',
                'MIS',
                'SAI'
            )
                then 'CDD / mission'

            when code_type_contrat in (
                'APP',
                'PRO'
            )
                then 'Alternance'

            when code_type_contrat in (
                'LIB',
                'FRA'
            )
                then 'Indépendant / franchise'

            when code_type_contrat is null
                then 'Non renseigné'

            else 'Autre'

        end as categorie_contrat,


        est_alternance,


        -- ============================================================
        -- EXPÉRIENCE
        -- ============================================================

        experience_exigee,
        libelle_experience,
        commentaire_experience,


        case

            when libelle_experience is null
                then 'Non renseigné'

            when lower(
                libelle_experience
            ) like '%débutant%'
              or lower(
                  libelle_experience
              ) like '%debutant%'
                then 'Débutant accepté'

            when lower(
                libelle_experience
            ) like '%an%'
                then 'Expérience demandée'

            else 'Autre'

        end as categorie_experience,


        -- ============================================================
        -- MÉTIER / ROME
        -- ============================================================

        code_rome,
        libelle_rome,
        libelle_appellation,


        -- ============================================================
        -- FAMILLE MÉTIER DATA
        --
        -- IMPORTANT :
        --
        -- La classification repose sur :
        --   - l'intitulé ;
        --   - la description ;
        --   - le ROME ;
        --   - l'appellation ROME.
        --
        -- Elle ne repose PAS directement sur le mot-clé utilisé
        -- lors de l'acquisition API.
        -- ============================================================

        case

            -- --------------------------------------------------------
            -- DATA ENGINEERING
            -- --------------------------------------------------------

            when texte_analyse_offre like '%data engineer%'
              or texte_analyse_offre like '%ingénieur data%'
              or texte_analyse_offre like '%ingenieur data%'
              or texte_analyse_offre like '%data ingénieur%'
              or texte_analyse_offre like '%data ingenieur%'
              or texte_analyse_offre like '%analytics engineer%'
                then 'Data Engineering'


            -- --------------------------------------------------------
            -- DATA ANALYSIS / BI
            -- --------------------------------------------------------

            when texte_analyse_offre like '%data analyst%'
              or texte_analyse_offre like '%business analyst%'
              or texte_analyse_offre like '%analyste data%'
              or texte_analyse_offre like '%analyste données%'
              or texte_analyse_offre like '%analyste donnees%'
              or texte_analyse_offre like '%bi analyst%'
              or texte_analyse_offre like '%business intelligence%'
              or texte_analyse_offre
                    ~ '(^|[^[:alnum:]])bi([^[:alnum:]]|$)'
                then 'Data Analysis / BI'


            -- --------------------------------------------------------
            -- DATA SCIENCE / IA
            -- --------------------------------------------------------

            when texte_analyse_offre like '%data scientist%'
              or texte_analyse_offre like '%machine learning%'
              or texte_analyse_offre like '%intelligence artificielle%'
              or texte_analyse_offre like '%ml engineer%'
              or texte_analyse_offre
                    ~ '(^|[^[:alnum:]])ia([^[:alnum:]]|$)'
                then 'Data Science / IA'


            else 'Autre data / numérique'

        end as famille_metier_data,


        -- ============================================================
        -- CLOUD / DEVOPS
        -- ============================================================

        case

            when texte_analyse_offre like '%cloud%'
              or texte_analyse_offre like '%devops%'
              or texte_analyse_offre like '%aws%'
              or texte_analyse_offre like '%azure%'
              or texte_analyse_offre like '%gcp%'
              or texte_analyse_offre like '%docker%'
              or texte_analyse_offre like '%kubernetes%'
              or texte_analyse_offre like '%terraform%'
              or texte_analyse_offre like '%ci/cd%'
              or texte_analyse_offre like '%cicd%'
                then true

            else false

        end as mention_cloud_devops,


        nullif(
            concat_ws(
                ', ',

                case
                    when texte_analyse_offre like '%aws%'
                        then 'AWS'
                end,

                case
                    when texte_analyse_offre like '%azure%'
                        then 'Azure'
                end,

                case
                    when texte_analyse_offre like '%gcp%'
                        then 'GCP'
                end,

                case
                    when texte_analyse_offre like '%cloud%'
                        then 'Cloud'
                end,

                case
                    when texte_analyse_offre like '%devops%'
                        then 'DevOps'
                end,

                case
                    when texte_analyse_offre like '%docker%'
                        then 'Docker'
                end,

                case
                    when texte_analyse_offre like '%kubernetes%'
                        then 'Kubernetes'
                end,

                case
                    when texte_analyse_offre like '%terraform%'
                        then 'Terraform'
                end,

                case
                    when texte_analyse_offre like '%ci/cd%'
                      or texte_analyse_offre like '%cicd%'
                        then 'CI/CD'
                end
            ),
            ''
        ) as technologies_cloud_devops_detectees,


        -- ============================================================
        -- SALAIRE
        -- ============================================================

        salaire_libelle,
        salaire_renseigne,
        periode_salaire,


        case
            when salaire_renseigne is true
                then 'Salaire renseigné'

            else 'Salaire non renseigné'
        end as statut_salaire,


        case
            when periode_salaire is null
                then 'Période non renseignée'

            else periode_salaire
        end as periode_salaire_normalisee,


        -- ============================================================
        -- TEMPS DE TRAVAIL
        -- ============================================================

        libelle_duree_travail,
        libelle_duree_travail_converti,


        case

            when lower(
                coalesce(
                    libelle_duree_travail,
                    ''
                )
                || ' '
                || coalesce(
                    libelle_duree_travail_converti,
                    ''
                )
            ) like '%temps plein%'
                then 'Temps plein'

            when lower(
                coalesce(
                    libelle_duree_travail,
                    ''
                )
                || ' '
                || coalesce(
                    libelle_duree_travail_converti,
                    ''
                )
            ) like '%temps partiel%'
                then 'Temps partiel'

            when libelle_duree_travail is null
             and libelle_duree_travail_converti is null
                then 'Non renseigné'

            else 'Autre'

        end as categorie_temps_travail,


        -- ============================================================
        -- QUALIFICATION / SECTEUR
        -- ============================================================

        code_qualification,
        libelle_qualification,

        code_naf,

        code_secteur_activite,
        libelle_secteur_activite,

        tranche_effectif_etablissement,


        case
            when code_secteur_activite is not null
              or libelle_secteur_activite is not null
                then true

            else false
        end as secteur_activite_renseigne,


        -- ============================================================
        -- ACCESSIBILITÉ / TENSION / DÉPLACEMENT
        -- ============================================================

        offre_difficile_a_pourvoir,
        accessible_travailleur_handicape,

        code_deplacement,
        libelle_deplacement,


        case
            when offre_difficile_a_pourvoir is true
                then 'Offre difficile à pourvoir'

            else 'Offre non signalée difficile'
        end as statut_tension_recrutement,


        -- ============================================================
        -- INFORMATIONS COMPLÉMENTAIRES
        -- ============================================================

        complement_exercice,

        origine_offre,
        url_origine_offre,

        horaires_travail,


        -- ============================================================
        -- ACQUISITION
        --
        -- Ces champs expliquent comment l'offre a été collectée.
        -- Ils ne déterminent pas directement famille_metier_data.
        -- ============================================================

        mot_cle_recherche,
        mots_cles_recherche,


        -- ============================================================
        -- LINEAGE RAW
        -- ============================================================

        fichier_source,
        objet_source_raw,


        -- ============================================================
        -- LINEAGE BRONZE
        -- ============================================================

        bronze_processing_run_id,
        bronze_schema_version,
        objet_source_bronze,


        -- ============================================================
        -- LINEAGE SILVER
        -- ============================================================

        batch_id,
        processing_run_id,
        silver_schema_version,

        fichier_source_silver,


        -- ============================================================
        -- SCORE QUALITÉ
        --
        -- 8 critères :
        --
        --   1. identifiant
        --   2. intitulé
        --   3. description
        --   4. contrat
        --   5. localisation exploitable
        --   6. information entreprise
        --   7. salaire
        --   8. code ROME
        --
        -- Important :
        -- la localisation ne nécessite pas obligatoirement
        -- des coordonnées GPS.
        -- ============================================================

        (
            case
                when id_offre is not null
                    then 1
                else 0
            end

          + case
                when intitule_offre is not null
                    then 1
                else 0
            end

          + case
                when description_offre is not null
                    then 1
                else 0
            end

          + case
                when code_type_contrat is not null
                    then 1
                else 0
            end

          + case
                when localisation_renseignee is true
                    then 1
                else 0
            end

          + case
                when information_entreprise_disponible is true
                    then 1
                else 0
            end

          + case
                when salaire_renseigne is true
                    then 1
                else 0
            end

          + case
                when code_rome is not null
                    then 1
                else 0
            end

        ) as score_qualite_offre,


        case

            when (
                case
                    when id_offre is not null
                        then 1
                    else 0
                end

              + case
                    when intitule_offre is not null
                        then 1
                    else 0
                end

              + case
                    when description_offre is not null
                        then 1
                    else 0
                end

              + case
                    when code_type_contrat is not null
                        then 1
                    else 0
                end

              + case
                    when localisation_renseignee is true
                        then 1
                    else 0
                end

              + case
                    when information_entreprise_disponible is true
                        then 1
                    else 0
                end

              + case
                    when salaire_renseigne is true
                        then 1
                    else 0
                end

              + case
                    when code_rome is not null
                        then 1
                    else 0
                end

            ) >= 7
                then 'Offre très complète'


            when (
                case
                    when id_offre is not null
                        then 1
                    else 0
                end

              + case
                    when intitule_offre is not null
                        then 1
                    else 0
                end

              + case
                    when description_offre is not null
                        then 1
                    else 0
                end

              + case
                    when code_type_contrat is not null
                        then 1
                    else 0
                end

              + case
                    when localisation_renseignee is true
                        then 1
                    else 0
                end

              + case
                    when information_entreprise_disponible is true
                        then 1
                    else 0
                end

              + case
                    when salaire_renseigne is true
                        then 1
                    else 0
                end

              + case
                    when code_rome is not null
                        then 1
                    else 0
                end

            ) >= 5
                then 'Offre moyennement complète'


            else 'Offre peu complète'

        end as categorie_qualite_offre


    from offres_preparees

)


select *
from offres_enrichies