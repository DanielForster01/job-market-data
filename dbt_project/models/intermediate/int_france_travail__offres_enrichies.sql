{{ config(materialized='table') }}

with offres_staging as (

    select *
    from {{ ref('stg_france_travail__offres') }}

),

dernier_batch as (

    select
        batch_id,
        max(date_chargement) as derniere_date_chargement
    from offres_staging
    where batch_id is not null
      and date_chargement is not null
    group by batch_id
    order by derniere_date_chargement desc
    limit 1

),

offres_du_dernier_chargement as (

    select
        offres_staging.*
    from offres_staging
    inner join dernier_batch
        on offres_staging.batch_id = dernier_batch.batch_id

),

offres_preparees as (

    select
        *,
        lower(
            coalesce(intitule_offre, '')
            || ' '
            || coalesce(description_offre, '')
            || ' '
            || coalesce(mot_cle_recherche, '')
        ) as texte_analyse_offre
    from offres_du_dernier_chargement

),

offres_enrichies as (

    select
        -- Identité de l'offre
        id_offre,
        intitule_offre,
        description_offre,

        -- Dates
        date_creation,
        date_actualisation,
        date_actualisation_coherente,
        date_ingestion,
        date_traitement_silver,
        date_chargement,

        coalesce(date_ingestion::date, date_chargement::date) as date_reference_analyse,

        case
            when date_creation is not null 
             and coalesce(date_ingestion::date, date_chargement::date) is not null
                then greatest(0, coalesce(date_ingestion::date, date_chargement::date) - date_creation::date)
            else null
        end as anciennete_offre_jours,

        case
            when date_creation is not null
             and date_actualisation is not null
                then greatest(0, date_actualisation::date - date_creation::date)
            else null
        end as delai_actualisation_jours,

        case
            when date_creation is not null
             and coalesce(date_ingestion::date, date_chargement::date) is not null
             and date_creation::date >= coalesce(date_ingestion::date, date_chargement::date) - interval '30 days'
                then true
            else false
        end as est_offre_recente_30j,

        -- Localisation
        libelle_lieu_travail,
        latitude,
        longitude,
        code_postal,
        code_commune,

        case
            when code_postal is not null
             and length(trim(code_postal)) >= 2
                then left(trim(code_postal), 2)
            else null
        end as code_departement,

        coordonnees_renseignees,

        case
            when coordonnees_renseignees is true
                then 'Coordonnées renseignées'
            else 'Coordonnées non renseignées'
        end as statut_coordonnees,

        -- Entreprise
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

        -- Contrat
        code_type_contrat,
        libelle_type_contrat,
        nature_contrat,

        case
            when est_alternance is true
                then 'Alternance'
            when code_type_contrat in ('CDI', 'CDI-I')
                then 'CDI'
            when code_type_contrat in ('CDD', 'CDD-I', 'MIS', 'SAI')
                then 'CDD / mission'
            when code_type_contrat in ('APP', 'PRO')
                then 'Alternance'
            when code_type_contrat in ('LIB', 'FRA')
                then 'Indépendant / franchise'
            when code_type_contrat is null
                then 'Non renseigné'
            else 'Autre'
        end as categorie_contrat,

        est_alternance,

        -- Expérience
        experience_exigee,
        libelle_experience,
        commentaire_experience,

        case
            when libelle_experience is null
                then 'Non renseigné'
            when lower(libelle_experience) like '%débutant%'
              or lower(libelle_experience) like '%debutant%'
                then 'Débutant accepté'
            when lower(libelle_experience) like '%an%'
                then 'Expérience demandée'
            else 'Autre'
        end as categorie_experience,

        -- Métier / ROME
        code_rome,
        libelle_rome,
        libelle_appellation,

        case
            when texte_analyse_offre like '%data engineer%'
              or texte_analyse_offre like '%ingénieur data%'
              or texte_analyse_offre like '%ingenieur data%'
              or texte_analyse_offre like '%data ingénieur%'
              or texte_analyse_offre like '%data ingenieur%'
              or texte_analyse_offre like '%analytics engineer%'
                then 'Data Engineering'

            when texte_analyse_offre like '%data analyst%'
              or texte_analyse_offre like '%business analyst%'
              or texte_analyse_offre like '%analyste data%'
              or texte_analyse_offre like '%analyste données%'
              or texte_analyse_offre like '%analyste donnees%'
              or texte_analyse_offre like '%bi%'
              or texte_analyse_offre like '%bi analyst%'
              or texte_analyse_offre like '%business intelligence%'
                then 'Data Analysis / BI'

            when texte_analyse_offre like '%data scientist%'
              or texte_analyse_offre like '%machine learning%'
              or texte_analyse_offre like '%intelligence artificielle%'
              or texte_analyse_offre like '%ia%'
              or texte_analyse_offre like '%ml engineer%'
                then 'Data Science / IA'

            else 'Autre data / numérique'
        end as famille_metier_data,

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
                case when texte_analyse_offre like '%aws%' then 'AWS' end,
                case when texte_analyse_offre like '%azure%' then 'Azure' end,
                case when texte_analyse_offre like '%gcp%' then 'GCP' end,
                case when texte_analyse_offre like '%cloud%' then 'Cloud' end,
                case when texte_analyse_offre like '%devops%' then 'DevOps' end,
                case when texte_analyse_offre like '%docker%' then 'Docker' end,
                case when texte_analyse_offre like '%kubernetes%' then 'Kubernetes' end,
                case when texte_analyse_offre like '%terraform%' then 'Terraform' end,
                case
                    when texte_analyse_offre like '%ci/cd%'
                      or texte_analyse_offre like '%cicd%'
                        then 'CI/CD'
                end
            ),
            ''
        ) as technologies_cloud_devops_detectees,

        -- Salaire
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

        -- Temps de travail
        libelle_duree_travail,
        libelle_duree_travail_converti,

        case
            when lower(coalesce(libelle_duree_travail, '') || ' ' || coalesce(libelle_duree_travail_converti, '')) like '%temps plein%'
                then 'Temps plein'
            when lower(coalesce(libelle_duree_travail, '') || ' ' || coalesce(libelle_duree_travail_converti, '')) like '%temps partiel%'
                then 'Temps partiel'
            when libelle_duree_travail is null
             and libelle_duree_travail_converti is null
                then 'Non renseigné'
            else 'Autre'
        end as categorie_temps_travail,

        -- Qualification / secteur
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

        -- Accessibilité / tension
        offre_difficile_a_pourvoir,
        accessible_travailleur_handicape,
        code_deplacement,
        libelle_deplacement,

        case
            when offre_difficile_a_pourvoir is true
                then 'Offre difficile à pourvoir'
            else 'Offre non signalée difficile'
        end as statut_tension_recrutement,

        -- Informations complémentaires
        complement_exercice,
        origine_offre,
        url_origine_offre,
        horaires_travail,

        -- Recherche / traçabilité
        mot_cle_recherche,
        fichier_source,
        fichier_source_silver,
        batch_id,

        -- Score qualité de l'offre
        (
            case when id_offre is not null then 1 else 0 end
          + case when intitule_offre is not null then 1 else 0 end
          + case when description_offre is not null then 1 else 0 end
          + case when code_type_contrat is not null then 1 else 0 end
          + case when libelle_lieu_travail is not null then 1 else 0 end
          + case when information_entreprise_disponible is true then 1 else 0 end
          + case when salaire_renseigne is true then 1 else 0 end
          + case when coordonnees_renseignees is true then 1 else 0 end
        ) as score_qualite_offre,

        case
            when (
                case when id_offre is not null then 1 else 0 end
              + case when intitule_offre is not null then 1 else 0 end
              + case when description_offre is not null then 1 else 0 end
              + case when code_type_contrat is not null then 1 else 0 end
              + case when libelle_lieu_travail is not null then 1 else 0 end
              + case when information_entreprise_disponible is true then 1 else 0 end
              + case when salaire_renseigne is true then 1 else 0 end
              + case when coordonnees_renseignees is true then 1 else 0 end
            ) >= 7
                then 'Offre très complète'

            when (
                case when id_offre is not null then 1 else 0 end
              + case when intitule_offre is not null then 1 else 0 end
              + case when description_offre is not null then 1 else 0 end
              + case when code_type_contrat is not null then 1 else 0 end
              + case when libelle_lieu_travail is not null then 1 else 0 end
              + case when information_entreprise_disponible is true then 1 else 0 end
              + case when salaire_renseigne is true then 1 else 0 end
              + case when coordonnees_renseignees is true then 1 else 0 end
            ) >= 5
                then 'Offre moyennement complète'

            else 'Offre peu complète'
        end as categorie_qualite_offre

    from offres_preparees

)

select *
from offres_enrichies
