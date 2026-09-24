{{ config(materialized='table') }}

with offres as (

    select *
    from {{ ref('int_france_travail__offres_enrichies') }}

),

volume_global as (

    select
        count(distinct id_offre) as nombre_total_offres_global
    from offres

),

agregats_famille as (

    select
        famille_metier_data,

        -- Traçabilité
        max(batch_id) as batch_id,
        max(fichier_source_silver) as fichier_source_silver,
        max(date_chargement) as date_chargement_batch,

        -- Volume
        count(distinct id_offre)::integer as nombre_offres,

        -- Contrats
        count(distinct case
            when categorie_contrat = 'CDI'
                then id_offre
        end)::integer as nombre_offres_cdi,

        count(distinct case
            when categorie_contrat = 'CDD / mission'
                then id_offre
        end)::integer as nombre_offres_cdd_mission,

        count(distinct case
            when categorie_contrat = 'Alternance'
                then id_offre
        end)::integer as nombre_offres_alternance,

        count(distinct case
            when categorie_contrat = 'Indépendant / franchise'
                then id_offre
        end)::integer as nombre_offres_independant_franchise,

        -- Salaire
        count(distinct case
            when salaire_renseigne is true
                then id_offre
        end)::integer as nombre_offres_salaire_renseigne,

        count(distinct case
            when salaire_renseigne is not true
                then id_offre
        end)::integer as nombre_offres_salaire_non_renseigne,

        -- Signal transversal Cloud / DevOps
        count(distinct case
            when mention_cloud_devops is true
                then id_offre
        end)::integer as nombre_offres_mention_cloud_devops,

        -- Tension de recrutement
        count(distinct case
            when offre_difficile_a_pourvoir is true
                then id_offre
        end)::integer as nombre_offres_difficiles_a_pourvoir,

        -- Information entreprise
        count(distinct case
            when information_entreprise_disponible is true
                then id_offre
        end)::integer as nombre_offres_information_entreprise_disponible,

        -- Géolocalisation
        count(distinct case
            when coordonnees_renseignees is true
                then id_offre
        end)::integer as nombre_offres_geolocalisees,

        count(distinct code_departement)::integer as nombre_departements_couverts,

        -- Récence
        count(distinct case
            when est_offre_recente_30j is true
                then id_offre
        end)::integer as nombre_offres_recentes_30j,

        -- Qualité
        round(avg(score_qualite_offre)::numeric, 2) as score_moyen_qualite_offre,

        count(distinct case
            when categorie_qualite_offre = 'Offre très complète'
                then id_offre
        end)::integer as nombre_offres_tres_completes,

        count(distinct case
            when categorie_qualite_offre = 'Offre moyennement complète'
                then id_offre
        end)::integer as nombre_offres_moyennement_completes,

        count(distinct case
            when categorie_qualite_offre = 'Offre peu complète'
                then id_offre
        end)::integer as nombre_offres_peu_completes,

        -- Ancienneté / actualisation
        round(avg(anciennete_offre_jours)::numeric, 2) as anciennete_moyenne_offres_jours,
        round(avg(delai_actualisation_jours)::numeric, 2) as delai_moyen_actualisation_jours

    from offres
    group by famille_metier_data

),

kpi_famille as (

    select
        current_timestamp as date_calcul_kpi,

        row_number() over (
            order by agregats_famille.nombre_offres desc
        )::integer as rang_famille_metier,

        agregats_famille.famille_metier_data,

        agregats_famille.batch_id,
        agregats_famille.fichier_source_silver,
        agregats_famille.date_chargement_batch,

        agregats_famille.nombre_offres,

        round(
            (
                100.0
                * agregats_famille.nombre_offres
                / nullif(volume_global.nombre_total_offres_global, 0)
            )::numeric,
            2
        ) as part_offres_pct,

        -- Contrats
        agregats_famille.nombre_offres_cdi,

        round(
            (
                100.0
                * agregats_famille.nombre_offres_cdi
                / nullif(agregats_famille.nombre_offres, 0)
            )::numeric,
            2
        ) as part_cdi_dans_famille_pct,

        agregats_famille.nombre_offres_cdd_mission,

        round(
            (
                100.0
                * agregats_famille.nombre_offres_cdd_mission
                / nullif(agregats_famille.nombre_offres, 0)
            )::numeric,
            2
        ) as part_cdd_mission_dans_famille_pct,

        agregats_famille.nombre_offres_alternance,

        round(
            (
                100.0
                * agregats_famille.nombre_offres_alternance
                / nullif(agregats_famille.nombre_offres, 0)
            )::numeric,
            2
        ) as part_alternance_dans_famille_pct,

        agregats_famille.nombre_offres_independant_franchise,

        round(
            (
                100.0
                * agregats_famille.nombre_offres_independant_franchise
                / nullif(agregats_famille.nombre_offres, 0)
            )::numeric,
            2
        ) as part_independant_franchise_dans_famille_pct,

        -- Salaire
        agregats_famille.nombre_offres_salaire_renseigne,

        round(
            (
                100.0
                * agregats_famille.nombre_offres_salaire_renseigne
                / nullif(agregats_famille.nombre_offres, 0)
            )::numeric,
            2
        ) as taux_transparence_salariale_pct,

        agregats_famille.nombre_offres_salaire_non_renseigne,

        -- Cloud / DevOps transversal
        agregats_famille.nombre_offres_mention_cloud_devops,

        round(
            (
                100.0
                * agregats_famille.nombre_offres_mention_cloud_devops
                / nullif(agregats_famille.nombre_offres, 0)
            )::numeric,
            2
        ) as taux_mention_cloud_devops_pct,

        -- Tension
        agregats_famille.nombre_offres_difficiles_a_pourvoir,

        round(
            (
                100.0
                * agregats_famille.nombre_offres_difficiles_a_pourvoir
                / nullif(agregats_famille.nombre_offres, 0)
            )::numeric,
            2
        ) as taux_offres_difficiles_a_pourvoir_pct,

        -- Entreprise
        agregats_famille.nombre_offres_information_entreprise_disponible,

        round(
            (
                100.0
                * agregats_famille.nombre_offres_information_entreprise_disponible
                / nullif(agregats_famille.nombre_offres, 0)
            )::numeric,
            2
        ) as taux_information_entreprise_disponible_pct,

        -- Géolocalisation
        agregats_famille.nombre_offres_geolocalisees,

        round(
            (
                100.0
                * agregats_famille.nombre_offres_geolocalisees
                / nullif(agregats_famille.nombre_offres, 0)
            )::numeric,
            2
        ) as taux_offres_geolocalisees_pct,

        agregats_famille.nombre_departements_couverts,

        -- Récence
        agregats_famille.nombre_offres_recentes_30j,

        round(
            (
                100.0
                * agregats_famille.nombre_offres_recentes_30j
                / nullif(agregats_famille.nombre_offres, 0)
            )::numeric,
            2
        ) as taux_offres_recentes_30j_pct,

        agregats_famille.anciennete_moyenne_offres_jours,
        agregats_famille.delai_moyen_actualisation_jours,

        -- Qualité
        agregats_famille.score_moyen_qualite_offre,

        agregats_famille.nombre_offres_tres_completes,

        round(
            (
                100.0
                * agregats_famille.nombre_offres_tres_completes
                / nullif(agregats_famille.nombre_offres, 0)
            )::numeric,
            2
        ) as part_offres_tres_completes_pct,

        agregats_famille.nombre_offres_moyennement_completes,

        round(
            (
                100.0
                * agregats_famille.nombre_offres_moyennement_completes
                / nullif(agregats_famille.nombre_offres, 0)
            )::numeric,
            2
        ) as part_offres_moyennement_completes_pct,

        agregats_famille.nombre_offres_peu_completes,

        round(
            (
                100.0
                * agregats_famille.nombre_offres_peu_completes
                / nullif(agregats_famille.nombre_offres, 0)
            )::numeric,
            2
        ) as part_offres_peu_completes_pct

    from agregats_famille
    cross join volume_global

)

select *
from kpi_famille
order by nombre_offres desc
