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

agregats_contrat as (

    select
        categorie_contrat,

        -- Traçabilité
        max(batch_id) as batch_id,
        max(fichier_source_silver) as fichier_source_silver,
        max(date_chargement) as date_chargement_batch,

        -- Volume
        count(distinct id_offre)::integer as nombre_offres,

        -- Répartition par famille métier
        count(distinct case
            when famille_metier_data = 'Data Engineering'
                then id_offre
        end)::integer as nombre_offres_data_engineering,

        count(distinct case
            when famille_metier_data = 'Data Analysis / BI'
                then id_offre
        end)::integer as nombre_offres_data_analysis_bi,

        count(distinct case
            when famille_metier_data = 'Data Science / IA'
                then id_offre
        end)::integer as nombre_offres_data_science_ia,

        count(distinct case
            when famille_metier_data = 'Autre data / numérique'
                then id_offre
        end)::integer as nombre_offres_autre_data_numerique,

        -- Salaire
        count(distinct case
            when salaire_renseigne is true
                then id_offre
        end)::integer as nombre_offres_salaire_renseigne,

        count(distinct case
            when salaire_renseigne is not true
                then id_offre
        end)::integer as nombre_offres_salaire_non_renseigne,

        -- Cloud / DevOps
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

        round(
            avg(anciennete_offre_jours)::numeric,
            2
        ) as anciennete_moyenne_offres_jours,

        round(
            avg(delai_actualisation_jours)::numeric,
            2
        ) as delai_moyen_actualisation_jours,

        -- Qualité
        round(
            avg(score_qualite_offre)::numeric,
            2
        ) as score_moyen_qualite_offre,

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
        end)::integer as nombre_offres_peu_completes

    from offres
    group by categorie_contrat

),

kpi_contrat as (

    select
        current_timestamp as date_calcul_kpi,

        row_number() over (
            order by agregats_contrat.nombre_offres desc
        )::integer as rang_type_contrat,

        agregats_contrat.categorie_contrat,

        agregats_contrat.batch_id,
        agregats_contrat.fichier_source_silver,
        agregats_contrat.date_chargement_batch,

        -- Volume
        agregats_contrat.nombre_offres,

        round(
            (
                100.0
                * agregats_contrat.nombre_offres
                / nullif(volume_global.nombre_total_offres_global, 0)
            )::numeric,
            2
        ) as part_offres_pct,

        -- Data Engineering
        agregats_contrat.nombre_offres_data_engineering,

        round(
            (
                100.0
                * agregats_contrat.nombre_offres_data_engineering
                / nullif(agregats_contrat.nombre_offres, 0)
            )::numeric,
            2
        ) as part_data_engineering_dans_contrat_pct,

        -- Data Analysis / BI
        agregats_contrat.nombre_offres_data_analysis_bi,

        round(
            (
                100.0
                * agregats_contrat.nombre_offres_data_analysis_bi
                / nullif(agregats_contrat.nombre_offres, 0)
            )::numeric,
            2
        ) as part_data_analysis_bi_dans_contrat_pct,

        -- Data Science / IA
        agregats_contrat.nombre_offres_data_science_ia,

        round(
            (
                100.0
                * agregats_contrat.nombre_offres_data_science_ia
                / nullif(agregats_contrat.nombre_offres, 0)
            )::numeric,
            2
        ) as part_data_science_ia_dans_contrat_pct,

        -- Autres métiers data
        agregats_contrat.nombre_offres_autre_data_numerique,

        round(
            (
                100.0
                * agregats_contrat.nombre_offres_autre_data_numerique
                / nullif(agregats_contrat.nombre_offres, 0)
            )::numeric,
            2
        ) as part_autre_data_numerique_dans_contrat_pct,

        -- Transparence salariale
        agregats_contrat.nombre_offres_salaire_renseigne,

        round(
            (
                100.0
                * agregats_contrat.nombre_offres_salaire_renseigne
                / nullif(agregats_contrat.nombre_offres, 0)
            )::numeric,
            2
        ) as taux_transparence_salariale_pct,

        agregats_contrat.nombre_offres_salaire_non_renseigne,

        -- Cloud / DevOps
        agregats_contrat.nombre_offres_mention_cloud_devops,

        round(
            (
                100.0
                * agregats_contrat.nombre_offres_mention_cloud_devops
                / nullif(agregats_contrat.nombre_offres, 0)
            )::numeric,
            2
        ) as taux_mention_cloud_devops_pct,

        -- Tension
        agregats_contrat.nombre_offres_difficiles_a_pourvoir,

        round(
            (
                100.0
                * agregats_contrat.nombre_offres_difficiles_a_pourvoir
                / nullif(agregats_contrat.nombre_offres, 0)
            )::numeric,
            2
        ) as taux_offres_difficiles_a_pourvoir_pct,

        -- Informations entreprise
        agregats_contrat.nombre_offres_information_entreprise_disponible,

        round(
            (
                100.0
                * agregats_contrat.nombre_offres_information_entreprise_disponible
                / nullif(agregats_contrat.nombre_offres, 0)
            )::numeric,
            2
        ) as taux_information_entreprise_disponible_pct,

        -- Géolocalisation
        agregats_contrat.nombre_offres_geolocalisees,

        round(
            (
                100.0
                * agregats_contrat.nombre_offres_geolocalisees
                / nullif(agregats_contrat.nombre_offres, 0)
            )::numeric,
            2
        ) as taux_offres_geolocalisees_pct,

        agregats_contrat.nombre_departements_couverts,

        -- Récence
        agregats_contrat.nombre_offres_recentes_30j,

        round(
            (
                100.0
                * agregats_contrat.nombre_offres_recentes_30j
                / nullif(agregats_contrat.nombre_offres, 0)
            )::numeric,
            2
        ) as taux_offres_recentes_30j_pct,

        agregats_contrat.anciennete_moyenne_offres_jours,
        agregats_contrat.delai_moyen_actualisation_jours,

        -- Qualité
        agregats_contrat.score_moyen_qualite_offre,

        agregats_contrat.nombre_offres_tres_completes,

        round(
            (
                100.0
                * agregats_contrat.nombre_offres_tres_completes
                / nullif(agregats_contrat.nombre_offres, 0)
            )::numeric,
            2
        ) as part_offres_tres_completes_pct,

        agregats_contrat.nombre_offres_moyennement_completes,

        round(
            (
                100.0
                * agregats_contrat.nombre_offres_moyennement_completes
                / nullif(agregats_contrat.nombre_offres, 0)
            )::numeric,
            2
        ) as part_offres_moyennement_completes_pct,

        agregats_contrat.nombre_offres_peu_completes,

        round(
            (
                100.0
                * agregats_contrat.nombre_offres_peu_completes
                / nullif(agregats_contrat.nombre_offres, 0)
            )::numeric,
            2
        ) as part_offres_peu_completes_pct

    from agregats_contrat
    cross join volume_global

)

select *
from kpi_contrat
order by nombre_offres desc
