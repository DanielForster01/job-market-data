{{ config(materialized='table') }}

with offres as (

    select *
    from {{ ref('int_france_travail__offres_enrichies') }}

),

kpi_marche as (

    select
        -- Traçabilité du calcul
        current_timestamp as date_calcul_kpi,
        max(batch_id) as batch_id,
        max(fichier_source_silver) as fichier_source_silver,
        max(date_chargement) as date_chargement_batch,

        -- Volume global
        count(distinct id_offre)::integer as nombre_total_offres,

        -- Familles métiers data principales
        count(distinct case
            when famille_metier_data = 'Data Engineering'
                then id_offre
        end)::integer as nombre_offres_data_engineering,

        round(
            (
                100.0
                * count(distinct case
                    when famille_metier_data = 'Data Engineering'
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as part_data_engineering_pct,

        count(distinct case
            when famille_metier_data = 'Data Analysis / BI'
                then id_offre
        end)::integer as nombre_offres_data_analysis_bi,

        round(
            (
                100.0
                * count(distinct case
                    when famille_metier_data = 'Data Analysis / BI'
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as part_data_analysis_bi_pct,

        count(distinct case
            when famille_metier_data = 'Data Science / IA'
                then id_offre
        end)::integer as nombre_offres_data_science_ia,

        round(
            (
                100.0
                * count(distinct case
                    when famille_metier_data = 'Data Science / IA'
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as part_data_science_ia_pct,

        count(distinct case
            when famille_metier_data = 'Autre data / numérique'
                then id_offre
        end)::integer as nombre_offres_autre_data_numerique,

        round(
            (
                100.0
                * count(distinct case
                    when famille_metier_data = 'Autre data / numérique'
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as part_autre_data_numerique_pct,

        -- Signal transversal Cloud / DevOps
        -- Ce n'est pas une famille métier exclusive.
        -- Une offre peut être Data Engineering et mentionner AWS, Azure, Docker, Kubernetes, etc.
        count(distinct case
            when mention_cloud_devops is true
                then id_offre
        end)::integer as nombre_offres_mention_cloud_devops,

        round(
            (
                100.0
                * count(distinct case
                    when mention_cloud_devops is true
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as taux_offres_mention_cloud_devops_pct,

        -- Types de contrat
        count(distinct case
            when categorie_contrat = 'CDI'
                then id_offre
        end)::integer as nombre_offres_cdi,

        round(
            (
                100.0
                * count(distinct case
                    when categorie_contrat = 'CDI'
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as part_cdi_pct,

        count(distinct case
            when categorie_contrat = 'CDD / mission'
                then id_offre
        end)::integer as nombre_offres_cdd_mission,

        round(
            (
                100.0
                * count(distinct case
                    when categorie_contrat = 'CDD / mission'
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as part_cdd_mission_pct,

        count(distinct case
            when categorie_contrat = 'Alternance'
                then id_offre
        end)::integer as nombre_offres_alternance,

        round(
            (
                100.0
                * count(distinct case
                    when categorie_contrat = 'Alternance'
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as part_alternance_pct,

        count(distinct case
            when categorie_contrat = 'Indépendant / franchise'
                then id_offre
        end)::integer as nombre_offres_independant_franchise,

        round(
            (
                100.0
                * count(distinct case
                    when categorie_contrat = 'Indépendant / franchise'
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as part_independant_franchise_pct,

        count(distinct case
            when categorie_contrat in ('Autre', 'Non renseigné')
                then id_offre
        end)::integer as nombre_offres_autres_contrats,

        round(
            (
                100.0
                * count(distinct case
                    when categorie_contrat in ('Autre', 'Non renseigné')
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as part_autres_contrats_pct,

        -- Salaire / transparence
        count(distinct case
            when salaire_renseigne is true
                then id_offre
        end)::integer as nombre_offres_salaire_renseigne,

        round(
            (
                100.0
                * count(distinct case
                    when salaire_renseigne is true
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as taux_transparence_salariale_pct,

        count(distinct case
            when salaire_renseigne is not true
                then id_offre
        end)::integer as nombre_offres_salaire_non_renseigne,

        round(
            (
                100.0
                * count(distinct case
                    when salaire_renseigne is not true
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as taux_salaire_non_renseigne_pct,

        -- Tension de recrutement
        count(distinct case
            when offre_difficile_a_pourvoir is true
                then id_offre
        end)::integer as nombre_offres_difficiles_a_pourvoir,

        round(
            (
                100.0
                * count(distinct case
                    when offre_difficile_a_pourvoir is true
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as taux_offres_difficiles_a_pourvoir_pct,

        count(distinct case
            when offre_difficile_a_pourvoir is not true
                then id_offre
        end)::integer as nombre_offres_non_signalees_difficiles,

        -- Information entreprise
        count(distinct case
            when information_entreprise_disponible is true
                then id_offre
        end)::integer as nombre_offres_information_entreprise_disponible,

        round(
            (
                100.0
                * count(distinct case
                    when information_entreprise_disponible is true
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as taux_information_entreprise_disponible_pct,

        count(distinct case
            when information_entreprise_disponible is not true
                then id_offre
        end)::integer as nombre_offres_information_entreprise_absente,

        round(
            (
                100.0
                * count(distinct case
                    when information_entreprise_disponible is not true
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as taux_information_entreprise_absente_pct,

        -- Géolocalisation
        count(distinct case
            when coordonnees_renseignees is true
                then id_offre
        end)::integer as nombre_offres_geolocalisees,

        round(
            (
                100.0
                * count(distinct case
                    when coordonnees_renseignees is true
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as taux_offres_geolocalisees_pct,

        count(distinct case
            when coordonnees_renseignees is not true
                then id_offre
        end)::integer as nombre_offres_non_geolocalisees,

        round(
            (
                100.0
                * count(distinct case
                    when coordonnees_renseignees is not true
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as taux_offres_non_geolocalisees_pct,

        count(distinct code_departement)::integer as nombre_departements_couverts,

        -- Récence des offres
        count(distinct case
            when est_offre_recente_30j is true
                then id_offre
        end)::integer as nombre_offres_recentes_30j,

        round(
            (
                100.0
                * count(distinct case
                    when est_offre_recente_30j is true
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as part_offres_recentes_30j_pct,

        round(avg(anciennete_offre_jours)::numeric, 2) as anciennete_moyenne_offres_jours,

        round(avg(delai_actualisation_jours)::numeric, 2) as delai_moyen_actualisation_jours,

        -- Qualité / complétude des offres
        round(avg(score_qualite_offre)::numeric, 2) as score_moyen_qualite_offre,

        count(distinct case
            when categorie_qualite_offre = 'Offre très complète'
                then id_offre
        end)::integer as nombre_offres_tres_completes,

        round(
            (
                100.0
                * count(distinct case
                    when categorie_qualite_offre = 'Offre très complète'
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as part_offres_tres_completes_pct,

        count(distinct case
            when categorie_qualite_offre = 'Offre moyennement complète'
                then id_offre
        end)::integer as nombre_offres_moyennement_completes,

        round(
            (
                100.0
                * count(distinct case
                    when categorie_qualite_offre = 'Offre moyennement complète'
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as part_offres_moyennement_completes_pct,

        count(distinct case
            when categorie_qualite_offre = 'Offre peu complète'
                then id_offre
        end)::integer as nombre_offres_peu_completes,

        round(
            (
                100.0
                * count(distinct case
                    when categorie_qualite_offre = 'Offre peu complète'
                        then id_offre
                end)
                / nullif(count(distinct id_offre), 0)
            )::numeric,
            2
        ) as part_offres_peu_completes_pct

    from offres

)

select *
from kpi_marche