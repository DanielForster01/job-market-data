{{ config(materialized='table') }}

with offres as (

    select *
    from {{ ref('int_france_travail__offres_enrichies') }}

),

referentiel_departements as (

    select
        code_departement,
        nom_departement,
        code_region,
        nom_region
    from {{ ref('referentiel_departements') }}

),

offres_enrichies_geo as (

    select
        offres.*,

        coalesce(
            referentiel_departements.code_region,
            'NR'
        ) as code_region_mart,

        coalesce(
            referentiel_departements.nom_region,
            'Non renseigné'
        ) as nom_region_mart

    from offres

    left join referentiel_departements
        on offres.code_departement
        = referentiel_departements.code_departement

),

volume_global as (

    select
        count(distinct id_offre) as nombre_total_offres_global
    from offres_enrichies_geo

),

agregats_region as (

    select
        code_region_mart as code_region,
        nom_region_mart as nom_region,

        -- Traçabilité
        max(batch_id) as batch_id,
        max(fichier_source_silver) as fichier_source_silver,
        max(date_chargement) as date_chargement_batch,

        -- Volume
        count(distinct id_offre)::integer as nombre_offres,

        -- Nombre de départements couverts
        count(
            distinct case
                when code_departement is not null
                    then code_departement
            end
        )::integer as nombre_departements_couverts,

        -- Familles métiers
        count(
            distinct case
                when famille_metier_data = 'Data Engineering'
                    then id_offre
            end
        )::integer as nombre_offres_data_engineering,

        count(
            distinct case
                when famille_metier_data = 'Data Analysis / BI'
                    then id_offre
            end
        )::integer as nombre_offres_data_analysis_bi,

        count(
            distinct case
                when famille_metier_data = 'Data Science / IA'
                    then id_offre
            end
        )::integer as nombre_offres_data_science_ia,

        count(
            distinct case
                when famille_metier_data = 'Autre data / numérique'
                    then id_offre
            end
        )::integer as nombre_offres_autre_data_numerique,

        -- Contrats
        count(
            distinct case
                when categorie_contrat = 'CDI'
                    then id_offre
            end
        )::integer as nombre_offres_cdi,

        count(
            distinct case
                when categorie_contrat = 'CDD / mission'
                    then id_offre
            end
        )::integer as nombre_offres_cdd_mission,

        count(
            distinct case
                when categorie_contrat = 'Alternance'
                    then id_offre
            end
        )::integer as nombre_offres_alternance,

        count(
            distinct case
                when categorie_contrat = 'Indépendant / franchise'
                    then id_offre
            end
        )::integer as nombre_offres_independant_franchise,

        -- Transparence salariale
        count(
            distinct case
                when salaire_renseigne is true
                    then id_offre
            end
        )::integer as nombre_offres_salaire_renseigne,

        count(
            distinct case
                when salaire_renseigne is not true
                    then id_offre
            end
        )::integer as nombre_offres_salaire_non_renseigne,

        -- Cloud / DevOps
        count(
            distinct case
                when mention_cloud_devops is true
                    then id_offre
            end
        )::integer as nombre_offres_mention_cloud_devops,

        -- Tension
        count(
            distinct case
                when offre_difficile_a_pourvoir is true
                    then id_offre
            end
        )::integer as nombre_offres_difficiles_a_pourvoir,

        -- Informations entreprise
        count(
            distinct case
                when information_entreprise_disponible is true
                    then id_offre
            end
        )::integer as nombre_offres_information_entreprise_disponible,

        -- Géolocalisation
        count(
            distinct case
                when coordonnees_renseignees is true
                    then id_offre
            end
        )::integer as nombre_offres_geolocalisees,

        -- Récence
        count(
            distinct case
                when est_offre_recente_30j is true
                    then id_offre
            end
        )::integer as nombre_offres_recentes_30j,

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

        count(
            distinct case
                when categorie_qualite_offre = 'Offre très complète'
                    then id_offre
            end
        )::integer as nombre_offres_tres_completes,

        count(
            distinct case
                when categorie_qualite_offre = 'Offre moyennement complète'
                    then id_offre
            end
        )::integer as nombre_offres_moyennement_completes,

        count(
            distinct case
                when categorie_qualite_offre = 'Offre peu complète'
                    then id_offre
            end
        )::integer as nombre_offres_peu_completes

    from offres_enrichies_geo

    group by
        code_region_mart,
        nom_region_mart

),

kpi_region as (

    select
        current_timestamp as date_calcul_kpi,

        row_number() over (
            order by
                case
                    when agregats_region.code_region = 'NR'
                        then 1
                    else 0
                end,
                agregats_region.nombre_offres desc,
                agregats_region.code_region
        )::integer as rang_region,

        -- Référentiel régional
        agregats_region.code_region,
        agregats_region.nom_region,

        -- Traçabilité
        agregats_region.batch_id,
        agregats_region.fichier_source_silver,
        agregats_region.date_chargement_batch,

        -- Volume
        agregats_region.nombre_offres,

        round(
            (
                100.0
                * agregats_region.nombre_offres
                / nullif(
                    volume_global.nombre_total_offres_global,
                    0
                )
            )::numeric,
            2
        ) as part_offres_pct,

        agregats_region.nombre_departements_couverts,

        -- Data Engineering
        agregats_region.nombre_offres_data_engineering,

        round(
            (
                100.0
                * agregats_region.nombre_offres_data_engineering
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_data_engineering_pct,

        -- Data Analysis / BI
        agregats_region.nombre_offres_data_analysis_bi,

        round(
            (
                100.0
                * agregats_region.nombre_offres_data_analysis_bi
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_data_analysis_bi_pct,

        -- Data Science / IA
        agregats_region.nombre_offres_data_science_ia,

        round(
            (
                100.0
                * agregats_region.nombre_offres_data_science_ia
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_data_science_ia_pct,

        agregats_region.nombre_offres_autre_data_numerique,

        round(
            (
                100.0
                * agregats_region.nombre_offres_autre_data_numerique
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_autre_data_numerique_pct,

        -- CDI
        agregats_region.nombre_offres_cdi,

        round(
            (
                100.0
                * agregats_region.nombre_offres_cdi
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_cdi_pct,

        -- CDD / mission
        agregats_region.nombre_offres_cdd_mission,

        round(
            (
                100.0
                * agregats_region.nombre_offres_cdd_mission
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_cdd_mission_pct,

        -- Alternance
        agregats_region.nombre_offres_alternance,

        round(
            (
                100.0
                * agregats_region.nombre_offres_alternance
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_alternance_pct,

        -- Indépendant
        agregats_region.nombre_offres_independant_franchise,

        round(
            (
                100.0
                * agregats_region.nombre_offres_independant_franchise
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_independant_franchise_pct,

        -- Salaire
        agregats_region.nombre_offres_salaire_renseigne,

        round(
            (
                100.0
                * agregats_region.nombre_offres_salaire_renseigne
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as taux_transparence_salariale_pct,

        agregats_region.nombre_offres_salaire_non_renseigne,

        -- Cloud / DevOps
        agregats_region.nombre_offres_mention_cloud_devops,

        round(
            (
                100.0
                * agregats_region.nombre_offres_mention_cloud_devops
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as taux_mention_cloud_devops_pct,

        -- Tension
        agregats_region.nombre_offres_difficiles_a_pourvoir,

        round(
            (
                100.0
                * agregats_region.nombre_offres_difficiles_a_pourvoir
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as taux_offres_difficiles_a_pourvoir_pct,

        -- Information entreprise
        agregats_region.nombre_offres_information_entreprise_disponible,

        round(
            (
                100.0
                * agregats_region.nombre_offres_information_entreprise_disponible
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as taux_information_entreprise_disponible_pct,

        -- Géolocalisation
        agregats_region.nombre_offres_geolocalisees,

        round(
            (
                100.0
                * agregats_region.nombre_offres_geolocalisees
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as taux_offres_geolocalisees_pct,

        -- Récence
        agregats_region.nombre_offres_recentes_30j,

        round(
            (
                100.0
                * agregats_region.nombre_offres_recentes_30j
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as taux_offres_recentes_30j_pct,

        agregats_region.anciennete_moyenne_offres_jours,

        agregats_region.delai_moyen_actualisation_jours,

        -- Qualité
        agregats_region.score_moyen_qualite_offre,

        agregats_region.nombre_offres_tres_completes,

        round(
            (
                100.0
                * agregats_region.nombre_offres_tres_completes
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_offres_tres_completes_pct,

        agregats_region.nombre_offres_moyennement_completes,

        round(
            (
                100.0
                * agregats_region.nombre_offres_moyennement_completes
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_offres_moyennement_completes_pct,

        agregats_region.nombre_offres_peu_completes,

        round(
            (
                100.0
                * agregats_region.nombre_offres_peu_completes
                / nullif(
                    agregats_region.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_offres_peu_completes_pct

    from agregats_region

    cross join volume_global

)

select *
from kpi_region
order by rang_region
