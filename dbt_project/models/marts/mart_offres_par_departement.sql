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
            offres.code_departement,
            'NON_RENSEIGNE'
        ) as code_departement_mart,

        coalesce(
            referentiel_departements.nom_departement,
            'Non renseigné'
        ) as nom_departement_mart,

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

agregats_departement as (

    select
        code_departement_mart as code_departement,
        nom_departement_mart as nom_departement,
        code_region_mart as code_region,
        nom_region_mart as nom_region,

        -- Traçabilité
        max(batch_id) as batch_id,
        max(fichier_source_silver) as fichier_source_silver,
        max(date_chargement) as date_chargement_batch,

        -- Volume
        count(distinct id_offre)::integer as nombre_offres,

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

        -- Salaire
        count(
            distinct case
                when salaire_renseigne is true
                    then id_offre
            end
        )::integer as nombre_offres_salaire_renseigne,

        -- Cloud / DevOps
        count(
            distinct case
                when mention_cloud_devops is true
                    then id_offre
            end
        )::integer as nombre_offres_mention_cloud_devops,

        -- Tension de recrutement
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
        code_departement_mart,
        nom_departement_mart,
        code_region_mart,
        nom_region_mart

),

kpi_departement as (

    select
        current_timestamp as date_calcul_kpi,

        row_number() over (
            order by
                case
                    when agregats_departement.code_departement = 'NON_RENSEIGNE'
                        then 1
                    else 0
                end,
                agregats_departement.nombre_offres desc,
                agregats_departement.code_departement
        )::integer as rang_departement,

        -- Référentiel géographique
        agregats_departement.code_departement,
        agregats_departement.nom_departement,
        agregats_departement.code_region,
        agregats_departement.nom_region,

        -- Traçabilité
        agregats_departement.batch_id,
        agregats_departement.fichier_source_silver,
        agregats_departement.date_chargement_batch,

        -- Volume
        agregats_departement.nombre_offres,

        round(
            (
                100.0
                * agregats_departement.nombre_offres
                / nullif(
                    volume_global.nombre_total_offres_global,
                    0
                )
            )::numeric,
            2
        ) as part_offres_pct,

        -- Data Engineering
        agregats_departement.nombre_offres_data_engineering,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_data_engineering
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_data_engineering_pct,

        -- Data Analysis / BI
        agregats_departement.nombre_offres_data_analysis_bi,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_data_analysis_bi
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_data_analysis_bi_pct,

        -- Data Science / IA
        agregats_departement.nombre_offres_data_science_ia,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_data_science_ia
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_data_science_ia_pct,

        agregats_departement.nombre_offres_autre_data_numerique,

        -- CDI
        agregats_departement.nombre_offres_cdi,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_cdi
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_cdi_pct,

        -- CDD / mission
        agregats_departement.nombre_offres_cdd_mission,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_cdd_mission
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_cdd_mission_pct,

        -- Alternance
        agregats_departement.nombre_offres_alternance,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_alternance
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_alternance_pct,

        -- Indépendant / franchise
        agregats_departement.nombre_offres_independant_franchise,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_independant_franchise
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_independant_franchise_pct,

        -- Transparence salariale
        agregats_departement.nombre_offres_salaire_renseigne,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_salaire_renseigne
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as taux_transparence_salariale_pct,

        -- Cloud / DevOps
        agregats_departement.nombre_offres_mention_cloud_devops,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_mention_cloud_devops
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as taux_mention_cloud_devops_pct,

        -- Tension
        agregats_departement.nombre_offres_difficiles_a_pourvoir,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_difficiles_a_pourvoir
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as taux_offres_difficiles_a_pourvoir_pct,

        -- Information entreprise
        agregats_departement.nombre_offres_information_entreprise_disponible,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_information_entreprise_disponible
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as taux_information_entreprise_disponible_pct,

        -- Géolocalisation
        agregats_departement.nombre_offres_geolocalisees,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_geolocalisees
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as taux_offres_geolocalisees_pct,

        -- Récence
        agregats_departement.nombre_offres_recentes_30j,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_recentes_30j
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as taux_offres_recentes_30j_pct,

        agregats_departement.anciennete_moyenne_offres_jours,

        agregats_departement.delai_moyen_actualisation_jours,

        -- Qualité
        agregats_departement.score_moyen_qualite_offre,

        agregats_departement.nombre_offres_tres_completes,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_tres_completes
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_offres_tres_completes_pct,

        agregats_departement.nombre_offres_moyennement_completes,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_moyennement_completes
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_offres_moyennement_completes_pct,

        agregats_departement.nombre_offres_peu_completes,

        round(
            (
                100.0
                * agregats_departement.nombre_offres_peu_completes
                / nullif(
                    agregats_departement.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as part_offres_peu_completes_pct

    from agregats_departement

    cross join volume_global

)

select *
from kpi_departement
order by rang_departement