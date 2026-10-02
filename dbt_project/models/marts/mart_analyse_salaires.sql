{{ config(materialized='table') }}

-- ============================================================
-- Mart : mart_analyse_salaires
-- ============================================================
--
-- Objectif :
-- fournir une vue analytique unifiée des salaires selon :
--
--   1. Global
--   2. Famille métier
--   3. Type de contrat
--   4. Région
--
-- Le mart conserve comme dénominateur l'ensemble des offres,
-- y compris celles sans salaire renseigné.
--
-- Les statistiques salariales (moyenne, médiane, quartiles...)
-- sont calculées UNIQUEMENT sur les salaires pour lesquels
-- salaire_exploitable = true.
--
-- Grain :
-- une ligne par axe d'analyse / modalité.
-- ============================================================


with offres as (

    select
        id_offre,

        famille_metier_data,
        categorie_contrat,

        code_departement,

        salaire_renseigne,

        batch_id,
        fichier_source_silver,
        date_chargement

    from {{ ref('int_france_travail__offres_enrichies') }}

),


salaires as (

    select
        id_offre,

        salaire_parse_ok,
        salaire_suspect,
        salaire_exploitable,
        motif_non_exploitation,

        salaire_annuel_min,
        salaire_annuel_max,
        salaire_annuel_central

    from {{ ref('int_france_travail__salaires_normalises') }}

),


referentiel_departements as (

    select
        code_departement,
        code_region,
        nom_region

    from {{ ref('referentiel_departements') }}

),


base as (

    select
        offres.id_offre,

        offres.famille_metier_data,
        offres.categorie_contrat,

        offres.code_departement,

        coalesce(
            referentiel_departements.code_region,
            'NR'
        ) as code_region,

        coalesce(
            referentiel_departements.nom_region,
            'Non renseigné'
        ) as nom_region,

        offres.salaire_renseigne,

        coalesce(
            salaires.salaire_parse_ok,
            false
        ) as salaire_parse_ok,

        coalesce(
            salaires.salaire_suspect,
            false
        ) as salaire_suspect,

        coalesce(
            salaires.salaire_exploitable,
            false
        ) as salaire_exploitable,

        salaires.motif_non_exploitation,

        salaires.salaire_annuel_min,
        salaires.salaire_annuel_max,
        salaires.salaire_annuel_central,

        offres.batch_id,
        offres.fichier_source_silver,
        offres.date_chargement

    from offres

    left join salaires
        on offres.id_offre = salaires.id_offre

    left join referentiel_departements
        on offres.code_departement
        = referentiel_departements.code_departement

),


-- ============================================================
-- Transformation du dataset en format analytique "long".
--
-- Chaque offre apparaît une fois dans chacun des axes :
--
-- Global
-- Famille métier
-- Type de contrat
-- Région
-- ============================================================

axes as (

    -- --------------------------------------------------------
    -- GLOBAL
    -- --------------------------------------------------------

    select
        base.*,

        1 as ordre_axe,

        'Global'::text as axe_analyse,

        'GLOBAL'::text as code_modalite,

        'Ensemble'::text as modalite

    from base


    union all


    -- --------------------------------------------------------
    -- FAMILLE MÉTIER
    -- --------------------------------------------------------

    select
        base.*,

        2 as ordre_axe,

        'Famille métier'::text as axe_analyse,

        coalesce(
            famille_metier_data,
            'NON_RENSEIGNE'
        )::text as code_modalite,

        coalesce(
            famille_metier_data,
            'Non renseigné'
        )::text as modalite

    from base


    union all


    -- --------------------------------------------------------
    -- TYPE DE CONTRAT
    -- --------------------------------------------------------

    select
        base.*,

        3 as ordre_axe,

        'Type de contrat'::text as axe_analyse,

        coalesce(
            categorie_contrat,
            'NON_RENSEIGNE'
        )::text as code_modalite,

        coalesce(
            categorie_contrat,
            'Non renseigné'
        )::text as modalite

    from base


    union all


    -- --------------------------------------------------------
    -- RÉGION
    -- --------------------------------------------------------

    select
        base.*,

        4 as ordre_axe,

        'Région'::text as axe_analyse,

        code_region::text as code_modalite,

        nom_region::text as modalite

    from base

),


agregats as (

    select
        ordre_axe,
        axe_analyse,
        code_modalite,
        modalite,

        -- =====================================================
        -- Traçabilité
        -- =====================================================

        max(batch_id) as batch_id,

        max(fichier_source_silver)
            as fichier_source_silver,

        max(date_chargement)
            as date_chargement_batch,


        -- =====================================================
        -- Volume d'offres
        -- =====================================================

        count(
            distinct id_offre
        )::integer as nombre_offres,


        -- =====================================================
        -- Transparence salariale
        -- =====================================================

        count(
            distinct case
                when salaire_renseigne is true
                    then id_offre
            end
        )::integer as nombre_salaires_renseignes,


        -- =====================================================
        -- Salaires suspects
        -- =====================================================

        count(
            distinct case
                when salaire_suspect is true
                    then id_offre
            end
        )::integer as nombre_salaires_suspects,


        -- =====================================================
        -- Salaires exploitables
        -- =====================================================

        count(
            distinct case
                when salaire_exploitable is true
                    then id_offre
            end
        )::integer as nombre_salaires_exploitables,


        -- =====================================================
        -- Salaires non exploitables
        -- =====================================================

        count(
            distinct case
                when salaire_renseigne is true
                 and salaire_exploitable is false
                    then id_offre
            end
        )::integer as nombre_salaires_non_exploitables,


        -- =====================================================
        -- Salaire annuel moyen
        -- =====================================================

        round(
            avg(
                salaire_annuel_central
            ) filter (
                where salaire_exploitable is true
            ),
            2
        ) as salaire_annuel_moyen,


        -- =====================================================
        -- Salaire annuel médian
        -- =====================================================

        round(
            (
                percentile_cont(0.50)
                within group (
                    order by salaire_annuel_central
                )
                filter (
                    where salaire_exploitable is true
                )
            )::numeric,
            2
        ) as salaire_annuel_median,


        -- =====================================================
        -- Premier quartile
        -- =====================================================

        round(
            (
                percentile_cont(0.25)
                within group (
                    order by salaire_annuel_central
                )
                filter (
                    where salaire_exploitable is true
                )
            )::numeric,
            2
        ) as premier_quartile,


        -- =====================================================
        -- Troisième quartile
        -- =====================================================

        round(
            (
                percentile_cont(0.75)
                within group (
                    order by salaire_annuel_central
                )
                filter (
                    where salaire_exploitable is true
                )
            )::numeric,
            2
        ) as troisieme_quartile,


        -- =====================================================
        -- Minimum / maximum observés
        --
        -- Ces valeurs portent sur salaire_annuel_central.
        -- =====================================================

        min(
            salaire_annuel_central
        ) filter (
            where salaire_exploitable is true
        ) as salaire_annuel_min_observe,


        max(
            salaire_annuel_central
        ) filter (
            where salaire_exploitable is true
        ) as salaire_annuel_max_observe

    from axes

    group by
        ordre_axe,
        axe_analyse,
        code_modalite,
        modalite

),


volume_global as (

    select
        nombre_offres as nombre_total_offres

    from agregats

    where axe_analyse = 'Global'
      and code_modalite = 'GLOBAL'

),


kpi as (

    select
        current_timestamp as date_calcul_kpi,

        agregats.ordre_axe,
        agregats.axe_analyse,

        row_number() over (
            partition by agregats.axe_analyse
            order by
                agregats.nombre_offres desc,
                agregats.modalite
        )::integer as rang_modalite,

        agregats.code_modalite,
        agregats.modalite,

        -- Traçabilité
        agregats.batch_id,
        agregats.fichier_source_silver,
        agregats.date_chargement_batch,

        -- Volume
        agregats.nombre_offres,

        round(
            (
                100.0
                * agregats.nombre_offres
                / nullif(
                    volume_global.nombre_total_offres,
                    0
                )
            )::numeric,
            2
        ) as part_offres_pct,

        -- Salaires renseignés
        agregats.nombre_salaires_renseignes,

        round(
            (
                100.0
                * agregats.nombre_salaires_renseignes
                / nullif(
                    agregats.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as taux_transparence_salariale_pct,

        -- Salaires exploitables
        agregats.nombre_salaires_exploitables,

        round(
            (
                100.0
                * agregats.nombre_salaires_exploitables
                / nullif(
                    agregats.nombre_offres,
                    0
                )
            )::numeric,
            2
        ) as taux_salaires_exploitables_sur_total_pct,

        round(
            (
                100.0
                * agregats.nombre_salaires_exploitables
                / nullif(
                    agregats.nombre_salaires_renseignes,
                    0
                )
            )::numeric,
            2
        ) as taux_exploitabilite_salaires_renseignes_pct,

        -- Qualité salaire
        agregats.nombre_salaires_suspects,

        agregats.nombre_salaires_non_exploitables,

        -- Statistiques
        agregats.salaire_annuel_moyen,

        agregats.salaire_annuel_median,

        agregats.premier_quartile,

        agregats.troisieme_quartile,

        case
            when agregats.premier_quartile is not null
             and agregats.troisieme_quartile is not null
            then
                agregats.troisieme_quartile
                - agregats.premier_quartile

            else null
        end as ecart_interquartile,

        agregats.salaire_annuel_min_observe,

        agregats.salaire_annuel_max_observe

    from agregats

    cross join volume_global

)

select *
from kpi

order by
    ordre_axe,
    rang_modalite
