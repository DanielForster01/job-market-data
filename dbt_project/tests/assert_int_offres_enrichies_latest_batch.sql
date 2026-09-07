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

lignes_hors_dernier_batch as (

    select
        offres.id_offre,
        offres.batch_id,
        offres.date_chargement
    from {{ ref('int_france_travail__offres_enrichies') }} as offres
    left join dernier_batch
        on offres.batch_id = dernier_batch.batch_id
    where dernier_batch.batch_id is null

)

select *
from lignes_hors_dernier_batch
