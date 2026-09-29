with departements_offres as (

    select distinct
        code_departement
    from {{ ref('int_france_travail__offres_enrichies') }}
    where code_departement is not null

),

departements_inconnus as (

    select
        offres.code_departement

    from departements_offres as offres

    left join {{ ref('referentiel_departements') }} as reference
        on offres.code_departement = reference.code_departement

    where reference.code_departement is null

)

select *
from departements_inconnus
