with source as (

    select
        count(*) as nombre_salaires_source
    from {{ ref('int_france_travail__offres_enrichies') }}
    where salaire_renseigne is true

),

normalises as (

    select
        count(*) as nombre_salaires_normalises
    from {{ ref('int_france_travail__salaires_normalises') }}

)

select
    source.nombre_salaires_source,
    normalises.nombre_salaires_normalises

from source
cross join normalises

where source.nombre_salaires_source
      != normalises.nombre_salaires_normalises
