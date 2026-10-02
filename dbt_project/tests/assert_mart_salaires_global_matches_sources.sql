with mart as (

    select *
    from {{ ref('mart_analyse_salaires') }}

    where axe_analyse = 'Global'
      and code_modalite = 'GLOBAL'

),

offres as (

    select
        count(*) as nombre_offres,

        count(*) filter (
            where salaire_renseigne is true
        ) as nombre_salaires_renseignes

    from {{ ref('int_france_travail__offres_enrichies') }}

),

salaires as (

    select
        count(*) filter (
            where salaire_exploitable is true
        ) as nombre_salaires_exploitables,

        count(*) filter (
            where salaire_suspect is true
        ) as nombre_salaires_suspects

    from {{ ref('int_france_travail__salaires_normalises') }}

)

select
    mart.nombre_offres,
    offres.nombre_offres as nombre_offres_attendu,

    mart.nombre_salaires_renseignes,
    offres.nombre_salaires_renseignes
        as nombre_salaires_renseignes_attendu,

    mart.nombre_salaires_exploitables,
    salaires.nombre_salaires_exploitables
        as nombre_salaires_exploitables_attendu,

    mart.nombre_salaires_suspects,
    salaires.nombre_salaires_suspects
        as nombre_salaires_suspects_attendu

from mart

cross join offres
cross join salaires

where mart.nombre_offres
      != offres.nombre_offres

   or mart.nombre_salaires_renseignes
      != offres.nombre_salaires_renseignes

   or mart.nombre_salaires_exploitables
      != salaires.nombre_salaires_exploitables

   or mart.nombre_salaires_suspects
      != salaires.nombre_salaires_suspects
