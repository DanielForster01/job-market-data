with global as (

    select
        nombre_offres,
        nombre_salaires_renseignes,
        nombre_salaires_exploitables,
        nombre_salaires_suspects,
        nombre_salaires_non_exploitables

    from {{ ref('mart_analyse_salaires') }}

    where axe_analyse = 'Global'
      and code_modalite = 'GLOBAL'

),

axes as (

    select
        axe_analyse,

        sum(nombre_offres)
            as nombre_offres,

        sum(nombre_salaires_renseignes)
            as nombre_salaires_renseignes,

        sum(nombre_salaires_exploitables)
            as nombre_salaires_exploitables,

        sum(nombre_salaires_suspects)
            as nombre_salaires_suspects,

        sum(nombre_salaires_non_exploitables)
            as nombre_salaires_non_exploitables

    from {{ ref('mart_analyse_salaires') }}

    where axe_analyse <> 'Global'

    group by axe_analyse

)

select
    axes.*

from axes
cross join global

where axes.nombre_offres
      != global.nombre_offres

   or axes.nombre_salaires_renseignes
      != global.nombre_salaires_renseignes

   or axes.nombre_salaires_exploitables
      != global.nombre_salaires_exploitables

   or axes.nombre_salaires_suspects
      != global.nombre_salaires_suspects

   or axes.nombre_salaires_non_exploitables
      != global.nombre_salaires_non_exploitables
