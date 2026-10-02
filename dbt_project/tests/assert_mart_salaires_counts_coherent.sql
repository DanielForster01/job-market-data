select
    axe_analyse,
    modalite,
    nombre_offres,
    nombre_salaires_renseignes,
    nombre_salaires_exploitables,
    nombre_salaires_non_exploitables

from {{ ref('mart_analyse_salaires') }}

where nombre_salaires_renseignes > nombre_offres

   or nombre_salaires_exploitables
      > nombre_salaires_renseignes

   or nombre_salaires_non_exploitables
      > nombre_salaires_renseignes

   or (
        nombre_salaires_exploitables
        + nombre_salaires_non_exploitables
      ) != nombre_salaires_renseignes
