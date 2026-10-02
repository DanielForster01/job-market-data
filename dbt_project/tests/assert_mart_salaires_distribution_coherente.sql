select
    axe_analyse,
    modalite,

    nombre_salaires_exploitables,

    salaire_annuel_min_observe,
    premier_quartile,
    salaire_annuel_median,
    troisieme_quartile,
    salaire_annuel_max_observe

from {{ ref('mart_analyse_salaires') }}

where nombre_salaires_exploitables > 0

  and (
       salaire_annuel_min_observe is null

       or premier_quartile is null

       or salaire_annuel_median is null

       or troisieme_quartile is null

       or salaire_annuel_max_observe is null

       or salaire_annuel_min_observe
          > premier_quartile

       or premier_quartile
          > salaire_annuel_median

       or salaire_annuel_median
          > troisieme_quartile

       or troisieme_quartile
          > salaire_annuel_max_observe
  )
