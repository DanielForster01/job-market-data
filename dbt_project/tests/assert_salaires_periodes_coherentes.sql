select
    id_offre,
    salaire_libelle,
    periode_extraite,
    periode_salaire_normalisee

from {{ ref('int_france_travail__salaires_normalises') }}

where periode_salaire_coherente is false
