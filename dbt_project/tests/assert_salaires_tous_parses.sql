select
    id_offre,
    salaire_libelle
from {{ ref('int_france_travail__salaires_normalises') }}
where salaire_parse_ok is false
