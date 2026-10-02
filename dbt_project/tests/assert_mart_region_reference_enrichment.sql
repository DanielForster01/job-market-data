select
    code_region,
    nom_region
from {{ ref('mart_offres_par_region') }}
where code_region <> 'NR'
  and (
        nom_region is null
        or nom_region = 'Non renseigné'
  )
