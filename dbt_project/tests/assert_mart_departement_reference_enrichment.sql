select
    code_departement,
    nom_departement,
    code_region,
    nom_region
from {{ ref('mart_offres_par_departement') }}
where code_departement <> 'NON_RENSEIGNE'
  and (
        nom_departement is null
        or nom_departement = 'Non renseigné'
        or code_region is null
        or code_region = 'NR'
        or nom_region is null
        or nom_region = 'Non renseigné'
  )
