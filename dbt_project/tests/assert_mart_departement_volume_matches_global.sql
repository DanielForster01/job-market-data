with volume_departement as (

    select
        sum(nombre_offres) as total_offres_departements
    from {{ ref('mart_offres_par_departement') }}

),

volume_global as (

    select
        nombre_total_offres
    from {{ ref('mart_kpi_marche_emploi') }}

)

select
    volume_departement.total_offres_departements,
    volume_global.nombre_total_offres
from volume_departement
cross join volume_global
where volume_departement.total_offres_departements
      != volume_global.nombre_total_offres
