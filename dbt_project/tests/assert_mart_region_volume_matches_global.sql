with volume_regions as (

    select
        sum(nombre_offres) as total_offres_regions
    from {{ ref('mart_offres_par_region') }}

),

volume_global as (

    select
        nombre_total_offres
    from {{ ref('mart_kpi_marche_emploi') }}

)

select
    volume_regions.total_offres_regions,
    volume_global.nombre_total_offres
from volume_regions
cross join volume_global
where volume_regions.total_offres_regions
      != volume_global.nombre_total_offres
