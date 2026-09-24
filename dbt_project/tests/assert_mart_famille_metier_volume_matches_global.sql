with volume_famille as (

    select
        sum(nombre_offres) as total_offres_famille
    from {{ ref('mart_offres_par_famille_metier') }}

),

volume_global as (

    select
        nombre_total_offres
    from {{ ref('mart_kpi_marche_emploi') }}

)

select
    volume_famille.total_offres_famille,
    volume_global.nombre_total_offres
from volume_famille
cross join volume_global
where volume_famille.total_offres_famille != volume_global.nombre_total_offres
