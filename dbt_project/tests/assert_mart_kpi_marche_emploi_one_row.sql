with nombre_lignes as (

    select
        count(*) as total_lignes
    from {{ ref('mart_kpi_marche_emploi') }}

)

select *
from nombre_lignes
where total_lignes != 1
