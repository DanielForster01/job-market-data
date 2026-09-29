with controle as (

    select
        sum(part_offres_pct) as total_part_pct
    from {{ ref('mart_offres_par_departement') }}

)

select *
from controle
where total_part_pct < 99.50
   or total_part_pct > 100.50
