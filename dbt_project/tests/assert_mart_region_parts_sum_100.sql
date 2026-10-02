with controle_parts as (

    select
        sum(part_offres_pct) as total_part_pct
    from {{ ref('mart_offres_par_region') }}

)

select *
from controle_parts
where total_part_pct < 99.50
   or total_part_pct > 100.50
