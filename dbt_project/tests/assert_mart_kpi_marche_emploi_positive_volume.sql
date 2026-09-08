select *
from {{ ref('mart_kpi_marche_emploi') }}
where nombre_total_offres <= 0
