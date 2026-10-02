select
    axe_analyse,
    code_modalite,
    count(*) as nombre_lignes

from {{ ref('mart_analyse_salaires') }}

group by
    axe_analyse,
    code_modalite

having count(*) > 1
