select
    code_departement,
    nombre_offres,
    nombre_offres_data_engineering,
    nombre_offres_data_analysis_bi,
    nombre_offres_data_science_ia,
    nombre_offres_autre_data_numerique
from {{ ref('mart_offres_par_departement') }}
where
      nombre_offres_data_engineering
    + nombre_offres_data_analysis_bi
    + nombre_offres_data_science_ia
    + nombre_offres_autre_data_numerique
    != nombre_offres
