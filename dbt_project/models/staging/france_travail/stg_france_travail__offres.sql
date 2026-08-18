with source as (

    select *
    from {{ source('france_travail', 'france_travail_offres') }}

),

renamed as (

    select
        id_offre,
        intitule_offre,
        description_offre,

        date_creation,
        date_actualisation,
        date_actualisation_coherente,

        libelle_lieu_travail,
        latitude,
        longitude,
        code_postal,
        code_commune,
        coordonnees_renseignees,

        nom_entreprise,
        description_entreprise,
        entreprise_renseignee,
        description_entreprise_renseignee,
        information_entreprise_disponible,
        est_entreprise_adaptee,
        est_employeur_handi_engage,

        code_type_contrat,
        libelle_type_contrat,
        nature_contrat,

        experience_exigee,
        libelle_experience,
        commentaire_experience,

        code_rome,
        libelle_rome,
        libelle_appellation,

        salaire_libelle,
        salaire_renseigne,
        periode_salaire,

        est_alternance,
        nombre_postes,

        libelle_duree_travail,
        libelle_duree_travail_converti,

        code_qualification,
        libelle_qualification,
        code_naf,
        code_secteur_activite,
        libelle_secteur_activite,
        tranche_effectif_etablissement,

        offre_difficile_a_pourvoir,
        accessible_travailleur_handicape,
        code_deplacement,
        libelle_deplacement,

        complement_exercice,
        origine_offre,
        url_origine_offre,
        horaires_travail,

        mot_cle_recherche,
        date_ingestion,
        fichier_source,
        batch_id,
        date_chargement,
        fichier_source_silver,
        date_traitement_silver

    from source

)

select *
from renamed
