with source as (

    select *
    from {{ source('france_travail', 'france_travail_offres') }}

),

renamed as (

    select
        -- Identité de l'offre
        id_offre,
        intitule_offre,
        description_offre,

        -- Dates
        date_creation,
        date_actualisation,
        date_actualisation_coherente,

        -- Localisation
        libelle_lieu_travail,
        latitude,
        longitude,
        code_postal,
        code_commune,
        coordonnees_renseignees,
        localisation_renseignee,

        -- Entreprise
        nom_entreprise,
        description_entreprise,
        entreprise_renseignee,
        description_entreprise_renseignee,
        information_entreprise_disponible,
        est_entreprise_adaptee,
        est_employeur_handi_engage,

        -- Contrat
        code_type_contrat,
        libelle_type_contrat,
        nature_contrat,

        -- Expérience
        experience_exigee,
        libelle_experience,
        commentaire_experience,

        -- Métier / ROME
        code_rome,
        libelle_rome,
        libelle_appellation,

        -- Salaire
        salaire_libelle,
        salaire_renseigne,
        periode_salaire,

        -- Alternance / postes
        est_alternance,
        nombre_postes,

        -- Temps de travail
        libelle_duree_travail,
        libelle_duree_travail_converti,

        -- Qualification / secteur
        code_qualification,
        libelle_qualification,
        code_naf,
        code_secteur_activite,
        libelle_secteur_activite,
        tranche_effectif_etablissement,

        -- Accessibilité / tension / déplacement
        offre_difficile_a_pourvoir,
        accessible_travailleur_handicape,
        code_deplacement,
        libelle_deplacement,

        -- Informations complémentaires
        complement_exercice,
        origine_offre,
        url_origine_offre,
        horaires_travail,

        -- Acquisition
        mot_cle_recherche,
        mots_cles_recherche,

        -- Traçabilité ingestion
        date_ingestion,
        fichier_source,
        objet_source_raw,

        -- Traçabilité Bronze
        bronze_processing_run_id,
        bronze_schema_version,
        objet_source_bronze,

        -- Traçabilité Silver
        batch_id,
        processing_run_id,
        silver_schema_version,
        date_traitement_silver,

        -- Traçabilité PostgreSQL
        date_chargement,
        fichier_source_silver

    from source

)

select *
from renamed