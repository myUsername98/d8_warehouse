{{ config(materialized = "table") }} WITH source AS (
    SELECT *
    FROM {{ source("vegaprod", "PARTITE") }}
),
liste_pdv AS (
    SELECT DISTINCT upv_cod AS code_pdv,
        upv_trd AS code_site,
        upv_cli AS code_siege_social,
        client.holding AS holding,
        upv_matr AS matricule_da,
        upv_mod AS model_da,
        client.raison_sociale AS raison_social,
        client.ville AS ville
    FROM {{ source("vegaprod", "UNOPV") }} AS pdv
        INNER JOIN {{ ref("vega_ctbcont") }} AS client ON pdv.upv_cli = client.code_siege_social
),
max_airbyte_extracted AS (
    SELECT par_pv,
        max(_airbyte_extracted_at) AS max_extracted_at
    FROM source
    WHERE par_da = 'D'
    GROUP BY par_pv
),
renamed AS (
    SELECT DISTINCT cast(par_prg AS int) AS id_partite,
        par_cli AS code_siege_social,
        liste_pdv.raison_social AS raison_social,
        liste_pdv.ville AS ville,
        liste_pdv.holding AS holding,
        liste_pdv.matricule_da,
        liste_pdv.model_da,
        source.par_pv AS code_pdv,
        -- Préfixez avec la table source
        cast(par_dat AS timestamp) AS date_facture,
        cast(par_fat AS STRING) AS code_facture_avoir,
        cast(par_imp AS decimal) AS montant_ht,
        cast((par_val - par_imp) AS decimal) AS tva,
        cast(par_val AS decimal) AS montant_ttc,
        cast(par_val AS decimal) AS montant_ttc,
        source._airbyte_extracted_at AS derniere_maj_donnees
    FROM source
        INNER JOIN liste_pdv ON source.par_pv = liste_pdv.code_pdv
        INNER JOIN max_airbyte_extracted ON source.par_pv = max_airbyte_extracted.par_pv
    WHERE par_da = 'D'
        AND source._airbyte_extracted_at = max_airbyte_extracted.max_extracted_at
)
SELECT *
FROM renamed