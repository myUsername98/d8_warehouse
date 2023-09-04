{{ config(materialized="table") }}

with
    source as (select * from `smooth-copilot-393507.vegaprod.MODELLI`),

    renamed as (
        select
            mod_cod as code_model,
            mod_desc as description_model,
            mod_for as marque,
            mod_prodotto as code_produit_da,
            case
                when mod_obsoleto = 'S'
                then "Oui"
                when mod_obsoleto = 'N'
                then "Non"
                else "Autre"
            end as est_obsolete,
            case
                when mod_virtuale = 'S'
                then "Oui"
                when mod_virtuale = 'N'
                then "Non"
                else "Autre"
            end as est_virtuel,
            mod_ragmod as groupement_modele
        from source
    )

select *
from renamed
