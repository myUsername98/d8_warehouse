{{ config(materialized="table") }}

with
    source as (select * from {{ source("vegaprod", "BPCUSTOMER") }}),

    renamed as (
        select 
        bpcnum_0 as code_siege_social,
        bpcnam_0 as raison_sociale,
        pte_0 as mode_reglement,
        cast(credat_0 as timestamp) as date_creation,
        _airbyte_extracted_at as derniere_maj

        from source
    )

    select * from renamed