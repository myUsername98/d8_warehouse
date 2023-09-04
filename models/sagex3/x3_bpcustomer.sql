{{ config(materialized="table") }}

with
    source as (select * from `smooth-copilot-393507.sageX3.BPCUSTOMER`),

    renamed as (
        select 
        bpcnum_0 as code_siege_social,
        bpcnam_0 as raison_sociale,
        pte_0 as mode_reglement,
        cast(credat_0 as timestamp) as date_creation,
        _airbyte_emitted_at as derniere_maj

        from source
    )

    select * from renamed