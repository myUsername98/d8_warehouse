{{ config(materialized="table") }}

with
    source as (select * from {{ source("vegaprod", "CTBCONT") }}),

    renamed as (
        select
            ctb_cod as code_siege_social,
            ctb_holding as holding,
            ctb_desc as raison_sociale,
            ctb_ind as adresse1,
            ctb_ind2 as adresse2,
            ctb_tel as telephone,
            ctb_cap as code_postal,
            ctb_cit as ville,
            ctb_email as email,
            ctb_piva as siren,
            ctb_prov as departement,
            _airbyte_emitted_at as derniere_maj

        from source

        order by code_siege_social desc
    )

select *
from renamed
