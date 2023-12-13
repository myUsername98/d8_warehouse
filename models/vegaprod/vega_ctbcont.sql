{{ config(materialized="table") }}

with
source as (select * from {{ source("vegaprod", "CTBCONT") }}),

max_airbyte_extracted as (
    select
        ctb_cod,
        max(_airbyte_extracted_at) as max_extracted_date
    from source
    group by ctb_cod
),

renamed as (
    select distinct
        source.ctb_cod as code_siege_social,
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
        _airbyte_extracted_at as derniere_maj

    from source
    inner join
        max_airbyte_extracted
        on source.ctb_cod = max_airbyte_extracted.ctb_cod
    where _airbyte_extracted_at = max_extracted_date
    order by code_siege_social desc
)

select *
from renamed
