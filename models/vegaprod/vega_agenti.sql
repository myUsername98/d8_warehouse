{{ config(materialized="table") }}

with
    source as (select * from {{ source("vegaprod", "AGENTI") }}),

    renamed as (
        select distinct
            age_cod as code_agent,
            age_nome as nom_agent,
            age_fam as type,
            age_mans as fonction_principale,
            age_dataassunzione as date_debut_contrat,
            age_datacessatorap as date_fin_contrat
        from source
        order by nom_agent asc
    )

select *
from renamed
