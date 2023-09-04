{{ config(materialized="table") }}

with
    source as (select * from {{ source("vegaprod", "GIRIAGENTI") }}),

    renamed as (
        select
            gag_id as id_passage,
            gag_chia_cod as appel_associe_passage,
            gag_cli as code_site,
            gag_pv as code_pdv,
            cast(gag_dataprevista as timestamp) as date_prevu,
            cast(gag_dataorainiatt as timestamp) as debut_activite,
            cast(gag_dataoraevasione as timestamp) as fin_activite,
            case
                when gag_eseguito = 'S'
                then 'Oui'
                when gag_eseguito = 'N'
                then 'Non'
                else null
            end as est_prevu

        from source
    )

select *
from renamed
