{{ config(materialized="table") }}

with
    source as (select * from {{ source("vegaprod", "NOTECREDITO") }}),

    final as (
        select
            nc_prg as progressif_avoir,
            nc_prt as code_avoir,
            details_avoirs.pdv as pdv_concerne,
            details_avoirs.code_article as code_article,
            details_avoirs.description_article as description_article,
            details_avoirs.quantite as quantite,
            details_avoirs.prix as prix,
            cast(details_avoirs.quantite*details_avoirs.prix as decimal) as montant_ht
        from source as avoirs
        left join {{ref('vega_righenotecredito')}} as details_avoirs on details_avoirs.progressif_avoirs = avoirs.nc_prg
        order by progressif_avoir desc
    )

select *
from final
