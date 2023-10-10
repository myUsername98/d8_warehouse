{{ config(materialized="table") }}

with
    source as (select * from {{ source("vegaprod", "RIGHENOTECREDITO") }}),

    final as (
        select distinct
            ncd_seq as id_details_avoirs,
            ncd_nc as progressif_avoirs,
            ncd_art as code_article,
            ncd_desa as description_article,
            ncd_pv as pdv,
            ncd_qta as quantite,
            ncd_prznonscontato as prix_avant_reduction,
            ncd_prz as prix
        from source
        order by id_details_avoirs desc
    )

select *
from final
