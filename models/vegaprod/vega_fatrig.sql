{{ config(materialized="table") }}

with
    source as (select * from {{ source("vegaprod", "FATRIG") }}),

    renamed as (
        select distinct
            fri_seq as id_details_facture,
            fri_fat as id_progressif_facture,
            fri_art as code_article,
            fri_desa as description_article,
            fri_pv as pdv,
            fri_qta as quantite,
            fri_prznonscontato as prix_avant_reduction,
            fri_prz as prix,

        from source
    )

select *
from renamed
