{{ config(materialized="table") }}

with source as (select * from {{source("vegaprod", "FATRIG")}}),

renamed as (
    select distinct
        fri_fat as id_progressif_facture,
        cast(facture.date_facture as timestamp) as date_facture,
        facture.code_facture as code_facture,
        fri_art as code_produit,
        fri_desa as description_produit,
        fri_qta as quantite_produit,
        fri_prz as prix_produit,
        cast(fri_qta*fri_prz as decimal) as montant_ht,
        cast(_airbyte_extracted_at as timestamp) as derniere_maj_donnees,
    from source as fatrig
    left join {{ ref("vega_fatture") }} as facture on facture.progressif_facture = fatrig.fri_fat
    order by id_progressif_facture desc
)

select * from renamed