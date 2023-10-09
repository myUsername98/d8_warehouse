{{ config(materialized="table") }}

with
    source as (select * from {{ source("vegaprod", "FATTURE") }}),

    renamed as (
        select
            fat_prg as progressif_facture,
            fat_prt as code_facture,
            cast(fat_dat as timestamp) as date_facture,
            fat_pv as pdv_associe,
            fat_cli as code_siege_social,
            fat_ragsoc as raison_social,
            fat_mag as code_depot,
            fat_user as utilisateur,
            fat_d1 as note1,
            fat_d2 as note2,
            fat_d3 as note3,
            fat_iva as montant_tva,
            fat_netto as montant_ht,
            fat_lordo as montant_ttc,
            case
                when fat_evasa = 'S'
                then "Oui"
                when fat_evasa = 'N'
                then "Non"
                else "Autre"
            end as facture_executee,
            fat_email as email
        from source
        order by fat_prg desc
    )

select *
from renamed
