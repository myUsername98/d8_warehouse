{{ config(materialized="table") }}

with
    source as (select * from {{ source("vegaprod", "NOTECREDITO") }}),

    renamed as (
        select distinct
            nc_prg as progressif_avoir,
            nc_mag as code_depot,
            nc_prt as code_avoir,
            nc_dat as date_avoir,
            nc_fili as filiale_de_competence,
            nc_cli as code_siege_social,
            nc_trd as code_site,
            nc_ord as code_appel_associe,
            nc_user as auteur_avoir,
            nc_numfat as facture_associee,
            concat(nc_ragsoc, ' ', nc_ragsoc1) as raison_social,
            concat(
                nc_ind, ' ', nc_ind2, ' ', nc_cit, ' ', nc_cap, ' ', nc_prov
            ) as adresse_client,
            nc_netto as montant_ht,
            nc_iva as tva,
            nc_lordo as montant_ttc,
            nc_d1 as note1,
            nc_d2 as note2,
            nc_d3 as note3,
            cast(_airbyte_extracted_at as timestamp) as derniere_maj_donnees
        from source

        order by progressif_avoir desc
    )

select *
from renamed
