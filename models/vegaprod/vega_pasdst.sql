{{ config(materialized="table") }}

with
    source as (select * from {{ source("vegaprod", "PASDST") }}),

    renamed as (
        select
            psd_id as id_passage,
            cast(psd_dat as timestamp) as date_passage,
            cast(psd_drgf as timestamp) as date_enregistrement_fourniture,
            cast(psd_drgi as timestamp) as date_enregistrement_recette,
            psd_age as code_ecrd,
            psd_upvmag as depot_pdv,
            psd_pv as code_pdv,
            psd_matr as matricule_DA,
            psd_btm as consommation_theorique_livrees,
            psd_inca as consommation_encaissee,
            psd_vend as valeur_du_vendu,
            psd_itot as recette_passage,
            psd_note as notes_du_passage,
            psd_cli as code_siege_social,
            psd_trd as code_site,
            cast(_airbyte_extracted_at as timestamp) as derniere_maj_donnees
        from source
    )

select *
from renamed
