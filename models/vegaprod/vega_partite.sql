{{ config(materialized="table") }}

with
source as (select * from {{ source("vegaprod", "PARTITE") }}),

liste_pdv as (
    select distinct
        upv_cod as code_pdv,
        upv_trd as code_site,
        upv_cli as code_siege_social,
        client.holding as holding,
        upv_matr as matricule_da,
        upv_mod as model_da,
        client.raison_sociale as raison_social,
        client.ville as ville
    from {{ source("vegaprod", "UNOPV") }} as pdv
    inner join
        {{ ref("vega_ctbcont") }} as client
        on pdv.upv_cli = client.code_siege_social
),

max_airbyte_extracted as (
    select
        par_pv,
        max(_airbyte_extracted_at) as max_extracted_at
    from source
    where par_da = 'D'
    group by par_pv
),

renamed as (
    select distinct
        par_prg as id_partite,
        par_cli as code_siege_social,
        liste_pdv.raison_social as raison_social,
        liste_pdv.ville as ville,
        liste_pdv.holding as holding,
        liste_pdv.matricule_da,
        liste_pdv.model_da,
        source.par_pv as code_pdv,  -- Préfixez avec la table source
        cast(par_dat as timestamp) as date_facture,
        cast(par_fat as string) as code_facture_avoir,
        cast(par_imp as decimal) as montant_ht,
        cast((par_val - par_imp) as decimal) as tva,
        cast(par_val as decimal) as montant_ttc,
        source._airbyte_extracted_at as derniere_maj_donnees
    from source
    inner join liste_pdv on source.par_pv = liste_pdv.code_pdv
    inner join
        max_airbyte_extracted
        on source.par_pv = max_airbyte_extracted.par_pv
    where
        par_da = 'D'
        and source._airbyte_extracted_at
        = max_airbyte_extracted.max_extracted_at
)

select *
from renamed
