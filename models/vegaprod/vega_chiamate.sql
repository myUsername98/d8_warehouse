{{ config(materialized="table") }}

with
    source as (select * from {{source("vegaprod",'CHIAMATE')}}),

    renamed as (
        select distinct
            cast(chia_cod as int) as code_appel,
            cast(chia_dat as timestamp) as date_appel,
            chia_ctb as code_siege_social,
            client.raison_sociale as raison_sociale,
            cast(chia_fili as int) as filiale,
            case
                when chia_tip = 'R'
                then "Ravitaillement"
                when chia_tip = 'G'
                then "Panne"
                when chia_tip = 'L'
                then "Réclamation"
                when chia_tip = 'V'
                then "Commercial"
                else "Autre"
            end as type_appel,
            chia_tot as total,
            chia_pv as pdv,
            chia_d1 as description1,
            chia_d2 as description2,
            chia_d3 as description3,
            cast(_airbyte_emitted_at as timestamp) as derniere_maj
        from source as chiamate1
        left join
            {{ ref("vega_ctbcont") }} as client
            on chiamate1.chia_ctb = client.code_siege_social
        order by date_appel desc
    )

select *
from renamed