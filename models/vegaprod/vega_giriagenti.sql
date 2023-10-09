{{ config(materialized="table") }}

with
    source as (select * from {{ source("vegaprod", "GIRIAGENTI") }}),

    renamed as (
        select distinct
            cast(gag_id as int) as id_passage,
            gag_chia_cod as appel_associe_au_passage,
            gag_cli as code_site,
            gag_pv as code_pdv,
            agent.nom_agent as nom_agent,
            gag_age as code_agent,
            cast(gag_incassate as int) as consos_theorique_encaisse,
            cast(gag_teomerce as int) as consos_theorique_livrees,
            (gag_incassate - gag_teomerce) as difference_consos,
            cast(gag_dataprevista as timestamp) as date_prevu,
            cast(gag_dataorainiatt as timestamp) as debut_activite,
            case
                when gag_previsto = 'S'
                then 'Oui'
                when gag_previsto = 'N'
                then 'Non'
                else null
            end as est_prevu,
            case
                when gag_eseguito = 'S'
                then 'Oui'
                when gag_eseguito = 'N'
                then 'Non'
                else null
            end as est_fait

        from source as passage
        left join
            {{ ref("vega_agenti") }} as agent on agent.code_agent = passage.gag_age
        order by id_passage desc
    )

select *
from renamed
