{{ config(materialized="table") }}

with
    source as (select * from {{ source("vegaprod", "UNOPV") }}),

    renamed as (
        select distinct
            upv_cod as code_pdv,
            upv_cli as code_siege_social,
            cast(upv_matr as int) as matricule_pdv,
            case
                when upv_funz = 'K'
                then "Partielle"
                when upv_funz = 'D'
                then "Total"
                else "Autre"
            end as forme_de_gestion,
            model.description_model as model_da,
            cast(upv_inst as timestamp) as date_installation,
            cast(upv_dsos as timestamp) as date_dernier_remplacement,
            cast(upv_rit as timestamp) as date_retrait,
            cast(upv_agg as timestamp) as date_dernier_ravitaillement,
            nom_agent as ecrd_dernier_ravitaillement,
            {{ dbt.datediff("upv_inst", "upv_rit", "year") }}
            as nbr_annees_fonctionnement,
            {{ dbt.datediff("upv_inst", "upv_rit", "month") }}
            as nbr_de_mois_fonctionnement,
            {{ dbt.datediff("upv_agg", "current_date()", "day") }}
            as _jours_depuis_dernier_ravitaillement,
            cast(upv_dataultimoincasso as timestamp) as date_derniere_recette,
            case
                when upv_fake = 'S'
                then "Oui"
                when upv_fake = 'N'
                then "Non"
                else "Autre"
            end as est_pdv_fictif,
            case
                when upv_att = 'S' then "Oui" when upv_att = 'N' then "Non" else "Autre"
            end as pdv_actif,
            cast(upv_fili as int) as filiale,
            upv_note as notes,
            cast(_airbyte_extracted_at as timestamp) as derniere_maj_donnees
        from source as pdv
        left join {{ ref("vega_modelli") }} as model on model.code_model = pdv.upv_mod
        left join {{ref("vega_agenti")}} as agent on agent.code_agent = pdv.upv_age
    )

select *
from renamed