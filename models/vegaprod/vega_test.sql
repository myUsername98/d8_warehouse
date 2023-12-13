{{ config(materialized="table") }}

with
source as (select * from {{ source('vegaprod', 'CTBCONT') }}),

commandes as (
    select
        chia_ctb as code_client,
        count(chia_cod) as nbre_de_commande
    from {{ source("vegaprod", 'CHIAMATE') }}
    where chia_tip = 'R'
    group by chia_ctb
    order by nbre_de_commande desc
),

commandes_par_client as (
    select distinct
        ctb_cod as code_client,
        ctb_desc as raison_sociale,
        commandes.nbre_de_commande as nbre_de_commande_ravitaillement
    from source as client
    left join commandes on client.ctb_cod = commandes.code_client
    order by nbre_de_commande_ravitaillement desc
)

select * from commandes_par_client