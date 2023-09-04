{{ config(materialized="table") }}

with
    source as (select * from {{ source("vegaprod", "CLIENTI") }}),

    renamed as (
        select distinct
            cli_cod as code_site,
            cli_cont as code_siege_social,
            cli_nome as raison_sociale,
            cli_nome2 as raison_sociale2,
            cli_ind as adresse1,
            cli_ind2 as adresse2,
            cli_cit as ville,
            cli_cap as code_postal,
            cli_ind_geoloc as adresse_pour_geolocalisation,
            cli_latitwgsdec as latitude,
            cli_longitwgsdec as longitude,
            cli_fili as filiale_de_reference,
            cli_orar as heure_ouverture,
            cli_age as ecrd_associee,
            cli_note as notes,
            cast(_airbyte_emitted_at as timestamp) as derniere_maj
        from source
    )

select *
from renamed
