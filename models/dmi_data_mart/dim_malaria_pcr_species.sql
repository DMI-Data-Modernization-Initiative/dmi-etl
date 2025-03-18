with malaria_species_result as (
    select distinct
    pcr_malaria_species_code,
    pcr_malaria_species
    from {{ ref('intermediate_afi_malaria_pcr_species') }}
    where pcr_malaria_species_code is not null

),
final_data as (
  SELECT   
  {{ dbt_utils.surrogate_key( ['malaria_species_result.pcr_malaria_species_code']) }} as pcr_malaria_species_key,
  pcr_malaria_species_code,
  pcr_malaria_species
  FROM malaria_species_result
 union 
    select
        'unset' as pcr_malaria_species_key,
        -999 as pcr_malaria_species_code,
        'unset' as   pcr_malaria_species

)

select 
  final_data.*,
  cast(current_date as date) as load_date
from final_data 