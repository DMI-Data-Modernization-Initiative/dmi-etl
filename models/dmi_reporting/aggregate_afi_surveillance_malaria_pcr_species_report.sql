SELECT 
PID,
gender.gender_2  as gender,
facility.facility_name,
age_group.age_group_category,
epi_week.week_number,
date.year,
date.month,
cases_tested,
malaria_pcr_species.pcr_malaria_species,
cast(current_date as date) as load_date
FROM 
{{ ref('fct_aggregate_afi_surveillance_malaria_pcr_species') }} as malaria_pcr_species_results
left join {{ ref('dim_gender') }} as gender on gender.gender_key = malaria_pcr_species_results.gender_key 
left join {{ ref('dim_age_group_afi') }} as age_group on age_group.age_group_key = malaria_pcr_species_results.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = malaria_pcr_species_results.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = malaria_pcr_species_results.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = malaria_pcr_species_results.date_key
left join {{ ref('dim_malaria_pcr_species') }} as malaria_pcr_species on malaria_pcr_species.pcr_malaria_species_key = malaria_pcr_species_results.pcr_malaria_species_key