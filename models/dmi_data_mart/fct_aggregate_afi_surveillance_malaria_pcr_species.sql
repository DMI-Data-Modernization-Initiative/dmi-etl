WITH malaria_pcr_species_data AS (
   SELECT
  mpsd.pid,
  mpsd.pcr_malaria_species_code,
  mpsd.consent as enrolled,
  other_pcr_data.enr_interviewdate,
  other_pcr_data.calculated_age_days,
  other_pcr_data.gender,
  other_pcr_data.site::INT AS mflcode
   FROM {{ ref('intermediate_afi_malaria_pcr_species') }} AS mpsd
   LEFT JOIN {{ ref('stg_afi_surveillance') }} as other_pcr_data on other_pcr_data.pid = mpsd.pid
   WHERE mpsd.consent = 1 and  mpsd.pcr_malaria_species_code is not null
)


--TODO add the join to the other dimensions
SELECT 
    coalesce(gender.gender_key, 'unset') as gender_key,
    coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
    coalesce(facility.facility_key, 'unset') as facility_key,
    coalesce(date.date_key, 'unset') as date_key,
    coalesce(malaria_pcr_species.pcr_malaria_species_key, 'unset') as pcr_malaria_species_key,
    coalesce(age_group.age_group_key, 'unset') as age_group_key,
    enrolled as cases_tested,
    pid,
    cast(current_date as date) as load_date
    
FROM malaria_pcr_species_data   
left join {{ ref('dim_gender') }} as gender on gender.code  = malaria_pcr_species_data.gender
LEFT JOIN {{ ref('dim_epi_week') }} AS epi_week 
    ON malaria_pcr_species_data.enr_interviewdate BETWEEN epi_week.start_of_week AND epi_week.end_of_week 
LEFT JOIN {{ ref('dim_facility') }} AS facility 
    ON facility.mfl_code = malaria_pcr_species_data.mflcode 
 left join {{ ref('dim_date') }} as date on date.date = malaria_pcr_species_data.enr_interviewdate
LEFT JOIN {{ ref('dim_age_group_afi') }} AS age_group 
    ON malaria_pcr_species_data.calculated_age_days >= age_group.start_age_days 
    AND malaria_pcr_species_data.calculated_age_days < age_group.end_age_days
 left join {{ ref('dim_malaria_pcr_species') }} as malaria_pcr_species on malaria_pcr_species.pcr_malaria_species_code = malaria_pcr_species_data.pcr_malaria_species_code
