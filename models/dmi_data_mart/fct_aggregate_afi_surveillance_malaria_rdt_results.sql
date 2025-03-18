WITH malaria_rdt_results AS (
   SELECT
     malaria_rdt.PID,
     malaria_rdt.site:: INT AS mflcode,
     malaria_rdt.gender,
     malaria_rdt.calculated_age_days,
     malaria_rdt.enr_interviewdate,
     malaria_rdt.malariardtres, --This from labresult  - (Positive, Negative)
     malaria_rdt.malariares, --This from malaria result - (Mixed species, Pan Malaria & P.falciparum)
     CASE WHEN  malaria_rdt.consent= 1 AND malariardtres IS NOT NULL then 1 else 0 end as malaria_rdt_tested
    
   FROM {{ ref('stg_afi_surveillance') }} AS malaria_rdt 
   WHERE malaria_rdt.consent= 1  AND malariardtres IS NOT NULL --This is the condition to filter out the those with a malaria result
)

SELECT 
coalesce(gender.gender_key, 'unset') as gender_key,
coalesce(age_group.age_group_key, 'unset') as age_group_key,
coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
coalesce(facility.facility_key, 'unset') as facility_key,
coalesce(date.date_key, 'unset') as date_key,
coalesce(result.lab_result_key, 'unset') as lab_result_key,
coalesce(malaria_result.malaria_pos_category_key, 'unset') as malaria_pos_category_key,
malaria_rdt_tested as cases_tested,
PID,
cast(current_date as date) as load_date

 FROM malaria_rdt_results

left join {{ ref('dim_gender') }} as gender on gender.code  = malaria_rdt_results.gender
LEFT JOIN {{ ref('dim_age_group_afi') }} AS age_group 
    ON malaria_rdt_results.calculated_age_days >= age_group.start_age_days 
    AND malaria_rdt_results.calculated_age_days < age_group.end_age_days
LEFT JOIN {{ ref('dim_epi_week') }} AS epi_week 
    ON malaria_rdt_results.enr_interviewdate BETWEEN epi_week.start_of_week AND epi_week.end_of_week 
LEFT JOIN {{ ref('dim_facility') }} AS facility 
    ON facility.mfl_code = malaria_rdt_results.mflcode 
 left join {{ ref('dim_date') }} as date on date.date = malaria_rdt_results.enr_interviewdate
  left join {{ ref('dim_lab_result') }} as result on result.code = malaria_rdt_results.malariardtres
 left join {{ ref('dim_malaria_results') }} as malaria_result on malaria_result.code = malaria_rdt_results.malariares::int
 
