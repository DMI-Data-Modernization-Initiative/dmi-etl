WITH co_infection_data AS (
   SELECT
     co_infection.PID,
     co_infection.site:: INT AS mflcode,
     co_infection.gender,
     co_infection.calculated_age_days,
     co_infection.enr_interviewdate,
     co_infection."Target"  as microbe_target
   FROM {{ ref('stg_afi_surveillance') }} AS co_infection
   WHERE co_infection."TacResult" is not null
)

SELECT 
coalesce(gender.gender_key, 'unset') as gender_key,
coalesce(age_group.age_group_key, 'unset') as age_group_key,
coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
coalesce(facility.facility_key, 'unset') as facility_key,
coalesce(date.date_key, 'unset') as date_key,
PID,
microbe_target,
cast(current_date as date) as load_date
FROM co_infection_data
left join {{ ref('dim_gender') }} as gender on gender.code  = co_infection_data.gender
LEFT JOIN {{ ref('dim_age_group_afi') }} AS age_group 
    ON co_infection_data.calculated_age_days >= age_group.start_age_days 
    AND co_infection_data.calculated_age_days < age_group.end_age_days
LEFT JOIN {{ ref('dim_epi_week') }} AS epi_week 
    ON co_infection_data.enr_interviewdate BETWEEN epi_week.start_of_week AND epi_week.end_of_week 
LEFT JOIN {{ ref('dim_facility') }} AS facility 
    ON facility.mfl_code = co_infection_data.mflcode 
 left join {{ ref('dim_date') }} as date on date.date = co_infection_data.enr_interviewdate