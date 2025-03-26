select 
    COALESCE(gender.gender_key, 'unset') AS gender_key,
    COALESCE(age_group.age_group_key, 'unset') AS age_group_key,
    COALESCE(epi_week.epi_week_key, 'unset') AS epi_week_key,
    COALESCE(facility.facility_key, 'unset') AS facility_key,
    COALESCE(date.date_key, 'unset') AS date_key,
    coalesce(result.lab_result_key, 'unset') as lab_result_key,
    CASE when  consent=1 then 1 else 0 end as enrolled,
    site::int as mflcode
from  {{ ref('stg_afi_surveillance') }} as afi_data_rsv 
left join {{ ref('dim_lab_result') }} as result on result.lab_result_2 = afi_data_rsv.rsvresult 

-- Joining gender dimension
LEFT JOIN {{ ref('dim_gender') }} AS gender 
    ON gender.code = afi_data_rsv.gender

-- Joining epidemiological week dimension
LEFT JOIN {{ ref('dim_epi_week') }} AS epi_week 
    ON afi_data_rsv.screening_date BETWEEN epi_week.start_of_week AND epi_week.end_of_week 

-- Joining age group dimension
LEFT JOIN {{ ref('dim_age_group_afi') }} AS age_group 
    ON afi_data_rsv.calculated_age_days >= age_group.start_age_days 
    AND afi_data_rsv.calculated_age_days < age_group.end_age_days

-- Joining facility dimension
LEFT JOIN {{ ref('dim_facility') }} AS facility 
    ON facility.mfl_code = afi_data_rsv.site 

-- Joining date dimension
LEFT JOIN {{ ref('dim_date') }} AS date 
    ON date.date = afi_data_rsv.screening_date 
