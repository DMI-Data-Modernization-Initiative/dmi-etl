SELECT 
    COALESCE(gender.gender_key, 'unset') AS gender_key,
    COALESCE(age_group.age_group_key, 'unset') AS age_group_key,
    COALESCE(epi_week.epi_week_key, 'unset') AS epi_week_key,
    COALESCE(facility.facility_key, 'unset') AS facility_key,
    COALESCE(date.date_key, 'unset') AS date_key,
    COALESCE(case_classification.case_classification_key, 'unset') AS case_classification_key,
    COUNT(DISTINCT enroll.PID) AS no_of_cases,
    CAST(CURRENT_DATE AS DATE) AS load_date
FROM {{ ref('stg_afi_surveillance') }} AS enroll

-- Joining case classification dimension
LEFT JOIN {{ ref('dim_case_classification') }} AS case_classification 
    ON case_classification.case_classification = enroll.proposed_combined_case

-- Joining gender dimension
LEFT JOIN {{ ref('dim_gender') }} AS gender 
    ON gender.code = enroll.gender

-- Joining epidemiological week dimension
LEFT JOIN {{ ref('dim_epi_week') }} AS epi_week 
    ON enroll.screening_date BETWEEN epi_week.start_of_week AND epi_week.end_of_week 

-- Joining age group dimension
LEFT JOIN {{ ref('dim_age_group_afi') }} AS age_group 
    ON enroll.calculated_age_days >= age_group.start_age_days 
    AND enroll.calculated_age_days < age_group.end_age_days

-- Joining facility dimension
LEFT JOIN {{ ref('dim_facility') }} AS facility 
    ON facility.mfl_code = enroll.site 

-- Joining date dimension
LEFT JOIN {{ ref('dim_date') }} AS date 
    ON date.date = enroll.screening_date 

WHERE enroll.eligible = 1  -- only include eligible cases
    AND enroll.consent = 1 -- only include cases with consent(the enrolled cases)

GROUP BY 
    COALESCE(gender.gender_key, 'unset'),
    COALESCE(age_group.age_group_key, 'unset'),
    COALESCE(epi_week.epi_week_key, 'unset'),
    COALESCE(facility.facility_key, 'unset'),
    COALESCE(date.date_key, 'unset'),
    COALESCE(case_classification.case_classification_key, 'unset')
