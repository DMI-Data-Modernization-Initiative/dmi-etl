select
    coalesce(gender.gender_key, 'unset') as gender_key,
    coalesce(age_group.age_group_key, 'unset') as age_group_key,
    coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
    coalesce(facility.facility_key, 'unset') as facility_key,
    coalesce(date.date_key, 'unset') as date_key,
    coalesce(dim_outcome.outcome_key, 'unset') as outcome_key,
	count(*) as no_of_cases,
    cast(current_date as date) as load_date
from dbt_shield_dev.stg_afi_clinical_course_outcome as outcome
left join {{ ref('stg_afi_enroll_and_household_info') }} as enroll on enroll."PID" = outcome."PID" 
left join {{ ref('dim_gender') }} as gender on gender.code = enroll."Gender"
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on enroll."Ageyrs" >= age_group.start_age 
    and  enroll."Ageyrs"  < age_group.end_age 
left join {{ ref('dim_epi_week') }} as epi_week on enroll.interview_date  >= epi_week.start_of_week 
    and enroll.interview_date <= epi_week.end_of_week 
left join {{ ref('dim_facility') }} as facility on facility.code = left(outcome."PID", 3) 
left join {{ ref('dim_date') }} as date on date.date = enroll.interview_date
left join {{ ref('dim_outcome') }} as dim_outcome on dim_outcome.outcome_code = outcome."Outcome"
group by 
    coalesce(gender.gender_key, 'unset'),
    coalesce(age_group.age_group_key, 'unset'),
    coalesce(epi_week.epi_week_key, 'unset'),
    coalesce(facility.facility_key, 'unset'),
    coalesce(date.date_key, 'unset'),
    coalesce(dim_outcome.outcome_key, 'unset')