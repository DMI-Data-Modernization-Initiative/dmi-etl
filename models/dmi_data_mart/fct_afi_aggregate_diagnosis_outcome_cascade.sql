select
    coalesce(gender.gender_key, 'unset') as gender_key,
    coalesce(age_group.age_group_key, 'unset') as age_group_key,
    coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
    coalesce(facility.facility_key, 'unset') as facility_key,
    coalesce(date.date_key, 'unset') as date_key,
    count(distinct enroll."PID") as enrolled,
	sum(case when abstraction."Admissiondiagnosis" is not null then 1 else 0 end) as have_diagnosis, 
	sum(case when outcome."Outcome" is not null then 1 else 0 end) as have_outcome,
    cast(current_date as date) as load_date
from dbt_shield_dev.stg_afi_enroll_and_household_info as enroll
left join {{ ref('stg_afi_physical_abstraction') }} abstraction on abstraction."PID" = enroll."PID" 
left join {{ ref('stg_afi_clinical_course_outcome') }} as outcome on outcome."PID"  = enroll."PID" 
left join {{ ref('dim_gender') }} as gender on gender.code  = enroll."Gender"
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on enroll."Ageyrs" >= age_group.start_age 
    and  enroll."Ageyrs"  < age_group.end_age 
left join {{ ref('dim_epi_week') }} as epi_week on enroll.interview_date  >= epi_week.start_of_week 
    and enroll.interview_date <= epi_week.end_of_week 
left join {{ ref('dim_facility') }} as facility on facility.code = left(enroll."PID", 3)
left join {{ ref('dim_date') }} as date on date.date = enroll.interview_date 
group by
        coalesce(gender.gender_key, 'unset'),
        coalesce(age_group.age_group_key, 'unset'),
        coalesce(epi_week.epi_week_key, 'unset'),
        coalesce(facility.facility_key, 'unset'),
        coalesce(date.date_key, 'unset')