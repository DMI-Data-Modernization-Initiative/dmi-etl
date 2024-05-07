select 
    gender.gender_2 as gender,
    age_group.age_group_category,
    facility.facility_name,
    facility.latitude,
    facility.longitude,
    epi_week.week_number,
    epi_week.year,
    date.date,
    cascade.enrolled,
    cascade.have_diagnosis,
    cascade.have_outcome,
    cast(current_date as date) as load_date
from {{ ref('fct_afi_aggregate_diagnosis_outcome_cascade')}} as cascade 
left join {{ ref('dim_gender') }} as gender on gender.gender_key = cascade.gender_key 
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on age_group.age_group_key = cascade.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = cascade.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = cascade.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = cascade.date_key