select 
    gender.gender_2 as gender,
    age_group.age_group_category,
    facility.facility_name,
    facility.latitude,
    facility.longitude,
    epi_week.week_number,
    epi_week.year,
    date.date,
    dim_outcome.outcome,
    outcomes.no_of_cases
from {{ ref('fct_afi_aggregate_outcomes') }} as outcomes
left join {{ ref('dim_gender') }} as gender on gender.gender_key = outcomes.gender_key 
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on age_group.age_group_key = outcomes.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = outcomes.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = outcomes.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = outcomes.date_key
left join {{ ref('dim_outcome') }} as dim_outcome on dim_outcome.outcome_key = outcomes.outcome_key
