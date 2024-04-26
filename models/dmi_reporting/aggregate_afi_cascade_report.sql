select 
    gender.gender_2 as gender,
    age_group.age_group_category,
    facility.facility_name,
    epi_week.week_number,
    epi_week.year,
    date.date,
    screened,
	eligible,
	enrolled,
	eligible_for_sampling,
	sampled
from {{ ref('fct_afi_aggregate_cascade') }} as cascade
left join {{ ref('dim_gender') }} as gender on gender.gender_key = cascade.gender_key 
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on age_group.age_group_key = cascade.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = cascade.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = cascade.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = cascade.date_key 
