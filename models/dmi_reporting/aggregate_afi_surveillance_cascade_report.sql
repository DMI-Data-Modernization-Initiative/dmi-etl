select 
    gender.gender_2 as gender,
    age_group.age_group_category,
    facility.facility_name,
    facility.latitude,
    facility.longitude,
    epi_week.week_number,
    epi_week.year as epi_year,
    date.date,
    date.month,
    date.year,
    date.calendarquarter,
    screened,
	eligible,
	enrolled,
	eligible_sampling,
	sampled,
    cast(current_date as date) as load_date
from {{ ref('fct_aggregate_afi_surveillance_cascade') }} as cascade
left join {{ ref('dim_gender') }} as gender on gender.gender_key = cascade.gender_key 
left join {{ ref('dim_age_group_afi') }} as age_group on age_group.age_group_key = cascade.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = cascade.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = cascade.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = cascade.date_key 