select
    gender.gender_2 as gender,
    age_group.age_group_category,
    facility.facility_name,
    facility.latitude,
    facility.longitude,
    epi_week.week_number,
    epi_week.year,
    date.date,
    dim_coinfection.co_infection,
    coinfection.no_of_cases
from {{ ref('fct_afi_aggregate_coinfection') }} as coinfection 
left join {{ ref('dim_gender') }} as gender on gender.gender_key = coinfection.gender_key 
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on age_group.age_group_key = coinfection.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = coinfection.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = coinfection.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = coinfection.date_key
left join {{ ref('dim_coinfection') }} as dim_coinfection on dim_coinfection.co_infection_key = coinfection.co_infection_key