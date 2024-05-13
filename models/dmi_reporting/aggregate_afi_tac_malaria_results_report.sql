select
    gender.gender_2 as gender,
    age_group.age_group_category,
    facility.facility_name,
    facility.latitude,
    facility.longitude,
    epi_week.week_number,
    epi_week.year,
    date.date,
    dim_result.malaria_pos_category,
    no_of_cases,
    no_of_positive_cases
from {{ ref('fct_afi_aggregate_tac_malaria_results') }} as malaria_results
left join {{ ref('dim_gender') }} as gender on gender.gender_key = malaria_results.gender_key 
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on age_group.age_group_key = malaria_results.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = malaria_results.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = malaria_results.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = malaria_results.date_key
left join {{ ref('dim_malaria_result')}} as dim_result on dim_result.malaria_pos_category_key = malaria_results.malaria_pos_category_key