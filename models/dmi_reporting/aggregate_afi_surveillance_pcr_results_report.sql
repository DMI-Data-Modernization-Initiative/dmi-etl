SELECT gender.gender_2 as gender,
    age_group.age_group_category,
    facility.facility_name,
    facility.latitude,
    facility.longitude,
    epi_week.week_number,
    epi_week.year,
    date.date,
    date.month,
    date.Year as calender_year,
    disease as disease,
    positive_results.no_of_cases,
    pid,
    cast(current_date as date) as load_date
from {{ ref('fct_aggregate_afi_surveillance_pcr_positive_results') }} as positive_results
left join {{ ref('dim_gender') }} as gender on gender.gender_key = positive_results.gender_key 
left join {{ ref('dim_age_group_afi') }} as age_group on age_group.age_group_key = positive_results.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = positive_results.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = positive_results.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = positive_results.date_key
left join {{ ref('dim_disease') }} as disease on disease.disease_key = positive_results.disease_key


