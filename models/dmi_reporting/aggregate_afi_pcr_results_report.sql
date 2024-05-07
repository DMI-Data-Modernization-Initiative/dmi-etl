select 
	gender.gender_2 as gender,
    age_group.age_group_category,
    facility.facility_name,
    facility.latitude,
    facility.longitude,
    epi_week.week_number,
    epi_week.year,
    date.date,
    'Negative' as disease, /* putting a 'Negative' dummy value on disease to enable plotting of Negative cases and Positive cases per disease in one graph */
    negative_results.negative_tests as no_of_cases,
    cast(current_date as date) as load_date
from {{ ref('fct_afi_aggregate_pcr_negative_results') }} as negative_results
left join {{ ref('dim_gender') }} as gender on gender.gender_key = negative_results.gender_key 
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on age_group.age_group_key = negative_results.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = negative_results.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = negative_results.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = negative_results.date_key

union all 

select 
	gender.gender_2 as gender,
    age_group.age_group_category,
    facility.facility_name,
    facility.latitude,
    facility.longitude,
    epi_week.week_number,
    epi_week.year,
    date.date,
    disease as disease,
    positive_results.no_of_cases,
    cast(current_date as date) as load_date
from {{ ref('fct_afi_aggregate_pcr_positive_results') }} as positive_results
left join {{ ref('dim_gender') }} as gender on gender.gender_key = positive_results.gender_key 
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on age_group.age_group_key = positive_results.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = positive_results.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = positive_results.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = positive_results.date_key
left join {{ ref('dim_disease') }} as disease on disease.disease_key = positive_results.disease_key