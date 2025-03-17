select 
    gender.gender_2 as gender,
    age_group.age_group_category,
    facility.facility_name,
    facility.latitude,
    facility.longitude,
    epi_week.week_number,
    epi_week.year,
    date.date,
    date.month,
    case_classification.case_classification,
    case_classification.case_classification_full_name,
    syndromes.no_of_cases,
    cast(current_date as date) as load_date
from {{ ref('fct_aggregate_afi_surveillance_syndromes') }} as syndromes
left join {{ ref('dim_gender') }} as gender on gender.gender_key = syndromes.gender_key 
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on age_group.age_group_key = syndromes.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = syndromes.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = syndromes.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = syndromes.date_key
left join {{ ref('dim_case_classification') }} as case_classification on case_classification.case_classification_key = syndromes.case_classification_key