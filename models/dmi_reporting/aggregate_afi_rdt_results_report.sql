select 
    gender.gender_2 as gender,
    age_group.age_group_category,
    facility.facility_name,
    facility.latitude,
    facility.longitude,
    epi_week.week_number,
    epi_week.year,
    date.date,
    lab_result.lab_result,
    rdt.rdt,
    rdt.cases as no_of_cases,
    cast(current_date as date) as load_date
from {{ref('fct_afi_aggregate_rdt_results') }} as rdt 
left join {{ ref('dim_gender') }} as gender on gender.gender_key = rdt.gender_key 
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on age_group.age_group_key = rdt.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = rdt.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = rdt.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = rdt.date_key
left join {{ ref('dim_lab_result') }} as lab_result on lab_result.lab_result_key = rdt.lab_result_key