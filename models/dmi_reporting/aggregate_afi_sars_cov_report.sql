select 
    gender.gender_2 as gender,
    age_group.age_group_category,
    facility.facility_name,
    epi_week.week_number,
    epi_week.year,
    date.date,
    lab_result.lab_result,
    lab_result.lab_result_2,
    sarscov.type,
    sarscov.no_of_cases
from {{ ref('fct_afi_aggregate_sars_cov') }} as sarscov 
left join {{ ref('dim_gender') }} as gender on gender.gender_key = sarscov.gender_key 
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on age_group.age_group_key = sarscov.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = sarscov.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = sarscov.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = sarscov.date_key
left join {{ ref('dim_lab_result') }} as lab_result on lab_result.lab_result_key = sarscov.lab_result_key