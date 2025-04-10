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
    date.calendarquarter,
    sars_cov.enrolled,
    lab_result.lab_result, 
    cast(current_date as date) as load_date
from {{ ref('fct_aggregate_afi_surveillance_sars_cov') }} as sars_cov
left join {{ ref('dim_gender') }} as gender on gender.gender_key = sars_cov.gender_key 
left join {{ ref('dim_age_group_afi') }} as age_group on age_group.age_group_key = sars_cov.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = sars_cov.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = sars_cov.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = sars_cov.date_key
 left join {{ ref('dim_lab_result') }} as lab_result on lab_result.lab_result_key = sars_cov.lab_result_key