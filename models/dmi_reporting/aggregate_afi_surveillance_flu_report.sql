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
    flu.enrolled,
    lab_result.lab_result,
    cast(current_date as date) as load_date
from {{ ref('fct_aggregate_afi_surveillance_flu') }} as flu
left join {{ ref('dim_gender') }} as gender on gender.gender_key = flu.gender_key 
left join {{ ref('dim_age_group_afi') }} as age_group on age_group.age_group_key = flu.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = flu.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = flu.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = flu.date_key
left join {{ ref('dim_lab_result') }} as lab_result on lab_result.lab_result_key = flu.lab_result_key