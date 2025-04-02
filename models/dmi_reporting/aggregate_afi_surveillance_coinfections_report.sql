select 
    gender.gender_2 as gender,
    age_group.age_group_category,
    facility.facility_name,
    epi_week.week_number,
    epi_week.year,
    date.date,
    date.month,
     co_infections.microbe_target,
     co_infections.PID,
    cast(current_date as date) as load_date
from {{ ref('fct_aggregate_afi_surveillance_coinfections') }} as co_infections
left join {{ ref('dim_gender') }} as gender on gender.gender_key = co_infections.gender_key 
left join {{ ref('dim_age_group_afi') }} as age_group on age_group.age_group_key = co_infections.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = co_infections.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = co_infections.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = co_infections.date_key
