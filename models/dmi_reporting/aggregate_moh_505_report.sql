select 
    disease.disease,
    moh_505.gender,
    age_group_category,
    epi_wk.week_number as epi_week,
    epi_wk.year,
    cast(epi_wk.start_of_week as date) as start_of_epi_week,
    cast(epi_wk.end_of_week as date) as end_of_epi_week,
    county.county,
    sub_county.sub_county,
    facility.facility_name,
    indicator.indicator_description,
    moh_505.indicator_value,
    moh_505.data_source,
    cast(current_date as date) as load_date
from {{ref('fct_aggregate_moh_505')}} as moh_505
left join {{ ref('dim_age_group') }} as age_group on age_group.age_group_key = moh_505.age_group_key
left join {{ ref('dim_epi_week') }} as epi_wk on epi_wk.epi_week_key = moh_505.epi_week_key
left join {{ ref('dim_county') }} as county on county.county_key = moh_505.county_key
left join {{ ref('dim_disease') }} as disease on disease.disease_key = moh_505.disease_key
left join {{ref('dim_facility')}} as facility on facility.facility_key = moh_505.facility_key
left join {{ref('dim_indicator')}} as indicator on indicator.indicator_key = moh_505.indicator_key
left join {{ref('dim_sub_county')}} as sub_county on sub_county.sub_county_key = moh_505.sub_county_key
