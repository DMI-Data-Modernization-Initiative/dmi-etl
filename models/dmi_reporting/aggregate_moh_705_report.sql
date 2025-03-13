select
    disease.disease,
    gender.gender,
    age_group.age_group_category,
    moh_705.year,
    moh_705.month,
    moh_705.year_month,
    concat(county.county, ' ', 'County') as county,
    concat(sub_county.sub_county, ' ', 'Sub County') as sub_county,
    facility.facility_name,
    indicator.indicator_description,
    moh_705.indicator_value,
    moh_705.data_source,
    cast(current_date as date) as load_date
from {{ ref("fct_aggregate_moh_705") }} as moh_705
left join {{ ref("dim_county") }} as county on county.county_key = moh_705.county_key
left join
    {{ ref("dim_disease") }} as disease on disease.disease_key = moh_705.disease_key
left join
    {{ ref("dim_facility") }} as facility
    on facility.facility_key = moh_705.facility_key
left join
    {{ ref("dim_indicator") }} as indicator
    on indicator.indicator_key = moh_705.indicator_key
left join
    {{ ref("dim_sub_county") }} as sub_county
    on sub_county.sub_county_key = moh_705.sub_county_key
left join
    {{ ref("dim_age_group_khis") }} as age_group
    on age_group.age_group_key = moh_705.age_group_key
left join {{ ref("dim_gender") }} as gender on gender.gender_key = moh_705.gender_key
