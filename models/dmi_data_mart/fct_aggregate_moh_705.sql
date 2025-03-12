with
    khis_data as (
        select

            coalesce(disease.disease_key, 'unset') as disease_key,
            'unset' as gender_key,
            coalesce(age_group.age_group_key, 'unset') as age_group_key,
            coalesce(county.county_key, 'unset') as county_key,
            coalesce(sub_county.sub_county_key, 'unset') as sub_county_key,
            'unset' as facility_key,
            indicators.indicator_key,
            moh_705_mappings.year as year,
            moh_705_mappings.month as month,
            moh_705_mappings.year_month as year_month,
            moh_705_mappings.indicator_value as indicator_value,
            'khis' as data_source
        from {{ ref("intermediate_khis_moh_705_mappings") }} as moh_705_mappings
        left join
            {{ ref("dim_county") }} as county on county.county = moh_705_mappings.county
        left join
            {{ ref("dim_sub_county") }} as sub_county
            on sub_county.sub_county = moh_705_mappings.sub_county
        left join
            {{ ref("dim_indicator") }} as indicators
            on indicators.indicator = moh_705_mappings.indicator
        left join
            {{ ref("dim_age_group_khis") }} as age_group
            on age_group.age_group_category = moh_705_mappings.age_group
        left join
            {{ ref("dim_disease") }} as disease
            on disease.disease = moh_705_mappings.disease
    )

select
    {{
        dbt_utils.surrogate_key(
            [
                "disease_key",
                "gender_key",
                "age_group_key",
                "county_key",
                "sub_county_key",
                "facility_key",
                "indicator_key",
                "indicator_value",
            ]
        )
    }} as fact_key, khis_data.*, cast(current_date as date) as load_date
from khis_data
