select
    disease,
    gender,
    age_group_category,
    epi_week,
    year,
    start_of_epi_week,
    end_of_epi_week,
    concat(county, ' ', 'Sub County') as county,
    facility_name,
    sum(case when indicator_description = 'Number of positive test results for SARS-COV2' then indicator_value else 0 end) as "Number of positive test results for SARS-COV2",
    sum(case when indicator_description = 'Number of  tests or specimens/samples' then indicator_value else 0 end) as "Number of  tests or specimens/samples",
    sum(case when indicator_description = 'Number of specimens tested positive for Influenza B (Victoria)' then indicator_value else 0 end) as "Number of specimens tested positive for Influenza B (Victoria)",
    sum(case when indicator_description = 'Number of specimens tested positive for Influenza A (1Pan H1 and 1 H3)' then indicator_value else 0 end) as "Number of specimens tested positive for Influenza A (1Pan H1 and 1 H3)",
    sum(case when indicator_description = 'Number of negative test results for SARS-COV2' then indicator_value else 0 end) as "Number of negative test results for SARS-COV2",
    sum(case when indicator_description = 'Number of ILI specimens/samples' then indicator_value else 0 end) as "Number of ILI specimens/samples",
    load_date
from (
    SELECT 
        disease.disease,
        moh_505.gender,
        age_group_category,
        epi_wk.week_number as epi_week,
        epi_wk.year,
        CAST(epi_wk.start_of_week as date) as start_of_epi_week,
        CAST(epi_wk.end_of_week as date) as end_of_epi_week,
        county.county,
        facility.facility_name,
        indicator.indicator_description,
        moh_505.indicator_value,
        moh_505.data_source,
        CAST(current_date as date) as load_date
    from {{ ref('fct_aggregate_moh_505') }} as moh_505
    left join {{ ref('dim_age_group') }} as age_group on age_group.age_group_key = moh_505.age_group_key
    left join {{ ref('dim_epi_week') }} as epi_wk on epi_wk.epi_week_key = moh_505.epi_week_key
    left join {{ ref('dim_county') }} as county on county.county_key = moh_505.county_key
    left join {{ ref('dim_disease') }} as disease on disease.disease_key = moh_505.disease_key
    left join {{ ref('dim_facility') }} as facility on facility.facility_key = moh_505.facility_key
    left join {{ ref('dim_indicator') }} as indicator on indicator.indicator_key = moh_505.indicator_key
) as long_format
group by
    disease,
    gender,
    age_group_category,
    epi_week,
    year,
    start_of_epi_week,
    end_of_epi_week,
    county,
    facility_name,
    load_date
