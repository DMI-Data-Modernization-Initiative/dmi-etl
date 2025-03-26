with
    model_output_data as (
        select
            county,
            sub_county,
            year_month,
            substring(year_month::text, 1, 4) as year,
            substring(year_month::text, 5, 2) as month,
            sub_county_iso_code,
            county_iso_code,
            population,
            predicted_cases,
            incidence,
            predicted_diseases
        from {{ ref("ebridge_respiratory_model_output") }} as model_output
    )
select
    model_output_data.county,
    model_output_data.sub_county,
    model_output_data.year,
    model_output_data.month,
    model_output_data.year_month,
    model_output_data.sub_county_iso_code,
    model_output_data.county_iso_code,
    model_output_data.population,
    model_output_data.predicted_cases,
    model_output_data.incidence,
    model_output_data.predicted_diseases,
    moh_705.pneuomonia_cases,
    moh_705.upper_respiratory_infections_cases,
    moh_705.total_cases
from model_output_data
left join
    {{ ref("aggregate_moh_705_respiratory_diseases_report") }} as moh_705
    on moh_705.county = model_output_data.county
    and moh_705.sub_county = model_output_data.sub_county
    and moh_705.year_month::int = model_output_data.year_month
where predicted_diseases = 'Respiratory Illness'
