with
    model_output_data as (
        select
            county,
            sub_county,
            epi_week,
            year,
            year_epi_week,
            sub_county_iso_code,
            county_iso_code,
            population,
            predicted_cases,
            incidence,
            predicted_diseases
        from {{ ref("ebridge_diarrhoeal_model_output") }} as model_output
    )
select
    model_output_data.county,
    model_output_data.sub_county,
    model_output_data.epi_week,
    model_output_data.year,
    model_output_data.year_epi_week,
    model_output_data.sub_county_iso_code,
    model_output_data.county_iso_code,
    model_output_data.population,
    model_output_data.predicted_cases,
    model_output_data.incidence,
    model_output_data.predicted_diseases,
    moh_505.typhoid_fever_cases,
    moh_505.dysentery_cases,
    moh_505.cholera_cases,
    moh_505.total_cases
from model_output_data
left join
    {{ ref("aggregate_moh_505_diarrheal_diseases_report") }} as moh_505
    on moh_505.county = model_output_data.county
    and moh_505.sub_county = model_output_data.sub_county
    and moh_505.year_epi_week = model_output_data.year_epi_week
where predicted_diseases = 'Diarrhoeal Diseases'
