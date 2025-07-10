with model_output_data as (
	select 
		county,
		sub_county,
		regexp_replace(epi_week , '([^0-9]*)([0-9]+).*', '\2')::int as epi_week,
		right(epi_week, 4)::int as year,
		sub_county_iso_code,
		county_iso_code,
		population,
		predicted_cases,
		incidence,
		predicted_diseases
	from {{ ref('ebridge_predictive_model_output') }} as model_output
)
select
	model_output_data.county,
	model_output_data.sub_county,
	model_output_data.epi_week,
	model_output_data.year,
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
left join {{ ref('aggregate_moh_505_diarrheal_diseases_report') }} as moh_505 on moh_505.county = model_output_data.county 
	and moh_505.sub_county  = model_output_data.sub_county 
	and moh_505.epi_week = model_output_data.epi_week
	and moh_505.year = model_output_data.year
where predicted_diseases = 'Diarrhoeal diseases'