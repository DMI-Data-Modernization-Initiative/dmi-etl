select 
	concat(county.county, ' ', 'County') as county,
    concat(sub_county.sub_county, ' ', 'Sub County') as sub_county,
    concat('W', epi_week.week_number, ' ', epi_week.year) as epi_week,
	model_output.population,
	model_output.predicted_cases,
	model_output.incidence,
	model_output.predicted_diseases,
	cast(current_date as date) as load_date
from {{ ref('fct_ebridge_predictive_model_output') }} as model_output
left join {{ ref('dim_county') }} as county on county.county_key = model_output.county_key
left join{{ ref('dim_sub_county') }} as sub_county on sub_county.sub_county_key = model_output.sub_county_key
left join{{ ref('dim_epi_week') }} as epi_week on epi_week.epi_week_key = model_output.epi_week_key