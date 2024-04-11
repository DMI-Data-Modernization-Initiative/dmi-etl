with ebridge_ml_output as (
	select * from {{ ref('intermediate_ebridge_output') }}
)
select 
	coalesce(county.county_key, 'unset') as county_key,
    coalesce(sub_county.sub_county_key, 'unset') as sub_county_key,
    coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
	population_tbl.population,
	ebridge_ml_output.predicted_cases,
	round((predicted_cases * 52/population_tbl.population * 100000), 2) as incidence,
	cast(current_date as date) as load_date
from ebridge_ml_output
left join {{ ref('sub_county_population') }} as population_tbl on ebridge_ml_output.subcountyid = population_tbl.subcounty_id
left join {{ ref('dim_county') }} as county on concat(county.county, ' ', 'County')  = ebridge_ml_output.county
left join {{ ref('dim_sub_county') }} as sub_county on concat(sub_county.sub_county, ' ', 'Sub County') = ebridge_ml_output.subcounty
left join {{ ref('dim_epi_week') }} as epi_week on epi_week.week_number = substring(ebridge_ml_output.epiweek from 'W(\d+)')::int
	and epi_week.year = substring(ebridge_ml_output.epiweek from '\d{4}$')::int