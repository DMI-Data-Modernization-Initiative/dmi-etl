select 
    sub_county_population.subcounty,
	sub_county_population.county,
	output.weeks,
	output.predicted,
	output.actual,
    round((predicted * 52/sub_county_population.population * 100000), 2) as incidence,
	output.predicted_diseases,
    cast(current_date as date) as load_date 
from {{ source('staging', 'stg_ebridge_output_actual_predicted') }} as output
left join {{ ref('sub_county_population') }} as sub_county_population on sub_county_population.subcounty_id = output.subcounty