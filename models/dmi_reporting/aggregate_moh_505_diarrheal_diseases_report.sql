with diarrheal_diseases  as (
	select 
		sub_county,
		county,
        epi_week,
		year,
		sum(case when disease = 'Typhoid fever' then indicator_value else 0 end) as typhoid_fever_cases,
		sum(case when disease = 'Dysentery' then indicator_value else 0 end) as dysentery_cases,
		sum(case when disease = 'Cholera' then indicator_value else 0 end) as cholera_cases
	from  {{ ref('aggregate_moh_505_report') }}
	where disease in ('Typhoid fever', 'Dysentery', 'Cholera')
		and indicator_description = 'Number of cases reported'
	group by
		sub_county,
		county,
        epi_week,
		year
)
select 
	diarrheal_diseases.*,
	typhoid_fever_cases + dysentery_cases + cholera_cases as total_cases
from diarrheal_diseases
where typhoid_fever_cases + dysentery_cases + cholera_cases > 0