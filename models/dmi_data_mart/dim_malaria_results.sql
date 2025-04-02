
with tac_data as (
select 
    distinct
	 case
	   when p_falciparum = 'POS' and p_vivax <> 'POS' then 'P.falciparum'
	   when p_falciparum <> 'POS' and p_vivax = 'POS' then 'P.vivax'
	   when p_falciparum = 'POS' and p_vivax = 'POS' then 'Mixed infection'
	   when plasmodium = 'POS' and p_falciparum <> 'POS' and p_vivax <> 'POS' then 'Other plasmodium'
	 end as malaria_pos_category
	from {{ ref('intermediate_tac_results') }}
),
data as (
	select
		case malaria_pos_category
			when 'P.falciparum' then 1
			when 'Mixed infection' then 5
		end as code,
		malaria_pos_category
	from tac_data

    union 

	select 
		distinct
		"malariares"::int as code,
		case "malariares"::int 
			when 1 then 'P.falciparum'
			when 2 then 'Pan malaria'
			when 3 then 'Mixed species'
            when 4 then 'Non faciparum species'
		end as malaria_pos_category
	from {{ ref('stg_afi_surveillance')  }}
),
final_data as (
select
    {{ dbt_utils.surrogate_key( ['data.malaria_pos_category']) }} as malaria_pos_category_key,
	code,
    malaria_pos_category
from data
where data.malaria_pos_category is not null

union

select 
    'unset' as outcome_key,
	-999 as code,
    'unset' as malaria_pos_category
)
select 
    final_data.*,
    cast(current_date as date) as load_date
from final_data