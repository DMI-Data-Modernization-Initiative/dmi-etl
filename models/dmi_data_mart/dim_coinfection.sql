with final_data as (
select 
	distinct 
    {{ dbt_utils.surrogate_key( ['tac_results.co_infection']) }} as co_infection_key,
    co_infection
from {{ ref('intermediate_tac_results') }} as tac_results

union 

select 
    'unset' as co_infection_key,
	'unset' as co_infection
)
select 
    final_data.*,
    cast(current_date as date) as load_date
from final_data