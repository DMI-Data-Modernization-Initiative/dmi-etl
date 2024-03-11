select 
	distinct {{ dbt_utils.surrogate_key( ['unit_type_source.unit_type']) }} as unit_type_key,
    unit_type,
    cast(current_date as date) as load_date 
from {{ref('stg_mdharura_ebs_aggregate') }} as unit_type_source
