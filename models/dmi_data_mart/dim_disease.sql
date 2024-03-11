
select
    {{ dbt_utils.surrogate_key( ['source_diseases.Disease']) }} as disease_key,
	disease,
	threshold,
	cast(current_date as date) as load_date
from {{ ref('source_diseases') }} as source_diseases
