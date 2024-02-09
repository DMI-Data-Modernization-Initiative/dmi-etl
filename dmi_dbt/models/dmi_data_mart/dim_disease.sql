
select
    {{ tsql_utils.surrogate_key( ['source_diseases.Disease']) }} as disease_key,
	disease,
	threshold
from {{ ref('source_diseases') }} as source_diseases
