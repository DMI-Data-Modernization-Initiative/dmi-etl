
select
    {{ tsql_utils.surrogate_key( ['source_diseases.Disease']) }} as disease_key,
	disease,
	threshold,
	cast(getdate() as date) as load_date
from {{ ref('source_diseases') }} as source_diseases
