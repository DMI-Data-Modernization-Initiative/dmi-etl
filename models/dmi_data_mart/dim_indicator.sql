select 
    {{ tsql_utils.surrogate_key( ['indicators_source.indicator']) }} as indicator_key,
    indicator,
    indicator_description,
    cast(getdate() as date) as load_date
from {{ ref('indicators_mapping') }} as indicators_source
