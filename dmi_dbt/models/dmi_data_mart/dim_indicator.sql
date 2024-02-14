select 
    {{ tsql_utils.surrogate_key( ['indicators_source.indicator']) }} as indicator_key,
    indicator,
    indicator_description
from {{ ref('indicators_mapping') }} as indicators_source
