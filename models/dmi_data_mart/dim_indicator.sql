select 
    {{ dbt_utils.surrogate_key( ['indicators_source.indicator']) }} as indicator_key,
    indicator,
    indicator_description,
    cast(current_date as date) as load_date
from {{ ref('indicators_mapping') }} as indicators_source
