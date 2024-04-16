with data as (
    select 
        {{ dbt_utils.surrogate_key( ['indicators_source.indicator']) }} as indicator_key,
        indicator,
        indicator_description
    from {{ ref('indicators_mapping') }} as indicators_source

    union 

    select 
        'unset' as indicator_key,
        'unset' as indicator,
        'unset' as indicator_description
)
select 
    data.*,
    cast(current_date as date) as load_date
from data 