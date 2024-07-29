select
    outcome,
    count(*) as outcome_count
from {{ ref('stg_krcs_cbs') }} as krcs
group by outcome