select
    county,
    count(*) as alert_count
from {{ref('stg_krcs_cbs')}} as krcs
group by county