select
    alert_village_name,
    count(*) as alert_count
from {{ ref('stg_krcs_cbs') }} as krcs
group by alert_village_name
order by alert_count desc