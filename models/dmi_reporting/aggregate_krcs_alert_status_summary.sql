select
    alert_status,
    count(*) as status_count
from {{ ref('stg_krcs_cbs') }} as krcs
group by alert_status