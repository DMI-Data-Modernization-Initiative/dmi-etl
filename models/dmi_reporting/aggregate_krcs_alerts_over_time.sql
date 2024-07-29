select
    date_trunc('month', cbs_date) as month,
    count(*) as alert_count
from {{ ref('stg_krcs_cbs') }} as krcs
group by date_trunc('month', cbs_date)
order by month