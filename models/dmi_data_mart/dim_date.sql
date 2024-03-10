with date_spine as (
  {{- dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2000-01-01' as date)",
    end_date="cast('2040-12-31' as date)"
    )
  -}}
)
select
    {{ dbt_utils.surrogate_key( ['date_day']) }} as DateKey,
    cast(date_day as date) as date,
    date_part('year', date_day) as Year,
    date_part('month', date_day) as Month,
    date_part('quarter', date_day) as CalendarQuarter,
    cast(current_date as date) as load_date
from date_spine
