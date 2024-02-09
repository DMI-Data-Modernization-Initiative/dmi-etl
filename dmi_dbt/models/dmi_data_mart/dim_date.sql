with date_spine as (
  {{- dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2000-01-01' as date)",
    end_date="cast('2040-12-31' as date)"
    )
  -}}
)
select
    {{ tsql_utils.surrogate_key( ['date_day']) }} as DateKey,
    cast(date_day as date) as date,
    year(date_day) as Year,
    month(date_day) Month,
    datepart(quarter, date_day) as CalendarQuarter
from date_spine
