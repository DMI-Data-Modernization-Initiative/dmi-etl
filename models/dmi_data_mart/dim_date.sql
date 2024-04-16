with date_spine as (
  {{- dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2000-01-01' as date)",
    end_date="cast('2040-12-31' as date)"
    )
  -}}
),
final_data as (
  select
      {{ dbt_utils.surrogate_key( ['date_day']) }} as date_key,
      cast(date_day as date) as date,
      date_part('year', date_day) as Year,
      date_part('month', date_day) as Month,
      date_part('quarter', date_day) as CalendarQuarter
  from date_spine

  union 
    select 
      'unset' as date_key,
      '1900-01-01'::date as date,
      -999 as Year,
      -999 as Month,
      -999 CalendarQuarter
)
select
  final_data.*,
  cast(current_date as date) as load_date
from final_data
