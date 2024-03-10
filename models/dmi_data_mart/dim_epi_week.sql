{% set start_date = '2000-01-01' %}
{% set end_date = '2040-12-31' %}

with date_range AS (
    select 
        generate_series(
            '{{ start_date }}'::date, 
            '{{ end_date }}'::date, 
            '1 day'::interval
        )::date AS date
),
dates as (
    select 
      date as date_day
    from date_range
),
epi_week_cte as (
  select
        date_day,
        date_trunc('week', date_day) - interval '1 day' as start_of_week,
        date_trunc('week', date_day) + interval '5 days' as end_of_week,
        trim(to_char(date_day, 'Day')) as weekday_name,
        date_part('year', date_day) as year,
        date_part('month', date_day) as month
  from dates
)
select
	{{ dbt_utils.surrogate_key( ['epi_week_cte.date_day']) }} as epi_week_key,
    start_of_week,
	end_of_week,
	date_part('week', date_day) as week_number,
  weekday_name,
  year,
  month,
  cast(current_date as date) as load_date
from epi_week_cte
where weekday_name = 'Sunday'