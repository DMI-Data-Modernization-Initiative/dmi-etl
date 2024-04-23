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
  		distinct
  		/* getting the start of week as Sunday and end of week as Sarturday */
        date_trunc('week', date_day) - interval '1 day' as start_of_week,
        date_trunc('week', date_day) + interval '5 days' as end_of_week
  from dates
 ),
summary as (
 select
 	start_of_week::date,
 	end_of_week::date,
 	trim(to_char(start_of_week, 'Day')) as start_week_day_name,
 	trim(to_char(end_of_week, 'Day')) as end_week_day_name,
 	/* if week number of end of week is 52 or 53 and month of end of week = 1 then put year as minus 1 */
 	case 
 		when date_part('month', end_of_week) = 1 and date_part('week', end_of_week) in (52,53) then date_part('year', end_of_week) - 1
 		else date_part('year', end_of_week)
 	end as year,
 	date_part('week', end_of_week) as week_number
 from epi_week_cte
),
final_data as (
  select 
    {{ dbt_utils.surrogate_key( ['summary.start_of_week']) }} as epi_week_key,
    start_of_week,
    end_of_week,
    week_number,
    start_week_day_name,
    end_week_day_name,
    year
  from summary

  union 

  select
    'unset' as epi_week_key,
    '1900-01-01'::date as start_of_week,
    '1900-01-01'::date as end_of_week,
    -999 as week_number,
    'unset' as start_week_day_name,
    'unset' as end_week_day_name,
    -999 as year
)
select 
  final_data.*,
  cast(current_date as date) as load_date
from final_data

