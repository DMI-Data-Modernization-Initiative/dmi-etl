with date_spine as (
  {{- dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2000-01-01' as date)",
    end_date="cast('2040-12-31' as date)"
    )
  -}}
),
epi_week_cte as (
    select
        date_day,
        dateadd(day, 1 - datepart(weekday, date_day), date_day) as start_of_week,
        dateadd(day, 7 - datepart(weekday, date_day), date_day) as end_of_week,
        datename(weekday, date_day) as weekday,
        year(date_day) as year,
        month(date_day) as month
    from  date_spine
)
select
	{{ tsql_utils.surrogate_key( ['epi_week_cte.date_day']) }} as epi_week_key,
    start_of_week,
	end_of_week,
	datepart(week, date_day) as week_number,
    year,
    month
from epi_week_cte
where WeekDay = 'Sunday'