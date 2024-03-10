with age_group_data as (
    select   
        start_age,
        end_age,
        case
            when start_age between 0 and 4 then '< 5 years'
            when end_age between 5 and 120 then '>= 5 years'
        end as age_group_category
    from
        (values
            (0, 4),
            (5, 120)
        ) as age_group(start_age, end_age)
)
select 
    {{ dbt_utils.surrogate_key( ['age_group_data.age_group_category']) }} as age_group_key,
    start_age,
    end_age,
	age_group_category,
    cast(current_date as date) as load_date
from age_group_data 

union 

 select 
   'unset' as age_group_key,
   -999 as start_age,
   -999 as end_age,
   'unset' as age_group_category,
    cast(current_date as date) as load_date
  




