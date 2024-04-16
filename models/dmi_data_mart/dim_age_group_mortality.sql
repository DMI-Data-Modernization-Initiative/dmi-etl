with age_group_data as (
    select   
        start_age,
        end_age,
        case
            when start_age >= 0 and start_age < 1 then '0 - < 1yr'
            when start_age >= 1 and start_age < 2 then '1 - < 2yrs'
            when start_age >= 2 and start_age < 5 then '2 - < 5yrs'
            when start_age >= 5 and start_age <= 17 then '5 - 17yrs'
            when start_age >= 18 and start_age <= 50 then '18 - 50yrs'
            when start_age > 50 then '51+ yrs'
        end as age_group_category
    from
        (values
            (0, 1),
            (1, 2),
            (2, 5),
            (5, 18),
            (18, 51),
            (51, 130)
        ) as age_group(start_age, end_age)	

),
final_data as (
    select 
        {{ dbt_utils.surrogate_key( ['age_group_data.age_group_category']) }} as age_group_key,
        start_age,
        end_age,
        age_group_category
    from age_group_data 

    union 

    select 
    'unset' as age_group_key,
    -999 as start_age,
    -999 as end_age,
    'unset' as age_group_category  
)
select 
    final_data.*,
    cast(current_date as date) as load_date
from final_data 