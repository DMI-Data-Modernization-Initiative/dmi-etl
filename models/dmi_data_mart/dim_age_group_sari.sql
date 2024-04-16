with age_group_data as (
    select   
        start_age,
        end_age,
        case 
            when start_age >= 0 and start_age < 0.5 then '< 6 Months'
            when start_age >= 0.5 and start_age < 2 then '6 - 23 months'
            when start_age >=  2 and start_age < 5 then '2 - 4 Years'
            when start_age >= 5 and start_age < 15 then '5 - 14 Years'
            when start_age >= 15 then '15 Years and Above'
        end as age_group_category
    from
        (values
            (0, 0.5),
            (0.5, 2),
            (2, 5),
            (5, 15),
            (15, 130)       
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
