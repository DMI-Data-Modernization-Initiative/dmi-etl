
with gender_code as (
  select 0 as code union all 
  select 1 as code
 ),
 final_data as (
  select
    {{ dbt_utils.surrogate_key( ['gender_code.code']) }} as gender_key,
    code,
    case code
        when 1 then 'Female'
      when 0 then 'Male'
    end as gender
  from gender_code

  union 

  select
    'unset' as gender_key,
    -999 as code,
    'unset' as gender
  from gender_code
)
select 
  final_data.*,
  cast(current_date as date) as load_date
from final_data

