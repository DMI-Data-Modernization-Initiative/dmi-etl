
with gender_code as (
  select 0 as code union all 
  select 1 as code union all
  select 2 as code 
 ),
 final_data as (
  select
    {{ dbt_utils.surrogate_key( ['gender_code.code']) }} as gender_key,
    code,
    /* taking care of where Male = 0 and Female = 1 & Male = 1 and Female = 2 so we have two gender columns: gender and gender_2 */
    case code
        when 1 then 'Female'
      when 0 then 'Male'
    end as gender,
    case code
      when 1 then 'Male'
      when 2 then 'Female'
    end as gender_2
  from gender_code

  union 

  select
    'unset' as gender_key,
    -999 as code,
    'unset' as gender,
    'unset' as gender_2
)
select 
  final_data.*,
  cast(current_date as date) as load_date
from final_data

