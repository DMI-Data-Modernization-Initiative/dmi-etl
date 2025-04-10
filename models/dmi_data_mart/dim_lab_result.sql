
with lab_result_code as (
  select 1 as code union all
  select 2 as code 
  
 ),
 final_data as (
  select
    {{ dbt_utils.surrogate_key( ['lab_result_code.code']) }} as lab_result_key,
    code,
    case code
        when 1 then 'Positive'
        when 2 then 'Negative'
    end as lab_result,
    case code 
      when 1 then 'POS'
      when 2 then 'NEG'
    end as lab_result_2
  from lab_result_code

  union 

  select
    'unset' as lab_result_key,
    -999 as code,
    'unset' as lab_result,
    'unset' as lab_result_2
)
select 
  final_data.*,
  cast(current_date as date) as load_date
from final_data

