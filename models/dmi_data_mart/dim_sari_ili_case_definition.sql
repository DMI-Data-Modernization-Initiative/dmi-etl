with case_def as (
  select 1 as code union all
  select 2 as code 
 ),
 final_data as (
  select
    {{ dbt_utils.surrogate_key( ['case_def.code']) }} as case_definition_key,
    code,
    case code
        when 1 then 'SARI'
        when 2 then 'ILI'
    end as case_definition
  from case_def
  union 

  select
    'unset' as case_definition_key,
    -999 as code,
    'unset' as case_definition
)
select 
  final_data.*,
  cast(current_date as date) as load_date
from final_data