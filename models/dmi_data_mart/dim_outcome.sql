with afi_outcome as (
    select 
        distinct "Outcome" as outcome_code
    from {{ ref('stg_afi_clinical_course_outcome') }}
    where "Outcome" in (1,2,3,4,5,6)
 ),
 final_data as (
    select
        {{ dbt_utils.surrogate_key( ['afi_outcome.outcome_code']) }} as outcome_key,
        outcome_code,
        case outcome_code
            when 1 then 'Discharged home in stable condition'
            when 2 then 'Discharged home in critical condition'
            when 3 then 'Discharged home against medical advice'
            when 4 then 'Transfered to another hospital'
            when 5 then 'Absconded'
            when 6 then 'Died'
        end as outcome
    from afi_outcome
    union 
    select
        'unset' as outcome_key,
        -999 as outcome_code,
        'unset' as outcome
)
select 
  final_data.*,
  cast(current_date as date) as load_date
from final_data