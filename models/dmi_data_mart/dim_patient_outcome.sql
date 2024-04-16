with outcome_data as (
    select 
        distinct 
        {{ dbt_utils.surrogate_key( ['sari_ili.outcome']) }} as outcome_key,
        outcome,
        case outcome
            when 1 then 'Discharge from Hospital alive' 
            when 2 then 'Death'
            when 3 then 'Refused Hospital Treatment'
            when 4 then 'Absconded'
            when 5 then 'Refered to another facility'
        end as patient_outcome
    from dbt_shield_dev.stg_sari_ili as sari_ili
    where outcome in (1, 2, 3, 4, 5)

    union 

    select 
        'unset' as outcome_key,
        -999 as outcome,
        'unset' as patient_outcome
)
select 
    outcome_data.*,
    cast(current_date as date) as load_date
from outcome_data