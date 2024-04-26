with data as (
    select 'SARI' as case_classification union all 
    select 'UF' as case_classification union all 
    select 'DF' as case_classification union all 
    select 'MERS-CoV' as case_classification
),
final_data as (
    select 
        {{ dbt_utils.surrogate_key( ['data.case_classification']) }} as case_classification_key,
        case_classification,
        case case_classification
            when 'SARI' then 'Severe Acute Respiratory Illness'
            when 'UF' then 'Undifferentiated Fever'
            when  'DF' then 'Diarrhoea Fever'
            when 'MERS-CoV' then 'Middle East respiratory syndrome coronavirus'
        end as case_classification_full_name
    from data 

    union 

    select 
    'unset' as case_classification_key,
    'unset' as case_classification,
    'unset' as case_classification_full_name
)
select 
    final_data.*,
    cast(current_date as date) as load_date
from final_data