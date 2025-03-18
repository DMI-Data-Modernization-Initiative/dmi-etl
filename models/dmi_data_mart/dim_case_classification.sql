with data as (
    select 'ILI' as case_classification union all 
    select  'SARI' as case_classification union all 
    select 'IUF/ILI' as case_classification union all 
    select  'IUF' as case_classification union all 
    select 'IUF/SARI' as case_classification union all 
    select 'DNS' as case_classification
),
final_data as (
    select 
        {{ dbt_utils.surrogate_key( ['data.case_classification']) }} as case_classification_key,
        case_classification,
        case case_classification
            when 'ILI' then 'Influenza-Like Illness'
            when 'SARI' then 'Severe Acute Respiratory Illness'
            when  'IUF/ILI' then ' Undifferentiated Fever /Influenza-Like Illness'
            when  'IUF' then 'Undifferentiated Fever'
            when 'IUF/SARI' then 'Undifferentiated Fever /Severe Acute Respiratory Illness'
            when 'DNS' then 'DNS'
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