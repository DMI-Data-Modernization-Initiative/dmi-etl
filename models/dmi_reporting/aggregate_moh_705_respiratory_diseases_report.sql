with
    respiratory_diseases as (
        select
            sub_county,
            county,
            year_month,
            year,
            month,
            sum(
                case
                    when indicator_name = 'Pneumonia <5 yrs' then indicator_value else 0
                end
            ) as "pneuomonia<5",

            sum(
                case
                    when indicator_name = 'Pneumonia >5 yrs' then indicator_value else 0
                end
            ) as "pneuomonia>5",

            sum(
                case
                    when indicator_name = 'Severe pneumonia <5 yrs'
                    then indicator_value
                    else 0
                end
            ) as "severe_pneuomonia<5",

            sum(
                case
                    when indicator_name = 'Severe pneumonia >5 yrs'
                    then indicator_value
                    else 0
                end
            ) as "severe_pneuomonia>5",

            sum(
                case
                    when indicator_name = 'Rev 2020 Pneumonia <5 yrs'
                    then indicator_value
                    else 0
                end
            ) as "rev_pneuomonia<5",

            sum(
                case
                    when indicator_name = 'Rev 2020 Pneumonia >5 yrs'
                    then indicator_value
                    else 0
                end
            ) as "rev_pneuomonia>5",

            sum(
                case
                    when indicator_name = 'Lower Respiratory Tract Infections <5 yrs'
                    then indicator_value
                    else 0
                end
            ) as "lrti<5",

            sum(
                case
                    when indicator_name = 'Lower Respiratory Tract Infections >5 yrs'
                    then indicator_value
                    else 0
                end
            ) as "lrti>5",

            sum(
                case
                    when indicator_name = 'Upper Respiratory Tract Infections <5 yrs'
                    then indicator_value
                    else 0
                end
            ) as "urti<5",

            sum(
                case
                    when indicator_name = 'Upper Respiratory Tract Infections >5 yrs'
                    then indicator_value
                    else 0
                end
            ) as "urti>5",

            sum(
                case when disease = 'S.pneumoniae' then indicator_value else 0 end
            ) as pneuomonia_cases,

            sum(
                case
                    when disease = 'Upper Respiratory Tract Infections'
                    then indicator_value
                    else 0
                end
            ) as upper_respiratory_infections_cases,

            sum(
                case
                    when disease = 'Lower Respiratory Tract Infections'
                    then indicator_value
                    else 0
                end
            ) as lower_respiratory_infections_cases
        from {{ ref("aggregate_moh_705_report") }}
        where
            disease in (
                'S.pneumoniae',
                'Upper Respiratory Tract Infections',
                'Lower Respiratory Tract Infections'
            )
            and indicator_description = 'Number of cases reported'
        group by sub_county, county, year, month, year_month
    )
select
    respiratory_diseases.*,
    pneuomonia_cases + upper_respiratory_infections_cases as total_cases
from respiratory_diseases
where pneuomonia_cases + upper_respiratory_infections_cases > 0
