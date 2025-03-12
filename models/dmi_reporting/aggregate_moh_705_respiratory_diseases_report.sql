with
    respiratory_diseases as (
        select
            sub_county,
            county,
            year_month,
            year,
            month,
            sum(
                case when disease = 'S.pneumoniae' then indicator_value else 0 end
            ) as pneuomonia_cases,

            sum(
                case
                    when disease = 'Upper Respiratory Tract Infections'
                    then indicator_value
                    else 0
                end
            ) as upper_respiratory_infections_cases
        from {{ ref("aggregate_moh_705_report") }}
        where
            disease in ('S.pneumoniae', 'Upper Respiratory Tract Infections')
            and indicator_description = 'Number of cases reported'
        group by sub_county, county, year, month, year_month
    )
select
    respiratory_diseases.*,
    pneuomonia_cases + upper_respiratory_infections_cases as total_cases
from respiratory_diseases
where pneuomonia_cases + upper_respiratory_infections_cases > 0
