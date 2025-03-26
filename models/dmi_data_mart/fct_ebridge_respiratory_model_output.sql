with
    ebridge_ml_output as (
        select * from {{ ref("intermediate_ebridge_respiratory_output") }}
    )
select
    coalesce(county.county_key, 'unset') as county_key,
    coalesce(sub_county.sub_county_key, 'unset') as sub_county_key,
    ebridge_ml_output.year_month,
    population_tbl.population,
    ebridge_ml_output.predicted_cases,
    ebridge_ml_output.predicted_diseases,
    round((predicted_cases * 52 / population_tbl.population * 100000), 2) as incidence,
    cast(current_date as date) as load_date
from ebridge_ml_output
left join
    {{ ref("sub_county_population") }} as population_tbl
    on ebridge_ml_output.subcounty = population_tbl.subcounty
left join
    {{ ref("dim_county") }} as county
    on concat(county.county, ' ', 'County') = ebridge_ml_output.county
left join
    {{ ref("dim_sub_county") }} as sub_county
    on concat(sub_county.sub_county, ' ', 'Sub County') = ebridge_ml_output.subcounty
