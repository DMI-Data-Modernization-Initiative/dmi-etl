select
    mappings.disease,
    age_group,
    year as year,
    month as month,
    concat(year, month) as year_month,
    trim(county) as county,
    trim(sub_county) as sub_county,
    mappings.indicator,
    long_data.indicator as source_indicator_name,
    case
        when indicator_value ~ '^\d+$' then indicator_value::int else null
    end as indicator_value,
    cast(current_date as date) as load_date
from {{ ref("intermediate_khis_moh_705_wide_to_long") }} as long_data
left join
    {{ ref("source_disease_indicator_mapppings") }} as mappings
    on mappings.source_indicator = long_data.indicator
where
    case when indicator_value ~ '^\d+$' then indicator_value::int else null end
    is not null
