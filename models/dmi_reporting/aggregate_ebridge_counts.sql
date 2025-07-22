select
    regexp_replace(sub_county.sub_county_unit_code, '\D', '', 'g') as subcounty_code,
    concat(epi_week.year, lpad(epi_week.week_number::text, 2, '0')) as year_epi_week,
    epi_week.year,
    epi_week.week_number as epi_week,
    count_c1,
    count_c2,
    count_c3,
    count_c4,
    count_c5,
    count_c6,
    count_c7,
    count_h1,
    count_h2,
    count_h3,
    concat(county.county, ' ', 'County') as county,
    concat(sub_county.sub_county, ' ', 'Sub County') as subcounty
from {{ ref("fct_aggregate_ebridge_counts") }} as ebridge
left join
    {{ ref("dim_epi_week") }} as epi_week
    on epi_week.epi_week_key = ebridge.epi_week_key
left join {{ ref("dim_county") }} as county on county.county_key = ebridge.county_key
left join
    {{ ref("dim_sub_county") }} as sub_county
    on sub_county.sub_county_key = ebridge.sub_county_key
