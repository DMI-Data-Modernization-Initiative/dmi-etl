select
    regexp_replace(sub_county.sub_county_unit_code, '\D', '', 'g') as subcounty_code,
    year_month,
    year,
    month,
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
from {{ ref("fct_aggregate_ebridge_counts_year_month") }} as ebridge
left join {{ ref("dim_county") }} as county on county.county_key = ebridge.county_key
left join
    {{ ref("dim_sub_county") }} as sub_county
    on sub_county.sub_county_key = ebridge.sub_county_key
