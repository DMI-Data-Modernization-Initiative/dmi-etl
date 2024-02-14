 select 
    {{ tsql_utils.surrogate_key( ['sub_county_source.sub_county']) }} as sub_county_key,
    sub_county_source.sub_county,
    sub_county_source.code,
    sub_county_source.sqkm,
    dim_county.county_key
 from {{ ref('kenya_sub_counties') }} as sub_county_source
 left join {{ ref('dim_county') }} as dim_county on dim_county.county = sub_county_source.county