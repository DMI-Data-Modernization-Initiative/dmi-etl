 with sub_county_data as (
 select 
    {{ tsql_utils.surrogate_key( ['sub_county_source.sub_county']) }} as sub_county_key,
    sub_county_source.sub_county,
    sub_county_source.code,
    sub_county_source.sqkm,
    dim_county.county_key
 from {{ ref('kenya_sub_counties') }} as sub_county_source
 left join {{ ref('dim_county') }} as dim_county on dim_county.county = sub_county_source.county

 union

 select 
   'unset' as sub_county_key,
   'unset' as sub_county,
   'unset' as code,
   -999 as sqkm,
   'unset' as county_key
 )
select 
   sub_county_data.*,
   cast(getdate() as date) as load_date
from sub_county_data
