 select 
    {{ tsql_utils.surrogate_key( ['county_source.county']) }} as county_key,
    county_source.*
 from {{ ref('kenya_counties') }} as county_source