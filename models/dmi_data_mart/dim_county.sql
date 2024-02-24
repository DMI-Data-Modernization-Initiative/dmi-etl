 select 
    {{ tsql_utils.surrogate_key( ['county_source.county']) }} as county_key,
    county_source.*,
    cast(getdate() as date) as load_date
 from {{ ref('kenya_counties') }} as county_source