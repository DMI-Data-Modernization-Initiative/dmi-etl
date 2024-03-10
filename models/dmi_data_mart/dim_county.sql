 with source_data as (
   select 
      {{ dbt_utils.surrogate_key( ['county_source.county']) }} as county_key,
      county_source.*,
      cast(current_date as date) as load_date
   from {{ ref('kenya_counties') }} as county_source

   union 

   select 
      'unset' as county_key,
      'unset' as county,
      'unset' as code,
      -999 as area_sqkm,
      cast(current_date as date) as load_date
 )
 select 
   * 
from source_data