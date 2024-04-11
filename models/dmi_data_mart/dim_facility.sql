with facility_data as (
    select 
        distinct 
        {{ dbt_utils.surrogate_key( ['facility_mapping.facility_name']) }} as facility_key,
        mfl_code,
        facility_name,
        facility_name_short,
        longitude,
        latitude
    from {{ ref('facility_mapping') }} as facility_mapping

    union 

    select 
        'unset' as facility_key,
        -999 as mfl_code,
        'unset' as facility_name,
        'unset' as facility_name_short,
        -999 as longitude,
        -999 as latitude
)
select 
    facility_data.*,
    cast(current_date as date) as load_date
from facility_data