with facility_data as (
    select 
        distinct 
        {{ dbt_utils.surrogate_key( ['sari_data.facility_name']) }} as facility_key,
        facility_name,
        longitude,
        latitude
    from {{ ref('stg_sari_ili') }} as sari_data

    union 
    select 
        'unset' as facility_key,
        'unset' as facility_name,
        -999 as longitude,
        -999 as latitude
)
select 
    facility_data.*,
    cast(current_date as date) as load_date
from facility_data