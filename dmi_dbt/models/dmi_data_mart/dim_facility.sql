select 
    distinct 
    {{ tsql_utils.surrogate_key( ['sari_data.facility_name']) }} as facility_key,
    facility_name,
    longitude,
    latitude
from {{ ref('stg_sari_ili') }} as sari_data