select 
    *
from {{ source('staging', 'stg_ebridge_output') }}