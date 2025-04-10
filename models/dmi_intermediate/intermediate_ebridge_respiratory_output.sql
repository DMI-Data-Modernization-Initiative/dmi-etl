select * from {{ source("staging", "stg_ebridge_output_respiratory_illness") }}
