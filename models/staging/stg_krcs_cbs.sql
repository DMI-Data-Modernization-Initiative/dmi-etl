with raw_krcs as (
    select
        cbs_date,
        volunteer_id,
        alert,
        alert_village_id,
        project,
        volunteer_name,
        volunteer_village_id,
        county,
        alert_village_name,
        alert_status,
        alert_details,
        id_mismatch,
        confirmed,
        investigated,
        investigation_date,
        outcome,
        action_red_cross,
        action_government,
        supervisor_name,
        supervisor_phone
    from {{ source('central_raw_krcs', 'cbs') }}
)

select
    cbs_date,
    volunteer_id,
    alert,
    alert_village_id,
    project,
    volunteer_name,
    volunteer_village_id,
    county,
    alert_village_name,
    alert_status,
    alert_details,
    id_mismatch,
    confirmed,
    investigated,
    investigation_date,
    outcome,
    action_red_cross,
    action_government,
    supervisor_name,
    supervisor_phone
from raw_krcs
