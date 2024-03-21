select 
	date.date_key,
	coalesce(county.county_key, 'unset') as county_key,
	coalesce(sub_county.sub_county_key, 'unset') as sub_county_key,
	unit.unit_type_key,
    hebs_signals_reported,
    hebs_signals_verified,
    hebs_signals_verified_true,
    hebs_signals_risk_assessed,
    hebs_signals_responded,
    hebs_signals_to_be_escalated,
    hebs_signals_escalated,
    cebs_signals_reported,
    cebs_signals_verified,
    cebs_signals_verified_true,
    cebs_signals_risk_assessed,
    cebs_signals_responded,
    cebs_signals_to_be_escalated,
    cebs_signals_escalated,
    chvs_registered,
    chvs_reporting,
    chas_registered,
    chas_verifying,
    hcws_registered,
    hcws_reporting,
    sfps_registered,
    sfps_verifying
from {{ ref('stg_mdharura_ebs_aggregate') }} as ebs
left join {{ ref('dim_date') }} as date on date.date = ebs.date_start
left join {{ ref('dim_county') }}  as county on concat(county.county, ' ', 'County') = ebs.county 
left join {{ ref('dim_sub_county') }} as sub_county on concat(sub_county.sub_county, ' ', 'Sub County') = ebs.subcounty
left join {{ ref('dim_unit_type') }} as unit on unit.unit_type  = ebs.unit_type
