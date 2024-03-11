select 
	date.date,
	county.county,
	sub_county.sub_county,
	unit.unit_type,
    sum(hebs_signals_reported) as hebs_signals_reported,
    sum(hebs_signals_verified) as hebs_signals_verified,
    sum(hebs_signals_verified_true) as hebs_signals_verified_true,
    sum(hebs_signals_risk_assessed) as hebs_signals_risk_assessed,
    sum(hebs_signals_responded) as hebs_signals_responded,
    sum(hebs_signals_to_be_escalated) as hebs_signals_to_be_escalated,
    sum(hebs_signals_escalated) as hebs_signals_escalated,
    sum(cebs_signals_reported) as cebs_signals_reported,
    sum(cebs_signals_verified) as cebs_signals_verified,
    sum(cebs_signals_verified_true) as cebs_signals_verified_true,
    sum(cebs_signals_risk_assessed) as cebs_signals_risk_assessed,
    sum(cebs_signals_responded) as cebs_signals_responded,
    sum(cebs_signals_to_be_escalated) as cebs_signals_to_be_escalated,
    sum(cebs_signals_escalated) as cebs_signals_escalated,
    sum(chvs_registered) as chvs_registered,
    sum(chvs_reporting) as chvs_reporting,
    sum(chas_registered) as chas_registered,
    sum(chas_verifying) as chas_verifying,
    sum(hcws_registered) as hcws_registered,
    sum(hcws_reporting) as hcws_reporting,
    sum(sfps_registered) as sfps_registered,
    sum(sfps_verifying) as sfps_verifying
from {{ ref('fct_aggregate_ebs_indicators') }} as ebs
left join {{ ref('dim_date') }} as date on date.date_key = ebs.date_key
left join {{ ref('dim_county') }}  as county on county.county_key = ebs.county_key 
left join {{ ref('dim_sub_county') }} as sub_county on sub_county.sub_county_key = ebs.sub_county_key
left join {{ ref('dim_unit_type') }} as unit on unit.unit_type_key  = ebs.unit_type_key
group by 
    date.date,
	county.county,
	sub_county.sub_county,
	unit.unit_type

