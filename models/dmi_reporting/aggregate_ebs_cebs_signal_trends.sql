SELECT date.date,
date.year,
date.month,
 epi_week.week_number,
concat(county.county, ' ', 'County') as county,
  cebs."SIGNALID",
  cebs."COUNTY",
  cebs."CEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING",
  cebs."CEBS_INVESTIGATIONFORM_ISEVENTINFECTIOUS",
  cebs."CEBS_INVESTIGATIONFORM_ID",
  cebs."CREATED_DATE",
  cebs."SIGNAL",
  cebs. "SIGNAL_TYPE"
from {{ ref('fct_ebs_cebs_signal_trends') }} as cebs
left join {{ ref('dim_date') }} as date on date.date_key = cebs.date_key
left join {{ ref('dim_county') }}  as county on county.county_key = cebs.county_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = cebs.epi_week_key