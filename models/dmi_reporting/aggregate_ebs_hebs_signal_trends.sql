SELECT date.date,
date.year,
date.month,
 epi_week.week_number,
concat(county.county, ' ', 'County') as county,
  hebs."SIGNALID",
  hebs."COUNTY",
  hebs."HEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING",
  hebs."HEBS_INVESTIGATIONFORM_ISEVENTINFECTIOUS",
  hebs."HEBS_INVESTIGATIONFORM_ID",
  hebs."CREATED_DATE",
  hebs."SIGNAL",
  hebs. "SIGNAL_TYPE"
from {{ ref('fct_ebs_hebs_signal_trends') }} as hebs
left join {{ ref('dim_date') }} as date on date.date_key = hebs.date_key
left join {{ ref('dim_county') }}  as county on county.county_key = hebs.county_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = hebs.epi_week_key