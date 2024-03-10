select 
    mappings.disease,
	age_group,
	substring(period, 2, 1) as epi_week,
	right(Period, 4) as year,
	case county
          when 'Elgeyo Marakwet' then 'Elgeyo-Marakwet'
          when 'Muranga' then 'Murang''a'
          when 'Tharaka Nithi' then 'Tharaka-Nithi'
          else county 
    end as county,    
	sub_county,
    mappings.indicator,
    long_data.indicator as source_indicator_name,
	case when indicator_value ~ '^\d+$' then indicator_value::int else null end as indicator_value,  /* handle non-text values so that we have just intergers */
    cast(current_date as date) as load_date
  from {{ref('intermediate_dhis_moh_505_wide_to_long')}} as long_data
  left join {{ref('source_disease_indicator_mapppings')}} as mappings on mappings.source_indicator = long_data.indicator
  where long_data.indicator not in (
        /* ommitng this indicators as they are not clear yet... */
                'Deaths Due to Malaria <5 yrs, Cases',
				'Deaths Due to Malaria <5 yrs, Deaths',
				'Deaths Due to Malaria >5 yrs, Cases',
				'Deaths Due to Malaria >5 yrs, Deaths'
            )
        and 
            case when indicator_value ~ '^\d+$' then indicator_value::int else null end is not null /* handle non-text values so that we have just intergers */
