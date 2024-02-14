with influenza_data as (
	select  
		'Influenza' as disease, 
		sari_data.*
	from {{ ref('stg_sari_ili') }} as sari_data
),
joined_data as (
	select
		disease.disease_key,
		age_group.age_group_key,
		epi_wk.epi_week_key,
		county.county_key,
        facility.facility_key,
        sari.gender,
		sari.h3n2,
		sari.ph1n1,
		victoria,
		covidpos,
		flutest,
		flua_positive,
		flub_positive
	from influenza_data as sari
	left join {{ ref('dim_age_group') }} as age_group on sari.age_in_years between age_group.start_age and age_group.end_age 
	left join {{ ref('dim_epi_week') }} as epi_wk on sari.date_collected between epi_wk.start_of_week and epi_wk.end_of_week
	left join {{ ref('dim_county') }} as county on county.county =  sari.county
	left join {{ ref('dim_disease') }} as disease on disease.disease = sari.disease
    left join {{ref('dim_facility')}} as facility on facility.facility_name = sari.facility_name
),
influenza_summary as (
	select
		disease_key,
		gender,
		age_group_key,
		county_key,
		epi_week_key,
        facility_key,
		sum(case when h3n2 = '1' or ph1n1 = '1' then 1 else 0 end) as no_of_specimen_positive_influenza_a,
		sum(case when victoria = '1' then 1 else 0 end) as no_of_specimen_positive_influenza_b,
		sum(case when covidpos = '1' then 1 else 0 end) as no_of_specimen_positive_SAR_COV2,
		sum(case when covidpos = '0' then 1 else 0 end) as no_of_specimen_negative_SAR_COV2,
		sum(case when flutest = '1' then 1 else 0 end) as no_of_tests,
		sum(case when flua_positive = '1' or flub_positive = '1' then 1 else 0 end) as no_ILI_tests
	from joined_data
	group by 		
		disease_key,
		gender,
		age_group_key,
		county_key,
		epi_week_key,
        facility_key
),
long_format_table as (
	select 
        {{ tsql_utils.surrogate_key( ['disease_key', 'gender', 'age_group_key', 'county_key',
         'epi_week_key', 'facility_key', 'indicator']) }} as fact_key,
		disease_key,
		gender,
		age_group_key,
		county_key,
		epi_week_key,
        facility_key,
        indicator,
		indicator_value
	from influenza_summary
	cross apply
	(
	values
		('no_of_specimen_positive_influenza_a', no_of_specimen_positive_influenza_a),
		('no_of_specimen_positive_influenza_b',no_of_specimen_positive_influenza_b),
		('no_of_specimen_positive_SAR_COV2', no_of_specimen_positive_SAR_COV2),
		('no_of_specimen_negative_SAR_COV2',no_of_specimen_negative_SAR_COV2),
		('no_of_tests', no_of_tests),
		('no_ILI_tests', no_ILI_tests)
	) as mapping (indicator, indicator_value)
)
select 
    disease_key,
    gender,
    age_group_key,
    county_key,
    epi_week_key,
    facility_key,
    indicators.indicator_key,
    indicator_value
from long_format_table
left join {{ref('dim_indicator')}} as indicators on indicators.indicator = long_format_table.indicator