with influenza_data as (
	select  
		'Influenza' as disease, 
		sari_data.*
	from {{ ref('stg_sari_ili') }} as sari_data
),
joined_data as (
	select
		disease.disease_key,
		coalesce(age_group.age_group_key, 'unset') as age_group_key,
		epi_wk.epi_week_key,
		county.county_key,
        coalesce(facility.facility_key, 'unset') as facility_key,
        coalesce(sari.gender, 'unset') as gender,
		sari.h3n2,
		sari.ph1n1,
		victoria,
		covidpos,
		flutest,
		flua_positive,
		flub_positive
	from influenza_data as sari
	left join {{ ref('dim_age_group') }} as age_group on sari.age_in_years between age_group.start_age and age_group.end_age 
	left join {{ ref('dim_epi_week') }} as epi_wk on sari.date_screened between epi_wk.start_of_week::date and epi_wk.end_of_week::date
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
		sum(case when flutest = '1' then 1 else 0 end) as no_of_tests
		---sum(case when flua_positive = '1' or flub_positive = '1' then 1 else 0 end) as no_ILI_tests
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
        {{ dbt_utils.surrogate_key( ['disease_key', 'gender', 'age_group_key', 'county_key',
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
	cross join lateral
	(
	values
		('no_of_specimen_positive_influenza_a', no_of_specimen_positive_influenza_a),
		('no_of_specimen_positive_influenza_b',no_of_specimen_positive_influenza_b),
		('no_of_specimen_positive_SAR_COV2', no_of_specimen_positive_SAR_COV2),
		('no_of_specimen_negative_SAR_COV2',no_of_specimen_negative_SAR_COV2),
		('no_of_tests', no_of_tests)
		--('no_ILI_tests', no_ILI_tests)
	) as mapping (indicator, indicator_value)
)
select 
    disease_key,
    gender,
    age_group_key,
    county_key,
	null as sub_county_key,
    epi_week_key,
    facility_key,
    indicators.indicator_key,
    indicator_value,
	'icap' as data_source,
	cast(current_date as date) as load_date
from long_format_table
left join {{ref('dim_indicator')}} as indicators on indicators.indicator = long_format_table.indicator

union 

select 
	disease_key,
    'unset' as gender,
    coalesce(age_group.age_group_key, 'unset') as age_group_key,
    coalesce(county.county_key, 'unset') as county_key,
	sub_county_key,
    epi_week_key,
    'unset' as facility_key,
    indicators.indicator_key,
    indicator_value,
	'dhis' as data_source,
	cast(current_date as date) as load_date
from {{ref('intermediate_dhis_moh_505_mappings')}} as dhis_moh_505_mappings
left join {{ ref('dim_age_group') }} as age_group on dhis_moh_505_mappings.age_group = age_group.age_group_category
left join {{ ref('dim_epi_week') }} as epi_wk on dhis_moh_505_mappings.epi_week::int = epi_wk.week_number::int
	and dhis_moh_505_mappings.year::int = epi_wk.year::int
left join {{ ref('dim_county') }} as county on county.county =  dhis_moh_505_mappings.county
left join {{ ref('dim_disease') }} as disease on disease.disease = dhis_moh_505_mappings.disease
left join {{ref('dim_sub_county')}} as sub_county on sub_county.sub_county = dhis_moh_505_mappings.sub_county
left join {{ref('dim_indicator')}} as indicators on indicators.indicator = dhis_moh_505_mappings.indicator



