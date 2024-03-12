with enriched_data as (
	select 
		"UNIT_CREATEDAT":: date as unit_created_at,
		"UNIT_PARENT_NAME",
		case substring("UNIT_PARENT_NAME" from '^(.*?) Sub County') 
			when 'Nakuru East' then 'Nakuru Town East'
			when 'Nakuru West' then 'Nakuru Town West'
			when 'Buuri  East' then 'Buuri East'
			when 'Imenti South'then 'South Imenti'
			when 'Imenti Central' then 'Central Imenti'
			when 'Imenti North' then 'North Imenti'
			else substring("UNIT_PARENT_NAME" from '^(.*?) Sub County')
		end as subcounty,
		substring("UNIT_PARENT_PARENT_NAME" from '^(.*?) County') as county,
		"SIGNAL"
	from {{ ref('stg_mdharura_ebs_linelist') }}	
)
select
	epi_week.epi_week_key,
	county.county_key,
	sub_county.sub_county_key,
	sum(case when "SIGNAL" = '1' then 1 else 0 end) as count_c1,
	sum(case when "SIGNAL" = '2' then 1 else 0 end) as count_c2,
	sum(case when "SIGNAL" = '3' then 1 else 0 end) as count_c3,
	sum(case when "SIGNAL" = '4' then 1 else 0 end) as count_c4,
	sum(case when "SIGNAL" = '5' then 1 else 0 end) as count_c5,
	sum(case when "SIGNAL" = '6' then 1 else 0 end) as count_c6,
	sum(case when "SIGNAL" = '7' then 1 else 0 end) as count_c7,
	sum(case when "SIGNAL" = 'h1' then 1 else 0 end) as count_h1,
	sum(case when "SIGNAL" = 'h2' then 1 else 0 end) as count_h2,
	sum(case when "SIGNAL" = 'h3' then 1 else 0 end) as count_h3
from enriched_data 
left join {{ ref('dim_epi_week') }} as epi_week on enriched_data.unit_created_at between epi_week.start_of_week and epi_week.end_of_week 
left join {{ ref('dim_county') }} as county on county.county = enriched_data.county
left join {{ ref('dim_sub_county') }} as sub_county on sub_county.sub_county = enriched_data.subcounty
group by 
	epi_week.epi_week_key,
	county.county_key,
	sub_county_key