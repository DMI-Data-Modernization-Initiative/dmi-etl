with subset_data as (
	select 
		screeningdate,
		datecollected ,
		mflcode::int,
		eligible::int,
		enrolled::int,
		case sarcov_result
			when 'NEG' then 'Negative'
			when 'POS' then  'Positive'
			else sarcov_result
		end as sarcov_result,
case rsv_result
			when 'NEG' then 'Negative'
			when 'POS' then  'Positive'
			else rsv_result
		end as rsv_result,
 case flu_result
			when 'NEG' then 'Negative'
			when 'POS' then  'Positive'
			else flu_result
		end as flu_result,
		calculated_age::decimal,
		sex::int,
		barcode::int
from {{ ref( 'stg_mortality_surveillance') }}
)
select
 	coalesce(facility.facility_key, 'unset') as facility_key,
 	coalesce(screen_date.date_key, 'unset') as screen_date_key,
 	coalesce(screen_epi_week.epi_week_key, 'unset') as screen_epi_week_key,
 	coalesce(date_collect.date_key, 'unset') as collect_date_key,
 	coalesce(collect_epi_week.epi_week_key, 'unset') as collect_epi_week_key,
 	coalesce(age_group.age_group_key, 'unset') as age_group_key,
 	coalesce(gender.gender_key, 'unset') as gender_key,
	count(*) as screened,
	sum(case when eligible = 1 then 1 else 0 end) as eligible,
	sum(case when enrolled = 1 then 1 else 0 end) as enrolled,
	sum(case when barcode > 1 then 1 else 0 end) as sampled,

	sum(case when sarcov_result in ('Inconclusive', 'Invalid', 'Negative', 'Positive') then 1 else 0 end) as number_sars_cov_2_tested,
	sum(case when sarcov_result = 'Positive' then 1 else 0 end) as number_sars_cov_2_positive,
	sum(case when sarcov_result = 'Negative' then 1 else 0 end) as number_sars_cov_2_negative,
	
	sum(case when rsv_result in ('Inconclusive', 'Invalid', 'Negative', 'Positive') then 1 else 0 end) as number_rsv_tested,
	sum(case when rsv_result = 'Positive' then 1 else 0 end) as number_rsv_positive,
	sum(case when rsv_result = 'Negative' then 1 else 0 end) as number_rsv_negative,

	sum(case when flu_result in ('Inconclusive', 'Invalid', 'Negative', 'Positive') then 1 else 0 end) as number_flu_tested,
	sum(case when flu_result = 'Positive' then 1 else 0 end) as number_flu_positive,
	sum(case when flu_result = 'Negative' then 1 else 0 end) as number_flu_negative,

	cast(current_date as date) as load_date
from subset_data
left join {{ ref( 'dim_facility' ) }} as facility on facility.mfl_code = subset_data.mflcode
left join {{ ref( 'dim_date') }} as screen_date on screen_date.date = subset_data.screeningdate
left join {{ ref( 'dim_epi_week') }} as screen_epi_week on subset_data.screeningdate >= screen_epi_week.start_of_week 
	and subset_data.screeningdate <= screen_epi_week.end_of_week 
left join {{ ref( 'dim_date') }} as date_collect on date_collect.date = subset_data.datecollected
left join {{ ref( 'dim_epi_week') }} as collect_epi_week on subset_data.datecollected >= collect_epi_week.start_of_week 
	and subset_data.datecollected <= collect_epi_week.end_of_week 
left join {{ ref('dim_gender') }} as gender on gender.code = subset_data.sex
left join {{ ref( 'dim_age_group_afi_and_mortality') }} as age_group on subset_data.calculated_age >= age_group.start_age 
	and subset_data.calculated_age < age_group.end_age /* making sure the end_age comparison is only for less than.. */
group by 
 	coalesce(facility.facility_key, 'unset'),
 	coalesce(screen_date.date_key, 'unset'),
 	coalesce(screen_epi_week.epi_week_key, 'unset'),
 	coalesce(date_collect.date_key, 'unset'),
 	coalesce(collect_epi_week.epi_week_key, 'unset'),
 	coalesce(age_group.age_group_key, 'unset'),
 	coalesce(gender.gender_key, 'unset')
