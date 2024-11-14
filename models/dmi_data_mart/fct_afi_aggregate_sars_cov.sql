with npop as (
	select
		swab."PID",
		left(swab."PID", 3) as site_short_code,
		sarscov."Results",
		sarscov."Date Collected"::date as date_collected,
		'npop' as type
 from staging.stg_afi_npop_swab as swab
 left join staging.stg_afi_sarscov_merged_results as sarscov on sarscov."Sample Name" = swab."Barcode"
 where left(swab."PID", 3) <> 'MRT'
),
nasal as (
	select 
		swab."PID",
		left(swab."PID", 3) as site_short_code,
		sarscov."Results",
		sarscov."Date Collected"::date as date_collected,
		'nasal' as type
	from staging.stg_afi_nasal_swab as swab
	left join staging.stg_afi_sarscov_merged_results as sarscov on sarscov."Sample Name" = swab."Barcode" 
),
throat as (
	select 
		swab."PID",
		left(swab."PID", 3) as site_short_code,
		sarscov."Results",
		sarscov."Date Collected"::date as date_collected,
		'throat' as type
	from staging.stg_afi_throat_swab as swab
	left join staging.stg_afi_sarscov_merged_results as sarscov on sarscov."Sample Name" = swab."Barcode" 
),
unioned_data as (
	 select  * from  npop 
	  union all
	 select * from nasal 
	  union all 
	 select * from throat
)                
 select
 	coalesce(gender.gender_key, 'unset') as gender_key,
    coalesce(age_group.age_group_key, 'unset') as age_group_key,
    coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
    coalesce(facility.facility_key, 'unset') as facility_key,
    coalesce(date.date_key, 'unset') as date_key,
    coalesce(result.lab_result_key, 'unset') as lab_result_key,
	count(*) as no_of_cases,
	unioned_data.type,
	cast(current_date as date) as load_date
 from unioned_data as unioned_data
 left join staging.stg_afi_enroll_and_household_info as enroll on enroll."PID" = unioned_data."PID" 
 left join {{ ref('dim_gender') }} as gender on gender.code  = enroll."Gender"
 left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on enroll."Ageyrs" >= age_group.start_age 
     and  enroll."Ageyrs"  < age_group.end_age 
 left join {{ ref('dim_epi_week') }} as epi_week on unioned_data.date_collected  >= epi_week.start_of_week 
     and unioned_data.date_collected <= epi_week.end_of_week 
 left join {{ ref('dim_facility') }} as facility on facility.code = unioned_data.site_short_code
 left join staging.dim_date as date on date.date = unioned_data.date_collected
 left join {{ ref('dim_lab_result') }} as result on result.lab_result_2 = unioned_data."Results" 
 group by 
  	coalesce(gender.gender_key, 'unset'),
    coalesce(age_group.age_group_key, 'unset'),
    coalesce(epi_week.epi_week_key, 'unset'),
    coalesce(facility.facility_key, 'unset'),
    coalesce(date.date_key, 'unset'),
    coalesce(result.lab_result_key, 'unset'),
    unioned_data.type
