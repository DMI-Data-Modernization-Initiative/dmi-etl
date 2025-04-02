with sub_set_data as (
	select 
    distinct
		"Unique_ID",
		pid,
		target,
		result
	from  {{ ref('intermediate_afi_surveillance_tac_results') }}
),
positive_results as (
	select
		coalesce(gender.gender_key, 'unset') as gender_key,
		coalesce(age_group.age_group_key, 'unset') as age_group_key,
		coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
		coalesce(facility.facility_key, 'unset') as facility_key,
		coalesce(date.date_key, 'unset') as date_key,
		coalesce(disease_key, 'unset') as disease_key,
		count(*) as no_of_cases,
		enroll.pid
	from sub_set_data as pcr_results
	left join {{ ref('stg_afi_surveillance') }} as enroll on enroll.pid = pcr_results.pid 
	LEFT JOIN {{ ref('dim_gender') }} AS gender 
    ON gender.code = enroll.gender

LEFT JOIN {{ ref('dim_age_group_afi') }} AS age_group 
    ON enroll.calculated_age_days >= age_group.start_age_days 
    AND enroll.calculated_age_days < age_group.end_age_days
	left join {{ ref('dim_epi_week') }} as epi_week on enroll.screening_date  >= epi_week.start_of_week 
		and enroll.screening_date <= epi_week.end_of_week 
	left join {{ ref('dim_facility') }} as facility on facility.mfl_code = enroll.site 
	left join {{ ref('dim_date') }} as date on date.date = enroll.screening_date
	left join {{ ref('dim_disease') }} as disease on disease.disease = pcr_results.target
    where result = 'Positive'
	group by 
		coalesce(gender.gender_key, 'unset'),
		coalesce(age_group.age_group_key, 'unset'),
		coalesce(epi_week.epi_week_key, 'unset'),
		coalesce(facility.facility_key, 'unset'),
		coalesce(date.date_key, 'unset'),
		coalesce(disease_key, 'unset'),
		enroll.pid
)
select 
	positive_results.*,
	cast(current_date as date) as load_date
from positive_results