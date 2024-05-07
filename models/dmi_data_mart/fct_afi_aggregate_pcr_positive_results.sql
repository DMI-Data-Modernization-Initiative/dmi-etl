with sub_set_data as (
	select 
		pidnumber,
		barcodeno,
		result,
		disease,
		disease_result 
	from  {{ ref('intermediate_unpivoted_tac_results') }}
),
positive_results as (
	select
		coalesce(gender.gender_key, 'unset') as gender_key,
		coalesce(age_group.age_group_key, 'unset') as age_group_key,
		coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
		coalesce(facility.facility_key, 'unset') as facility_key,
		coalesce(date.date_key, 'unset') as date_key,
		coalesce(disease_key, 'unset') as disease_key,
		count(*) as no_of_cases
	from sub_set_data as pcr_results
	left join {{ ref('stg_afi_enroll_and_household_info') }} as enroll on enroll."PID" = pcr_results.pidnumber 
	left join {{ ref('dim_gender') }} as gender on gender.code = enroll."Gender"
	left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on enroll."Ageyrs" >= age_group.start_age 
		and  enroll."Ageyrs"  < age_group.end_age 
	left join {{ ref('dim_epi_week') }} as epi_week on enroll.interview_date  >= epi_week.start_of_week 
		and enroll.interview_date <= epi_week.end_of_week 
	left join {{ ref('dim_facility') }} as facility on facility.code = left(pcr_results.pidnumber, 3) 
	left join {{ ref('dim_date') }} as date on date.date = enroll.interview_date
	left join {{ ref('dim_disease') }} as disease on disease.disease = pcr_results.disease
	where disease_result = 'POS'
	group by 
		coalesce(gender.gender_key, 'unset'),
		coalesce(age_group.age_group_key, 'unset'),
		coalesce(epi_week.epi_week_key, 'unset'),
		coalesce(facility.facility_key, 'unset'),
		coalesce(date.date_key, 'unset'),
		coalesce(disease_key, 'unset')
)
select 
	positive_results.*,
	cast(current_date as date) as load_date
from positive_results