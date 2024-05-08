with malaria_results_data as (
	select 
		malaria_rdt."PID",
		enroll."Gender",
		enroll."Ageyrs",
		enroll.interview_date,
		left(enroll."PID", 3) as site_short_code,
		"ResultValue",
		malaria_rdt."Barcode"
	from {{ ref('stg_afi_malaria_rdt') }} as malaria_rdt
	left join {{ ref('stg_afi_lab_results') }} as results on results."Barcode"  = malaria_rdt."Barcode"  
	left join {{ ref('stg_afi_enroll_and_household_info') }} as enroll on enroll."PID" = malaria_rdt."PID"
),
leptospirosis_data as (
	select 
		leptospirosis_rdt."PID",
		enroll."Gender",
		enroll."Ageyrs",
		enroll.interview_date,
		left(enroll."PID", 3) as site_short_code,
		"ResultValue",
		leptospirosis_rdt."Barcode" 
	from {{ ref('stg_afi_leptospirosis_rdt') }} as leptospirosis_rdt
	left join {{ ref('stg_afi_lab_results') }} as results on results."Barcode"  = leptospirosis_rdt."Barcode" 
	left join {{ ref('stg_afi_enroll_and_household_info') }} as enroll on enroll."PID" = leptospirosis_rdt."PID"
),
malaria_rdt_results as (
 select
 	coalesce(gender.gender_key, 'unset') as gender_key,
    coalesce(age_group.age_group_key, 'unset') as age_group_key,
    coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
    coalesce(facility.facility_key, 'unset') as facility_key,
    coalesce(date.date_key, 'unset') as date_key,
    coalesce(result.lab_result_key, 'unset') as lab_result_key,
 	count(distinct "PID") as cases,
 	'Malaria' as rdt,
	cast(current_date as date) as load_date
 from malaria_results_data
 left join {{ ref('dim_gender') }} as gender on gender.code  = malaria_results_data."Gender"
 left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on malaria_results_data."Ageyrs" >= age_group.start_age 
    and malaria_results_data."Ageyrs"  < age_group.end_age 
 left join {{ ref('dim_epi_week') }} as epi_week on malaria_results_data.interview_date >= epi_week.start_of_week 
    and malaria_results_data.interview_date <= epi_week.end_of_week 
 left join {{ ref('dim_facility') }} as facility on facility.code = malaria_results_data.site_short_code
 left join {{ ref('dim_date') }} as date on date.date = malaria_results_data.interview_date
 left join {{ ref('dim_lab_result') }} as result on result.code = malaria_results_data."ResultValue"
 group by
        coalesce(gender.gender_key, 'unset'),
        coalesce(age_group.age_group_key, 'unset'),
        coalesce(epi_week.epi_week_key, 'unset'),
        coalesce(facility.facility_key, 'unset'),
        coalesce(date.date_key, 'unset'),
        coalesce(result.lab_result_key, 'unset')
),
leptospirosis_rdt_results as (
 select
 	coalesce(gender.gender_key, 'unset') as gender_key,
    coalesce(age_group.age_group_key, 'unset') as age_group_key,
    coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
    coalesce(facility.facility_key, 'unset') as facility_key,
    coalesce(date.date_key, 'unset') as date_key,
    coalesce(result.lab_result_key, 'unset') as lab_result_key,
 	count(distinct "PID") as cases,
 	'Leptospirosis' as rdt,
	cast(current_date as date) as load_date
 from leptospirosis_data
 left join {{ ref('dim_gender') }} as gender on gender.code  = leptospirosis_data."Gender"
 left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on leptospirosis_data."Ageyrs" >= age_group.start_age 
    and leptospirosis_data."Ageyrs"  < age_group.end_age 
 left join {{ ref('dim_epi_week') }} as epi_week on leptospirosis_data.interview_date >= epi_week.start_of_week 
    and leptospirosis_data.interview_date <= epi_week.end_of_week 
 left join {{ ref('dim_facility') }} as facility on facility.code = leptospirosis_data.site_short_code
 left join{{ ref('dim_date') }} as date on date.date = leptospirosis_data.interview_date
 left join{{ ref('dim_lab_result') }} as result on result.code = leptospirosis_data."ResultValue"
 group by
        coalesce(gender.gender_key, 'unset'),
        coalesce(age_group.age_group_key, 'unset'),
        coalesce(epi_week.epi_week_key, 'unset'),
        coalesce(facility.facility_key, 'unset'),
        coalesce(date.date_key, 'unset'),
        coalesce(result.lab_result_key, 'unset')
)
select 
	*
from malaria_rdt_results

union all

select 
	*
from leptospirosis_rdt_results

