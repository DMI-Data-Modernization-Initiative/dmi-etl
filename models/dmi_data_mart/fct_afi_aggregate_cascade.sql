with screened_and_eligible_cte as (
  select
  	coalesce(gender.gender_key, 'unset') as gender_key,
  	coalesce(age_group.age_group_key, 'unset') as age_group_key,
  	coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
  	coalesce(facility.facility_key, 'unset') as facility_key,
  	coalesce(date.date_key, 'unset') as date_key,
	count(distinct "Unique_ID") as screened,
	sum("Eligible") as eligible
  from {{ ref('stg_afi_screening') }} as screening
  left join {{ ref('dim_gender') }} as gender on gender.code  = screening."PatientGender"
  left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on screening."Ageyears" >= age_group.start_age 
  	and  screening."Ageyears"  < age_group.end_age 
  left join {{ ref('dim_epi_week') }} as epi_week on screening.interview_date  >= epi_week.start_of_week 
	and screening.interview_date <= epi_week.end_of_week 
  left join {{ ref('dim_facility') }} as facility on facility.afi_study_site_id = screening."StudySite" 
  left join {{ ref('dim_date') }} as date on date.date = screening.interview_date
  group by 
	coalesce(gender.gender_key, 'unset'),
	coalesce(age_group.age_group_key, 'unset'),
	coalesce(epi_week.epi_week_key, 'unset'),
	coalesce(facility.facility_key, 'unset'),
	coalesce(date.date_key, 'unset')
),
enrolled_cte as (
  select
  	coalesce(gender.gender_key, 'unset') as gender_key,
  	coalesce(age_group.age_group_key, 'unset') as age_group_key,
  	coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
  	coalesce(facility.facility_key, 'unset') as facility_key,
  	coalesce(date.date_key, 'unset') as date_key,
  	count(distinct enroll."PID") as enrolled
  from {{ ref('stg_afi_enroll_and_household_info') }} as enroll
  left join {{ ref('dim_gender') }} as gender on gender.code  = enroll."Gender"
  left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on enroll."Ageyrs" >= age_group.start_age 
  	and  enroll."Ageyrs"  < age_group.end_age 
  left join {{ ref('dim_epi_week') }} as epi_week on enroll.interview_date  >= epi_week.start_of_week 
	and enroll.interview_date <= epi_week.end_of_week 
  left join {{ ref('dim_facility') }} as facility on facility.code = left(enroll."PID", 3)
  left join {{ ref('dim_date') }} as date on date.date = enroll.interview_date 
  group by 
  	coalesce(gender.gender_key, 'unset'),
  	coalesce(age_group.age_group_key, 'unset'),
  	coalesce(epi_week.epi_week_key, 'unset'),
  	coalesce(facility.facility_key, 'unset'),
  	coalesce(date.date_key, 'unset')
),
eligibility_sampling_cte as (
    select
        coalesce(gender.gender_key, 'unset') as gender_key,
        coalesce(age_group.age_group_key, 'unset') as age_group_key,
        coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
        coalesce(facility.facility_key, 'unset') as facility_key,
        coalesce(date.date_key, 'unset') as date_key,
        count(distinct case_def."PID") as eligible_for_sampling
    from {{ ref('intermediate_afi_case_classification') }} as case_def
    left join {{ ref('stg_afi_enroll_and_household_info') }} as enroll on enroll."PID" = case_def."PID" 
    left join {{ ref('dim_gender') }} as gender on gender.code  = enroll."Gender"
    left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on enroll."Ageyrs" >= age_group.start_age 
        and  enroll."Ageyrs"  < age_group.end_age 
    left join {{ ref('dim_epi_week') }} as epi_week on enroll.interview_date  >= epi_week.start_of_week 
        and enroll.interview_date <= epi_week.end_of_week 
    left join {{ ref('dim_facility') }} as facility on facility.code = left(enroll."PID", 3)
    left join {{ ref('dim_date') }} as date on date.date = enroll.interview_date 
    where CaseClassification in ('SARI', 'UF', 'MERS-CoV')
    group by
        coalesce(gender.gender_key, 'unset'),
        coalesce(age_group.age_group_key, 'unset'),
        coalesce(epi_week.epi_week_key, 'unset'),
        coalesce(facility.facility_key, 'unset'),
        coalesce(date.date_key, 'unset')
),
sampled_cte as (
    select
        coalesce(gender.gender_key, 'unset') as gender_key,
        coalesce(age_group.age_group_key, 'unset') as age_group_key,
        coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
        coalesce(facility.facility_key, 'unset') as facility_key,
        coalesce(date.date_key, 'unset') as date_key,
        count(distinct collection."PID") as sampled
    from {{ ref('stg_afi_sample_collection') }} as collection
    left join {{ ref('stg_afi_enroll_and_household_info') }} as enroll on enroll."PID" = collection."PID" 
    left join {{ ref('dim_gender') }} as gender on gender.code  = enroll."Gender"
    left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on enroll."Ageyrs" >= age_group.start_age 
        and  enroll."Ageyrs"  < age_group.end_age 
    left join {{ ref('dim_epi_week') }} as epi_week on enroll.interview_date  >= epi_week.start_of_week 
        and enroll.interview_date <= epi_week.end_of_week 
    left join {{ ref('dim_facility') }} as facility on facility.code = left(enroll."PID", 3)
    left join {{ ref('dim_date') }} as date on date.date = enroll.interview_date 
    where "Barcode" not in (0, 22, 88888, 222222, 1111111, 11110000, 22220000, 22222222, 88888888) and 
    collection."PID" is not null
    group by
        coalesce(gender.gender_key, 'unset'),
        coalesce(age_group.age_group_key, 'unset'),
        coalesce(epi_week.epi_week_key, 'unset'),
        coalesce(facility.facility_key, 'unset'),
        coalesce(date.date_key, 'unset')
),
screened_eligible_enrolled as (
	select 
		coalesce(screened_and_eligible_cte.gender_key, enrolled_cte.gender_key) as gender_key,
		coalesce(screened_and_eligible_cte.age_group_key, enrolled_cte.age_group_key) as age_group_key,
		coalesce(screened_and_eligible_cte.epi_week_key, enrolled_cte.epi_week_key) as epi_week_key,
		coalesce(screened_and_eligible_cte.facility_key, enrolled_cte.facility_key) as facility_key,
		coalesce(screened_and_eligible_cte.date_key, enrolled_cte.date_key) as date_key,
		coalesce(screened_and_eligible_cte.screened, 0) as screened,
		coalesce(screened_and_eligible_cte.eligible, 0) as eligible,
		coalesce(enrolled_cte.enrolled, 0) as enrolled
	from screened_and_eligible_cte
	full join enrolled_cte on screened_and_eligible_cte.gender_key = enrolled_cte.gender_key
		and screened_and_eligible_cte.age_group_key = enrolled_cte.age_group_key
		and screened_and_eligible_cte.epi_week_key = enrolled_cte.epi_week_key
		and screened_and_eligible_cte.facility_key = enrolled_cte.facility_key
		and screened_and_eligible_cte.date_key = enrolled_cte.date_key
),
screened_eligible_enrolled_eligibility_sampling as (
	select 
		coalesce(screened_eligible_enrolled.gender_key, eligibility_sampling_cte.gender_key) as gender_key,
		coalesce(screened_eligible_enrolled.age_group_key, eligibility_sampling_cte.age_group_key) as age_group_key,
		coalesce(screened_eligible_enrolled.epi_week_key, eligibility_sampling_cte.epi_week_key) as epi_week_key,
		coalesce(screened_eligible_enrolled.facility_key, eligibility_sampling_cte.facility_key) as facility_key,
		coalesce(screened_eligible_enrolled.date_key, eligibility_sampling_cte.date_key) as date_key,
		coalesce(eligibility_sampling_cte.eligible_for_sampling, 0) as eligible_for_sampling,
		screened_eligible_enrolled.screened,
		screened_eligible_enrolled.eligible,
		screened_eligible_enrolled.enrolled
	from screened_eligible_enrolled
	full join eligibility_sampling_cte on screened_eligible_enrolled.gender_key = eligibility_sampling_cte.gender_key
		and eligibility_sampling_cte.age_group_key = screened_eligible_enrolled.age_group_key
		and eligibility_sampling_cte.epi_week_key = screened_eligible_enrolled.epi_week_key
		and eligibility_sampling_cte.facility_key = screened_eligible_enrolled.facility_key
		and eligibility_sampling_cte.date_key = screened_eligible_enrolled.date_key
),
screened_eligible_enrolled_eligibility_sampling_and_sampled as (
	select 
		coalesce(screened_eligible_enrolled_eligibility_sampling.gender_key, sampled_cte.gender_key) as gender_key,
		coalesce(screened_eligible_enrolled_eligibility_sampling.age_group_key, sampled_cte.age_group_key) as age_group_key,
		coalesce(screened_eligible_enrolled_eligibility_sampling.epi_week_key, sampled_cte.epi_week_key) as epi_week_key,
		coalesce(screened_eligible_enrolled_eligibility_sampling.facility_key, sampled_cte.facility_key) as facility_key,
		coalesce(screened_eligible_enrolled_eligibility_sampling.date_key, sampled_cte.date_key) as date_key,
		coalesce(sampled_cte.sampled, 0) as sampled,
		screened_eligible_enrolled_eligibility_sampling.eligible_for_sampling,
		screened_eligible_enrolled_eligibility_sampling.screened,
		screened_eligible_enrolled_eligibility_sampling.eligible,
		screened_eligible_enrolled_eligibility_sampling.enrolled
	from screened_eligible_enrolled_eligibility_sampling
	full join sampled_cte on screened_eligible_enrolled_eligibility_sampling.gender_key = sampled_cte.gender_key
		and sampled_cte.age_group_key = screened_eligible_enrolled_eligibility_sampling.age_group_key
		and sampled_cte.epi_week_key = screened_eligible_enrolled_eligibility_sampling.epi_week_key
		and sampled_cte.facility_key = screened_eligible_enrolled_eligibility_sampling.facility_key
		and sampled_cte.date_key = screened_eligible_enrolled_eligibility_sampling.date_key
)
select 
	gender_key,
	age_group_key,
	epi_week_key,
	facility_key,
	date_key,
	screened,
	eligible,
	enrolled,
	eligible_for_sampling,
	sampled,
	cast(current_date as date) as load_date
from screened_eligible_enrolled_eligibility_sampling_and_sampled