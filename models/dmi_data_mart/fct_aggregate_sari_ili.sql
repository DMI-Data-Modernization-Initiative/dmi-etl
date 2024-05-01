with subset_data as (
	select 
		eligible,
		enrolled,
		flutest,
		covidtest::int,
		flub_positive::int,
		flua_positive::int,
		covidpos::int,
		flu_positive::int,
		ph1n1::int,
		h3n2::int,
		unsub_non::int,
		notsubtyp::int,
		victoria::int,
		yamagata::int,
		flub_undetermined::int,
		age_in_years,
		facility_name,
		date_screened,
		outcome,
		case_def
	from {{ ref ('stg_sari_ili') }}
)
select
	coalesce(facility.facility_key, 'unset') as facility_key,
	coalesce(screen_date.date_key, 'unset') as date_key,
	coalesce(age_group.age_group_key, 'unset') as age_group_key,
	coalesce(pat_outcome.outcome_key, 'unset') as outcome_key,
	coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
	coalesce(dim_case_def.case_definition_key, 'unset') as case_definition_key,
    sum(eligible) as eligibe,
	sum(enrolled) as enrolled,
	sum(eligible) - sum(enrolled) as not_enrolled,
	sum(case when flutest = 1 or covidtest = 1 then 1 else 0 end) as tested,
	sum(enrolled) - sum(case when flutest = 1 or covidtest::int = 1 then 1 else 0 end) as not_tested,
	sum(case when flutest = 1 and covidtest = 1 then 1 else 0 end) as tested_for_influenza_sars_cov_2,
	sum(case when flutest = 1 and covidtest = 0 then 1 else 0 end) as tested_for_influenza_only,
	sum(case when flutest = 0 and covidtest = 1 then 1 else 0 end) as tested_for_sars_cov_2_only,
	sum(case when flutest = 1 then 1 else 0 end) as tested_for_influenza,
	sum(case when covidtest = 1 then 1 else 0 end) as tested_for_covid,
    sum(case when flu_positive = 1 then 1 else 0 end) as influenza_positive,
    sum(case when covidpos = 1 then 1 else 0 end) as covid_positive,
    sum(case when flub_positive = 0 and flua_positive = 1 then 1 else 0 end) as influenza_a,
    sum(case when flub_positive = 1 and flua_positive = 0 then 1 else 0 end) as influenza_b,
    sum(case when flub_positive = 1 and flua_positive = 1 then 1 else 0 end) as influenza_a_b,
    sum(case when flua_positive = 1 and flub_positive = 0 and ph1n1 = 1 then 1 else 0 end) as a_h1n1,
    sum(case when flua_positive = 1 and flub_positive = 0 and h3n2 = 1 then 1 else 0 end) as a_h3n2,
    sum(case when flua_positive = 1 and flub_positive = 0 and unsub_non= 1 then 1 else 0 end) as non_sub_typable,
    sum(case when flua_positive = 1 and flub_positive = 0 and notsubtyp = 1 then 1 else 0 end) as not_yet_sub_typed,
    sum(case when flua_positive = 0 and flub_positive = 1 and victoria = 1 then 1 else 0 end) as b_victoria,
    sum(case when flua_positive = 0 and flub_positive = 1 and yamagata = 1 then 1 else 0 end) as b_yamagata,
    sum(case when flua_positive = 0 and flub_positive = 1 and flub_undetermined = 1 then 1 else 0 end) as not_determined,
	cast(current_date as date) as load_date
from subset_data as sari_ili
left join {{ ref('dim_facility') }} as facility on facility.facility_name_short = sari_ili.facility_name
left join {{ ref('dim_date') }} as screen_date on screen_date."date"  = sari_ili.date_screened
left join {{ ref('dim_patient_outcome') }} as pat_outcome on pat_outcome.outcome = sari_ili.outcome
left join {{ ref ('dim_age_group_sari')}} as age_group on sari_ili.age_in_years >= age_group.start_age 
	and sari_ili.age_in_years < age_group.end_age /* making sure the end_age comparison is only for less than.. */
left join {{ ref( 'dim_epi_week') }} as epi_week on sari_ili.date_screened >= epi_week.start_of_week 
	and sari_ili.date_screened <= epi_week.end_of_week 
left join {{ ref('dim_sari_ili_case_definition')}} as dim_case_def on dim_case_def.code = sari_ili.case_def
group by 
	coalesce(facility.facility_key, 'unset'),
	coalesce(screen_date.date_key, 'unset'),
	coalesce(age_group.age_group_key, 'unset'),
	coalesce(pat_outcome.outcome_key, 'unset'),
	coalesce(epi_week.epi_week_key, 'unset'),
	coalesce(dim_case_def.case_definition_key, 'unset')