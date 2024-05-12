with data as (
	select 
	 distinct pidnumber,
	 case
	   when p_falciparum = 'POS' and p_vivax <> 'POS' then 'P.falciparum'
	   when p_falciparum <> 'POS' and p_vivax = 'POS' then 'P.vivax'
	   when p_falciparum = 'POS' and p_vivax = 'POS' then 'Mixed infection'
	   when plasmodium = 'POS' and p_falciparum <> 'POS' and p_vivax <> 'POS' then 'Other plasmodium'
	 end as malaria_pos_category,
	 p_falciparum,
	 p_vivax,
	 plasmodium
	from {{ ref('intermediate_tac_results') }}
    where p_falciparum = 'POS' or p_vivax = 'POS' or plasmodium = 'POS' or 
         p_falciparum = 'NEG' or p_vivax = 'NEG' or plasmodium = 'NEG'
)
select 
    coalesce(gender.gender_key, 'unset') as gender_key,
    coalesce(age_group.age_group_key, 'unset') as age_group_key,
    coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
    coalesce(facility.facility_key, 'unset') as facility_key,
    coalesce(date.date_key, 'unset') as date_key,
    coalesce(malaria_pos_category_key, 'unset') as malaria_pos_category_key,
	count(pidnumber) as no_of_cases,
    sum(case when malaria_pos_category_key is not null then 1 else 0 end) as no_of_positive_cases
from data 
left join {{ ref('stg_afi_enroll_and_household_info') }} as enroll on enroll."PID" = data.pidnumber
left join {{ ref('dim_gender') }} as gender on gender.code  = enroll."Gender"
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on enroll."Ageyrs" >= age_group.start_age 
    and  enroll."Ageyrs"  < age_group.end_age 
left join {{ ref('dim_epi_week') }} as epi_week on enroll.interview_date  >= epi_week.start_of_week 
    and enroll.interview_date <= epi_week.end_of_week 
left join {{ ref('dim_facility') }} as facility on facility.code = left(enroll."PID", 3)
left join {{ ref('dim_date') }} as date on date.date = enroll.interview_date 
left join {{ ref('dim_malaria_result') }} as results on results.malaria_pos_category = data.malaria_pos_category
group by
    coalesce(gender.gender_key, 'unset'),
    coalesce(age_group.age_group_key, 'unset'),
    coalesce(epi_week.epi_week_key, 'unset'),
    coalesce(facility.facility_key, 'unset'),
    coalesce(date.date_key, 'unset'),
    coalesce(malaria_pos_category_key, 'unset')
