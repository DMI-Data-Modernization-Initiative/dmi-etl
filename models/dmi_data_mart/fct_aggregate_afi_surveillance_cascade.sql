
with subset_data as (
select 
         screening_date,
		 gender as sex,
        CASE When  "Unique_ID"  IS NOT NULL then 1 else 0 end as screened,
		--eligible is when the person is screened and consent not equal to capped (6)
	    CASE when eligible =1 and consent != 6 THEN 1 else 0 end as eligible,
		CASE when  consent=1 then 1 else 0 end as enrolled,
		 --eligble sampling remove the DNS
		CASE WHEN consent=1 and proposed_combined_case != 'DNS' then 1 else 0 end as eligible_sampling, 
	    CASE when consent = 1 and sampled = 1 then 1 else 0 end as sampled,
		CASE WHEN eligible = 1 and consent = 2 then 1 else 0 end as declined_enrollment,
        site::int as mflcode,
		screeningpoint,
		high_temp_recorded,
		calculated_age_days 
	
from {{ ref ('stg_afi_surveillance') }}
)

SELECT 
    coalesce(facility.facility_key, 'unset') as facility_key,
	coalesce(screen_date.date_key, 'unset') as date_key,
	coalesce(gender.gender_key, 'unset') as gender_key,
	coalesce(age_group.age_group_key, 'unset') as age_group_key,
	coalesce(screeningpoint.screeningpoint_key, 'unset') as screeningpoint_key,
	coalesce(screen_epi_week.epi_week_key, 'unset') as epi_week_key,
	sum(case when screened = 1 then 1 else 0 end) as screened,
	sum(case when eligible = 1 then 1 else 0 end) as eligible,
	sum(case when enrolled = 1 then 1 else 0 end) as enrolled,
    sum(case when eligible_sampling = 1 then 1 else 0 end) as eligible_sampling,
    sum(case when sampled = 1 then 1 else 0 end) as sampled,
    sum(case when declined_enrollment = 1 then 1 else 0 end) as declined_enrollment,

	cast(current_date as date) as load_date
FROM subset_data
left join {{ ref( 'dim_facility' ) }} as facility on facility.mfl_code = subset_data.mflcode
left join {{ ref( 'dim_date') }} as screen_date on screen_date.date = subset_data.screening_date

left join {{ ref( 'dim_epi_week') }} as screen_epi_week on subset_data.screening_date >= screen_epi_week.start_of_week 
	and subset_data.screening_date <= screen_epi_week.end_of_week 
	
	left join {{ ref('dim_gender') }} as gender on gender.code = subset_data.sex
left join {{ ref( 'dim_age_group_afi') }} as age_group on subset_data.calculated_age_days >= age_group.start_age_days 
	and subset_data.calculated_age_days < age_group.end_age_days
left join {{ ref('dim_afi_screening_point') }} as screeningpoint on screeningpoint.screeningpoint = subset_data.screeningpoint

group by 
 	coalesce(facility.facility_key, 'unset'),
 	coalesce(screen_date.date_key, 'unset'),
 	coalesce(age_group.age_group_key, 'unset'),
 	coalesce(gender.gender_key, 'unset'),
	coalesce(screeningpoint.screeningpoint_key, 'unset'),
	coalesce(screen_epi_week.epi_week_key, 'unset')