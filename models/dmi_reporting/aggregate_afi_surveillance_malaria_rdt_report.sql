SELECT 
PID,
gender.gender_2  as gender,
facility.facility_name,
age_group.age_group_category,
epi_week.week_number,
date.year,
date.month,
result.lab_result as result,
malaria_result.malaria_pos_category,
malaria_rdt_results.cases_tested,
cast(current_date as date) as load_date
FROM 
{{ ref('fct_aggregate_afi_surveillance_malaria_rdt_results') }} as malaria_rdt_results


left join {{ ref('dim_gender') }} as gender on gender.gender_key = malaria_rdt_results.gender_key 
left join {{ ref('dim_age_group_afi') }} as age_group on age_group.age_group_key = malaria_rdt_results.age_group_key
left join {{ ref('dim_facility') }} as facility on facility.facility_key = malaria_rdt_results.facility_key
left join {{ ref('dim_epi_week')}} as epi_week on epi_week.epi_week_key = malaria_rdt_results.epi_week_key
left join {{ ref('dim_date') }} as date on date.date_key = malaria_rdt_results.date_key

  left join {{ ref('dim_lab_result') }} as result on result.lab_result_key = malaria_rdt_results.lab_result_key
 left join {{ ref('dim_malaria_results') }} as malaria_result on malaria_result.malaria_pos_category_key = malaria_rdt_results.malaria_pos_category_key
 