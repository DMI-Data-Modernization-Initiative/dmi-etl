select
    facility.facility_name,
    facility.latitude,
    facility.longitude,
    screen_date.date as screen_date,
    screen_date.year as screen_year,
    screen_date.month as screen_month,
    age_group.age_group_category,
    pat_outcome.patient_outcome,
    epi_week.week_number as screening_epi_week,
    epi_week.year as screening_year,
    dim_case_def.case_definition,
    sari_ili.eligible,
    sari_ili.enrolled,
    sari_ili.not_enrolled,
    sari_ili.tested,
    sari_ili.not_tested,
    sari_ili.tested_for_influenza_sars_cov_2,
    sari_ili.tested_for_influenza_only,
    sari_ili.tested_for_sars_cov_2_only,
    sari_ili.tested_for_influenza,
    sari_ili.tested_for_covid,
    sari_ili.influenza_positive,
    sari_ili.covid_positive,
    sari_ili.influenza_a,
    sari_ili.influenza_b,
    sari_ili.influenza_a_b,
    sari_ili.a_h1n1,
    sari_ili.a_h3n2,
    sari_ili.non_sub_typable,
    sari_ili.not_yet_sub_typed,
    sari_ili.b_victoria,
    sari_ili.b_yamagata,
    sari_ili.not_determined,
    cast(current_date as date) as load_date
from {{ ref("fct_aggregate_sari_ili") }} as sari_ili
left join
    {{ ref("dim_facility") }} as facility
    on facility.facility_key = sari_ili.facility_key
left join
    {{ ref("dim_date") }} as screen_date on screen_date.date_key = sari_ili.date_key
left join
    {{ ref("dim_patient_outcome") }} as pat_outcome
    on pat_outcome.outcome_key = sari_ili.outcome_key
left join
    {{ ref("dim_age_group_sari") }} as age_group
    on age_group.age_group_key = sari_ili.age_group_key
left join
    {{ ref("dim_epi_week") }} as epi_week
    on epi_week.epi_week_key = sari_ili.epi_week_key
left join
    {{ ref("dim_sari_ili_case_definition") }} as dim_case_def
    on dim_case_def.case_definition_key = sari_ili.case_definition_key
