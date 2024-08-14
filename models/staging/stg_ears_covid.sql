with ears_covid as (
    select
        id::int as id,
        trim(case_id)::text as case_id,
        upper(trim(name))::text as name,
        age::int as age,
        trim(sex)::text as sex,
        trim(phone_number)::text as phone_number,
        trim(occupation)::text as occupation,
        trim(county)::text as county,
        trim(country_of_diagnosis)::text as country_of_diagnosis,
        trim(nationality)::text as nationality,
        trim(subcounty)::text as subcounty,
        trim(ward)::text as ward,
        trim(village)::text as village,
        trim(travel_history)::text as travel_history,
        trim(where_from)::text as where_from,
        trim(imported)::text as imported,
        trim(flight_no)::text as flight_no,
        trim(history_of_contact)::text as history_of_contact,
        trim(date_of_arrival)::text as date_of_arrival,
        symptoms::boolean as symptoms,
        classification::boolean as classification,
        trim(onset_date)::text as onset_date,
        cough::boolean as cough,
        fever::boolean as fever,
        difficulty_breathing::boolean as difficulty_breathing,
        trim(visit_health_facility)::text as visit_health_facility,
        trim(date_hospital_visit)::text as date_hospital_visit,
        trim(health_facility_name)::text as health_facility_name,
        trim(hospitalization)::text as hospitalization,
        trim(admission_date)::text as admission_date,
        sample_collected::boolean as sample_collected,
        trim(date_sample_collected)::text as date_sample_collected,
        trim(lab_result)::text as lab_result,
        trim(lab_site)::text as lab_site,
        trim(date_lab_confirmation)::text as date_lab_confirmation,
        trim(date_first_follow_up)::text as date_first_follow_up,
        trim(result_first_follow_up)::text as result_first_follow_up,
        date_second_follow_up::timestamp as date_second_follow_up,
        trim(result_second_follow_up)::text as result_second_follow_up,
        date_third_follow_up::timestamp as date_third_follow_up,
        trim(result_third_follow_up)::text as result_third_follow_up,
        trim(outcome)::text as outcome,
        date_of_outcome::timestamp as date_of_outcome,
        trim(quarantine_facility)::text as quarantine_facility,
        trim(comobidity)::text as comobidity,
        trim(management)::text as management,
        trim(status_need)::text as status_need,
        date_announced_by_cs::timestamp as date_announced_by_cs,
        trim(type_of_testing)::text as type_of_testing,
        graph_date::timestamp as graph_date,
        trim(status)::text as status,
        edited_by_id::int as edited_by_id,
        trim(others)::text as others,
        trim(vaccinated)::text as vaccinated,
        trim(dosage)::text as dosage
    from {{ source('central_raw_ears', 'veoc_covid_linelist_results') }}
)
select
    id,
    case_id,
    name,
    age,
    case
        when age < 2 then '0-2 yrs'
        when age between 2 and 5 then '2-5 yrs'
        when age between 5 and 16 then '5-16 yrs'
        when age > 16 then '16+ yrs'
        else 'Unknown'
    end as age_group,
    case
        when sex IN ('F', 'f', 'female') then 'Female'
        when sex IN ('M', 'm', 'male') then 'Male'
        else 'Unknown'
    end as sex,
    phone_number,
    case
        when occupation IN ('/Unknown') then 'Unknown'
        else occupation
    end as occupation,
    nationality,
    country_of_diagnosis,
    county,
    subcounty,
    ward,
    village,
    travel_history,
    where_from,
    imported,
    flight_no,
    history_of_contact,
    date_of_arrival,
    symptoms,
    classification,
    onset_date,
    cough,
    fever,
    difficulty_breathing,
    visit_health_facility,
    date_hospital_visit,
    health_facility_name,
    hospitalization,
    admission_date,
    sample_collected,
    date_sample_collected,
    case
        when lab_result in ('POSITIIVE', 'Positive', 'positive', 'POSITIVE', 'POSTIVE', 'POSITVE') then 'Positive'
        when lab_result in ('Negative') then 'Negative'
        when lab_result in ('Inconclusive') then 'Inconclusive'
        else 'Unknown'
    end as lab_result,
    lab_site,
    date_lab_confirmation,
    date_first_follow_up,
    result_first_follow_up,
    date_second_follow_up,
    result_second_follow_up,
    date_third_follow_up,
    result_third_follow_up,
    outcome,
    date_of_outcome,
    quarantine_facility,
    comobidity,
    management,
    status_need,
    date_announced_by_cs,
    case
        when type_of_testing in ('Antgen', 'antigen', 'Antigen', 'Antigens', 'ANTIGEN', 'Antigen/PCR', 'Antigen Rapid test') then 'Antigen'
        when type_of_testing in ('NP SWAB', 'NP/OP', 'NP/OP SWAB', 'OP/NP SWAB') then 'NP/OP'
        when type_of_testing in ('OTHER') then 'Other'
        when type_of_testing in ('Pcr', 'PCR') then 'PCR'
        when type_of_testing in ('Rapid test', 'Rapid Test', 'RDT', 'ROTAGENE', 'RT-PCR') then 'RDT'
        else 'Unknown'
    end as type_of_testing,
    graph_date,
    status,
    edited_by_id,
    others,
    vaccinated,
    dosage
from ears_covid