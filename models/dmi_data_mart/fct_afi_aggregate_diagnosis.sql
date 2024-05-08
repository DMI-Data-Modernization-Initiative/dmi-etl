with cleaned_data as (
	select
		distinct "PID",	
		case "Admissiondiagnosis"
			when 'STRO ENTERETIS' then 'Gastro enteritis'
			when 'NEONATAL SEPSIS' then 'Neonatal sepsis'
			when 'MALARIA' then 'Malaria'
			when 'PNEUMONIA' then 'Pneumonia'
			when 'MENINGITIS' then 'Meningitis'
			when 'OTHER' then 'Other'
			when 'ACUTE RESPIRATORY ILLNESS' then 'Acute respiratory illness'
			when 'RESPIRATORY OR CARDIAC CONDITIONS' then 'Respiratory or cardiac conditions'
			else "Admissiondiagnosis"	
		end as "Admissiondiagnosis",
		case "Otheradmissiondiagnosis"
			when 'ACUTE GASTRITIS' then 'Acute gastritis'
			when 'ACUTE GE IN SHOCK' then 'Acute gastro enteritis in shock'
			when 'SEPTICAEMIA' then 'Septicaemia'
			when 'ANAEMIA' then 'Anaemia'
			when 'ANEAMIA' then 'Anaemia'
			when 'ACUTE KIDNEY INJURY' then 'Acute kidney injury'
			when 'ACUTE SEVERE DEHYDRATION' then 'Acute severe dehydration'
			when 'SEPTICEAMIA' then 'Septicaemia'
			when 'GASTROENTERITIS' then 'Gastro enteritis'
			when 'CONGESTIVE CARDIAC FAILURE' then 'Congestive cardiac failure'
			when 'CCF' then 'Congestive cardiac failure'
			when 'SEVERE' then 'Severe'
			when 'ccf' then 'Congestive cardiac failure'
			when 'CONVULSIVE DISORDER' then 'Convulsive disorder'
			when 'covid 19' then 'COVID-19'
			when 'DELAYED MILDSTONE' then 'Delayed milestone'
			when 'SICKLE CELL DISEASE' then 'Sickle cell disease'
			when 'MALNUTRITION' then 'Malnutrition'
			when 'septicaemia' then 'Septicaemia'
			when 'sickle cell disease' then 'Sickle cell disease'
			when 'MENINGITIS' then 'Meningitis'
			when 'CEREBRAL PALSY WITH EPILEPSY' then 'Cerebral palsy with epilepsy'
			when 'malnurtrition' then 'Malnutrition'
			when 'SICKLE CELL CRISIS' then 'Sickle cell crisis'
			when 'DOWN SYNDROME' then 'Down syndrome'
			when 'FEBRILE CONVULSION' then 'Febrile convulsion'
			when 'PAINFUL CRISIS IN SICKLE CELL' then 'Painful crisis in sickle cell'
			when 'PAINFUL CRISIS IN KNOWN SICKLER' then 'Painful crisis in sickle cell'
			when '?' then ','
			when 'SOME DEHYDRATION' then 'Dehydration'
			when 'RHEUMATIC HEART DISEASE' then 'Rheumatice heart disease'
			when 'HYPERGLYCEAMIA IN KNOWN DIABETIC' then 'Hyperglyceamia in known diabetic'
			when 'RICKETS' then 'Rickets'
			when 'SCABIES' then 'Scabies'
			when 'TONSILITIS' then 'Tonsilitis'
			when 'VISCERAL LEISHMANIASIS' then 'Visceral leishmaniasis'
			when 'HEPATITIS' then 'Hepatitis'
			when 'MALARIA' then 'Malaria'
			when 'ACUTE FEBRILE ILLNESS' then 'Acute febrile illness'
			when 'ACUTE FEBILE ILLNESS' then 'Acute febrile illness'
			when ',' then ''
			when 'ABDOMINAL DISTENSION' then 'Abdominal distension'
			when 'meningitis' then 'Meningitis'
			when 'Convulsions' then 'Convulsion'
			when 'Convulsion' then 'Convulsions'
			else "Otheradmissiondiagnosis"
		end as "Otheradmissiondiagnosis"
	from {{ ref('stg_afi_physical_abstraction') }} 
)
select
	coalesce(gender.gender_key, 'unset') as gender_key,
    coalesce(age_group.age_group_key, 'unset') as age_group_key,
    coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
    coalesce(facility.facility_key, 'unset') as facility_key,
    coalesce(date.date_key, 'unset') as date_key,
	coalesce("Admissiondiagnosis", 'unset') as  admission_diagnosis,
    coalesce("Otheradmissiondiagnosis", 'unset') as other_admission_diagnosis,
	count(*) as no_of_cases
from cleaned_data
left join {{ ref('stg_afi_enroll_and_household_info') }} as enroll on enroll."PID" = cleaned_data."PID" 
left join {{ ref('dim_gender') }} as gender on gender.code = enroll."Gender"
left join {{ ref('dim_age_group_afi_and_mortality') }} as age_group on enroll."Ageyrs" >= age_group.start_age 
    and  enroll."Ageyrs"  < age_group.end_age 
left join {{ ref('dim_epi_week') }} as epi_week on enroll.interview_date  >= epi_week.start_of_week 
    and enroll.interview_date <= epi_week.end_of_week 
left join {{ ref('dim_facility') }} as facility on facility.code = left(cleaned_data."PID", 3) 
left join {{ ref('dim_date') }} as date on date.date = enroll.interview_date
group by 
	coalesce(gender.gender_key, 'unset'),
    coalesce(age_group.age_group_key, 'unset'),
    coalesce(epi_week.epi_week_key, 'unset'),
    coalesce(facility.facility_key, 'unset'),
    coalesce(date.date_key, 'unset'),
	coalesce("Admissiondiagnosis", 'unset'),
    coalesce("Otheradmissiondiagnosis", 'unset')