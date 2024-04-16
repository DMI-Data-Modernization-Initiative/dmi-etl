select 
	PatientID as patient_id,
	cast(Datescreened as date) as date_screened,
	cast(EnrolmentDate as date) as date_enrolled,
	Eligible as eligible,
	Enrolled as enrolled,
	F_NAME as facility_name,
	Age::decimal as age_in_years,
	case 
		when Sex = 1 then 'Male'
		else 'Female' 
	end as gender,
	cast(Dateonset as date) as date_onset,
	casedef as case_def,
	chronic,
	flutest,
	case 
		when covidtest = '' then NULL
		else covidtest
	end as covidtest,
	cast(Dateassesment as date) as date_assesment,
	outcome,
	cast(colldate as date) as date_collected,
	samplecollected,
	sampletested,
	cast(datetested as date) as date_tested,
	flupos as flu_positive,
	fluapos as flua_positive,
	h3n2,
	ph1n1,
	unsub_non,
	notsubtyp,
	flubpos as flub_positive,
	yamagata,
	victoria,
	flub_undetermined,
	case 
		when covidpos = '' then NULL
		else covidpos
	end as covidpos,
	case substring(county from '^(.*?) County')
		when 'Murang?a' then 'Muranga'
		when 'Homa' then 'Homa Bay'
		else substring(county from '^(.*?) County')
	end  as county,
	Longitude as longitude,
	Latitude as latitude
from {{ source('central_raw_sari_ili', 'sari_ili_data') }}  as raw_sari