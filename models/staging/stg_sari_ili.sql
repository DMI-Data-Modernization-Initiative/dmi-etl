select 
	PatientID as patient_id,
	cast(Datescreened as date) as date_screened,
	cast(EnrolmentDate as date) as date_enrolled,
	Eligible as eligible,
	Enrolled as enrolled,
	F_NAME as facility_name,
	round(Age, 0) as age_in_years,
	case 
		when Sex = 1 then 'Male'
		else 'Female' 
	end as gender,
	cast(Dateonset as date) as date_onset,
	casedef as case_def,
	chronic,
	flutest,
	covidtest,
	cast(Dateassesment as date) as date_assesment,
	outcome,
	try_cast(colldate as date) as date_collected,
	samplecollected,
	sampletested,
	try_cast(datetested as date) as date_tested,
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
	covidpos,
	case left(county, charindex(' ', county) - 1)
		when 'Murang?a' then 'Murang''a'
		when 'Homa' then 'Homa Bay'
		when 'Tharaka' then 'Tharaka-Nithi'
		when 'Taita' then 'Taita Taveta'
		when 'Tana' then 'Tana River'
		when 'Uasin' then 'Uasin Gishu'
		else left(county, charindex(' ', county) - 1)
	end  as county,
	Longitude as longitude,
	Latitude as latitude
from {{ source('central_raw_sari_ili', 'SARI_ILI_data') }}  as raw_sari