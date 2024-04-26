with classification_cte as (
select
	history_and_physical_exam."PID",
    case 
        when physical_abstraction.merscov_status = 1 then 'MERS-CoV'
        when physical_abstraction."sariStatus" = 1 and physical_abstraction.merscov_status <> 1 then 'SARI'
        when history_and_physical_exam."Cough"  = 1 and history_and_physical_exam."Coughdays" > 10 and history_and_physical_exam."Coughdays" <= 14 then 'UF'
        when physical_abstraction."sariStatus" <> 1 and physical_abstraction."Fevercriteria" = 1 and 
             history_and_physical_exam."Diarrhea" = 1 and history_and_physical_exam."Loosestools" <> 1 or history_and_physical_exam."Diarrhea" <> 1 then 'UF'
        when physical_abstraction."sariStatus" <> 1 and  history_and_physical_exam."Diarrhea" = 1 and history_and_physical_exam."Loosestools" = 1 then 'DF'
        else 'Non UF/SARI/DF/MERS-CoV'
    end as CaseClassification
 from {{ ref('stg_afi_history_and_physical_exam') }}  as history_and_physical_exam
 left join {{ ref('stg_afi_physical_abstraction') }} as physical_abstraction on physical_abstraction."PID" = history_and_physical_exam."PID" 
)
select 
	* 
from classification_cte

