select 
  "Unique_ID", 
  "ID", 
  "Source", 
  "PID", 
  "SampleCollected", 
  "Barcode", 
  "SpecimenCollDate", 
  "SpecimenCollTime", 
  rec_status, 
  "Datecreated"::date as "Datecreated", 
  "Createdby", 
  "Dateupdated", 
  "Updatedby", 
  "Specimencollected", 
  "SpecimenReason", 
  "SpecimenReasonother", 
  "ProcessedAt", 
  "MigratedAt", 
  "MigratedAtBarcode", 
  "UUID", 
  "UUIDBarcode" 
from  {{ source('central_raw_afi', 'malaria_rdt') }}

