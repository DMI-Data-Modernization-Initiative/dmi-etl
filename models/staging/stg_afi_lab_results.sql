select 
  "Unique_ID", 
  "ID", 
  "Source", 
  "PID", 
  "Barcode", 
  "ResultValue", 
  rec_status, 
  "Datecreated" as date_created, 
  "Createdby", 
  "Dateupdated", 
  "Updatedby", 
  "WeightBefore", 
  "WeightAfter", 
  "Bactec", 
  "GramStain", 
  "Culture", 
  "Chloraphenicol", 
  "Trimethoprim", 
  "Tetracycline", 
  "Ciprofloxacin", 
  "Nalidixic", 
  "Ampicilin", 
  "Sulfixosazole", 
  "Streptomycin", 
  "Kanamycin", 
  "Gentamycin", 
  "Cefriaxone", 
  "Furazolidone", 
  "Erythromycin", 
  "Oxacilin", 
  case "MalariaSpecies"
    when '' then null
    when '1.0' then '1'
    when '2.0' then '2'
    when '3.0' then '3'
  end as "MalariaSpecies", 
  "ParacountThick", 
  "ParacountThin", 
  "MigratedAt", 
  "UUID" 
from  {{ source('central_raw_afi', 'lab_results') }}
