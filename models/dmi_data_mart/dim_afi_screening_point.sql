WITH screeningpoint_data AS (
  SELECT DISTINCT   
        {{ dbt_utils.surrogate_key(['screeningpoint']) }} AS screeningpoint_key,
        screeningpoint, 
        CASE screeningpoint
            WHEN 1 THEN 'Inpatient' 
            WHEN 2 THEN 'Outpatient'
            WHEN 3 THEN 'Accident & Emergency'
            WHEN 4 THEN 'Isolation'
            WHEN 5 THEN 'Other'
        END AS screeningpoint_description
  FROM dbt_shield_dev.stg_afi_surveillance 
  WHERE screeningpoint IN (1, 2, 3, 4, 5)

  UNION 

  SELECT 
      'unset' AS screeningpoint_key,
      -999 AS screeningpoint,  -- Fixed column name
      'unset' AS screeningpoint_description
)

SELECT 
    screeningpoint_data.*,
    CAST(CURRENT_DATE AS DATE) AS load_date
FROM screeningpoint_data
