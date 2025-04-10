
WITH age_group_data AS (
    SELECT   
        start_age_days,
        end_age_days,
        
    CASE 
        WHEN start_age_days >= 0 and start_age_days < 365 THEN '0 - < 1yr'
        WHEN start_age_days >= 365 AND start_age_days < 730 THEN '1 - < 2yrs'
        WHEN start_age_days >= 730 AND start_age_days < 1825 THEN '2 - < 5yrs'
        WHEN start_age_days >= 1825 AND start_age_days < 6205 THEN '5 - 17yrs'
        WHEN start_age_days >= 6205 AND start_age_days < 18250 THEN '18 - 50yrs'
         WHEN start_age_days >= 18250 THEN '51+ yrs'
    END AS age_group_category
    
    FROM (VALUES
        (0, 365),       
        (365, 730),     
        (730, 1825),   
        (1825, 6205),   
        (6205, 18250),  
        (18250, 50000)  
    ) AS age_group(start_age_days, end_age_days)	
),

final_data AS (
    SELECT 
      {{ dbt_utils.surrogate_key( ['age_group_data.age_group_category']) }} AS age_group_key,  -- ✅ Uses dbt_utils.surrogate_key() for surrogate key
        start_age_days,
        end_age_days,
        age_group_category
    FROM age_group_data 

    UNION 

    SELECT 
        'unset' AS age_group_key,
        -999 AS start_age_days,
        -999 AS end_age_days,
        'unset' AS age_group_category  
)

SELECT 
    final_data.*,
    CAST(current_date AS DATE) AS load_date
FROM final_data
