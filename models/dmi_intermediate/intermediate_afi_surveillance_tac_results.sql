with nested_data as (
    select 
        "Unique_ID",
        pid,
        "Target",
        "TacResult"
    from {{ ref('stg_afi_surveillance') }}
),

split_data AS (

    SELECT 
        "Unique_ID",
        pid,
        unnest(string_to_array("Target",',')) as Target,
        "TacResult" as Result
    FROM nested_data
)

SELECT 
    "Unique_ID",
    pid,
    CASE WHEN Target IN ('P. vivax', 'P. falciparum') THEN 'Plasmodium'


    
         WHEN Target  =  'S. Typhi' THEN 'Salmonella'
         WHEN Target = 'C. burnetii' THEN 'C.burnetii'
         WHEN Target = 'Dengue' THEN 'Dengue fever'
         WHEN Target = 'S. pneumoniae' THEN 'S.pneumoniae'
         WHEN Target = 'B. pseudomallei' THEN 'B.pseudomallei'
         WHEN Target =  'Not tested' THEN NULL
    ELSE Target
    END AS Target,
    Result
 From split_data