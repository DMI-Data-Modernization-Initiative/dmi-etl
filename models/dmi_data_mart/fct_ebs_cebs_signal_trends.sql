-- models/intermediate/hebs_signal_events.sql


WITH cebs_base AS (
    SELECT * 
    FROM {{ source('central_raw_mdharura', 'mdharura_ebs_linelist') }}
    WHERE "UNIT_PARENT_PARENT_NAME" IN (
       'Nakuru County', 'Siaya County', 'Busia County', 'Meru County', 'Mombasa County'
    )
    AND "SIGNAL" IN ('1','2','3','4','5','6','7')
    AND "STATE" = 'live'
)

SELECT 
   coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
     coalesce(date.date_key, 'unset') as date_key,
   coalesce(county.county_key, 'unset') as county_key,
    "SIGNALID",
    "UNIT_PARENT_PARENT_NAME" AS "COUNTY",
    "CEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING",
    "CEBS_INVESTIGATIONFORM_ISEVENTINFECTIOUS",
    "CEBS_INVESTIGATIONFORM_ID",
    "CREATEDAT"::date AS "CREATED_DATE", 
    "SIGNAL",
    'CEBS' AS "SIGNAL_TYPE",
    cast(current_date as date) as load_date
FROM cebs_base


left join {{ ref('dim_date') }} as date on date.date = cebs_base."CREATEDAT"::date
left join {{ ref('dim_county') }}  as county on concat(county.county, ' ', 'County') = cebs_base."UNIT_PARENT_PARENT_NAME"
LEFT JOIN {{ ref('dim_epi_week') }} AS epi_week 
    ON cebs_base."CREATEDAT"::date BETWEEN epi_week.start_of_week AND epi_week.end_of_week 