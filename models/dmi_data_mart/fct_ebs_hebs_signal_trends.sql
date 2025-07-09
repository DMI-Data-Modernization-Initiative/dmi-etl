WITH hebs_base AS (
    SELECT * 
    FROM {{ source('central_raw_mdharura', 'mdharura_ebs_linelist') }}
    WHERE "UNIT_PARENT_PARENT_NAME" IN (
        'Nakuru County', 'Siaya County', 'Nairobi County', 'Meru County', 'Mombasa County'
    )
    AND "SIGNAL" IN ('h1', 'h2', 'h3')
    AND "STATE" = 'live'
)

SELECT 
   coalesce(epi_week.epi_week_key, 'unset') as epi_week_key,
   coalesce(date.date_key, 'unset') as date_key,
   coalesce(county.county_key, 'unset') as county_key,
    "SIGNALID",
    "UNIT_PARENT_PARENT_NAME" as "COUNTY",
    "HEBS_VERIFICATIONFORM_ISTHREATSTILLEXISTING",
    "HEBS_INVESTIGATIONFORM_ISEVENTINFECTIOUS",
    "HEBS_INVESTIGATIONFORM_ID",
    "CREATEDAT"::date AS "CREATED_DATE",
    "SIGNAL",
    'HEBS' AS "SIGNAL_TYPE",
    cast(current_date as date) as load_date
FROM hebs_base


left join {{ ref('dim_date') }} as date on date.date = hebs_base."CREATEDAT"::date
left join {{ ref('dim_county') }}  as county on concat(county.county, ' ', 'County') = hebs_base."UNIT_PARENT_PARENT_NAME"
LEFT JOIN {{ ref('dim_epi_week') }} AS epi_week 
    ON hebs_base."CREATEDAT"::date BETWEEN epi_week.start_of_week AND epi_week.end_of_week 