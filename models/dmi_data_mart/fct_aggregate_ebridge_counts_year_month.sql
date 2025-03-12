with
    enriched_data as (
        select
            "CREATEDAT"::date as created_at,
            "UNIT_PARENT_CODE" as subcounty_code_2,
            case
                when "UNIT_PARENT_NAME" = 'Buuri  East Sub County'
                then 'Buuri East Sub County'
                else "UNIT_PARENT_NAME"
            end as subcounty,
            "UNIT_PARENT_PARENT_NAME" as county,
            "SIGNAL",
            population.subcounty_id
        from {{ ref("stg_mdharura_ebs_linelist") }} as ebs
        inner join
            {{ ref("sub_county_population") }} as population
            on population.subcounty_mdharura_id = ebs."UNIT_PARENT_ID"
    )
select
    concat(dim_date.year, lpad(dim_date.month::text, 2, '0')) as year_month,
    dim_date.year,
    lpad(dim_date.month::text, 2, '0') as month,
    coalesce(county.county_key, 'unset') as county_key,
    coalesce(sub_county.sub_county_key, 'unset') as sub_county_key,
    subcounty_id as subcounty_code,
    subcounty_code_2,
    sum(case when "SIGNAL" = '1' then 1 else 0 end) as count_c1,
    sum(case when "SIGNAL" = '2' then 1 else 0 end) as count_c2,
    sum(case when "SIGNAL" = '3' then 1 else 0 end) as count_c3,
    sum(case when "SIGNAL" = '4' then 1 else 0 end) as count_c4,
    sum(case when "SIGNAL" = '5' then 1 else 0 end) as count_c5,
    sum(case when "SIGNAL" = '6' then 1 else 0 end) as count_c6,
    sum(case when "SIGNAL" = '7' then 1 else 0 end) as count_c7,
    sum(case when "SIGNAL" = 'h1' then 1 else 0 end) as count_h1,
    sum(case when "SIGNAL" = 'h2' then 1 else 0 end) as count_h2,
    sum(case when "SIGNAL" = 'h3' then 1 else 0 end) as count_h3,
    cast(current_date as date) as load_date
from enriched_data
left join {{ ref("dim_date") }} as dim_date on enriched_data.created_at = dim_date.date
left join
    {{ ref("dim_county") }} as county
    on concat(county.county, ' ', 'County') = enriched_data.county
left join
    {{ ref("dim_sub_county") }} as sub_county
    on concat(sub_county.sub_county, ' ', 'Sub County') = enriched_data.subcounty
group by
    dim_date.year,
    dim_date.month,
    county.county_key,
    sub_county_key,
    subcounty_code,
    subcounty_code_2
