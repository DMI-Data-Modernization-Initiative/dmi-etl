-- creating a table with test data
drop table dbt_shield_dev.test_data;

with test_data as (
	select
	    'Person ' || gs AS name,
	    floor(random() * 60 + 20) AS age,
	    '2020-01-01'::date + (floor(random() * 365) || 'days')::interval  as date_created,
	    case floor(random() * 3) 
        	when 0 then 'North'
       		when 1 then 'South'
        	when 2 then 'East'
        	else 'West'
        end as region
	from generate_series(1, 100000000) gs  -- Generating 1 million rows
)
select 
	name,
	age,
	date_created,
	region
into dbt_shield_dev.test_data
from test_data