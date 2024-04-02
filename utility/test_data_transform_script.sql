drop table dbt_shield_dev.aggregate_test_data; 
	
select 
	region,
	count(*) as count,
	avg(age) as avg_age,
	max(age) as max_age,
	min(age) as min_age
into dbt_shield_dev.aggregate_test_data
from dbt_shield_dev.test_data
group by region;

	
-------------------------------------------------------------------------------
	
drop table dbt_shield_dev.partition_test_data; 

select 
	name,
	age,
	date_created,
	region,
	row_number() over (partition by region order by date_created desc) as position
into dbt_shield_dev.partition_test_data
from dbt_shield_dev.test_data;