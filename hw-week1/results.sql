select count(*) from green_taxi_data 
where cast(lpep_pickup_datetime as DATE) between '2025-11-01' and '2025-11-30'
and trip_distance<=1;
--8,007

select dt from (
select 
cast(lpep_pickup_datetime as DATE) as dt,
rank() over (order by trip_distance desc) as rnk
from green_taxi_data 
where trip_distance<100) 
where rnk=1;
--2025-11-14

select taxi_zone_lookup."Zone", sum(total_amount) 
from green_taxi_data 
left join taxi_zone_lookup on green_taxi_data."PULocationID" = taxi_zone_lookup."LocationID"
where cast(lpep_pickup_datetime as DATE) = '2025-11-18'
group by 1
order by 2 desc;
--East Harlem North

select doz."Zone", sum(tip_amount) 
from green_taxi_data 
left join taxi_zone_lookup pu
	on green_taxi_data."PULocationID" = pu."LocationID"
left join taxi_zone_lookup doz
	on green_taxi_data."PULocationID" = doz."LocationID"	
where cast(lpep_pickup_datetime as DATE) between '2025-11-01' and '2025-11-30'
and pu."Zone" = 'East Harlem North'
group by 1
order by 2 desc;
--East Harlem North
