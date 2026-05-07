--q1
select name, bike_stands, available_bikes
from raw_velo_stations
where available_bikes = 0
order by bike_stands desc
limit 15;

--q2: 
select name, bike_stands
from raw_velo_stations
where available_bikes = 0
order by bike_stands desc
limit 5;

--q3 
select name, available_bikes, available_bike_stands
from raw_velo_stations
where number = 2010
order by last_update desc
limit 5;

--q4 :
select name, status, available_bikes
from raw_velo_stations
where available_bikes = 0 or status = 'CLOSED'
limit 20;

--q5
select name, available_bikes, bike_stands
from raw_velo_stations
where bike_stands > 0
and available_bikes > bike_stands * 0.85
limit 15;

--q6 
select
    count(*) as total,
    sum(case when position.lat != 0 and position.lng != 0
         and status in ('OPEN','CLOSED') then 1 else 0 end) as valides
from raw_velo_stations;
