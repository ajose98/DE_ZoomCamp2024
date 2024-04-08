/* Data used: 
- https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-09.parquet
- https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

Steps performed:
1. Run the docker-compose file to initiate the container running the Postgres db and the container
running pgAdmin
2. Build a docker image to run the python script that ingests the data into the database
3. Run the image mentioned in 2. 
4. Open pgAdmin and run the queries
*/

-- How many taxi trips were totally made on September 18th 2019?
-- note: the datetime columns are of type timestamp
select count(*)
from green_taxi_trips
where "lpep_pickup_datetime"::date = '2019-09-18' and "lpep_dropoff_datetime"::date = '2019-09-18'
-- ANSWER: 15 612

-- Which was the pick up day with the longest trip distance? 
-- Use the pick up time for your calculations.
select "lpep_pickup_datetime"::date
from green_taxi_trips
order by "trip_distance" desc
limit 1
-- ANSWER: 2019-09-26

-- Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown
-- Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
select z."Borough"
from green_taxi_trips gtt, zones z
where gtt."PULocationID" = z."LocationID" 
	and gtt."lpep_pickup_datetime"::date = '2019-09-18' 
	and z."Borough" != 'Unknown'
group by z."Borough"
having sum(gtt."total_amount") > 50000
-- ANSWER: "Brooklyn", "Manhattan" and "Queens"

-- For the passengers picked up in September 2019 in the zone name Astoria which was the drop off
-- zone that had the largest tip? We want the name of the zone, not the id.
select zones_do."Zone"
from green_taxi_trips gtt, zones z_pu, zones zones_do
where gtt."PULocationID" = z_pu."LocationID" and z_pu."Zone" = 'Astoria'
	and gtt."DOLocationID" = zones_do."LocationID"
	and DATE_PART('MONTH', gtt."lpep_pickup_datetime") = '09'
	and DATE_PART('YEAR', gtt."lpep_pickup_datetime") = '2019'
order by gtt."tip_amount" desc
limit 1
-- ANSWER: "JFK Airport"

/* note: the table green_taxi_trips should only have data from september 2019, however by running
the query below I found there are 64 rows which pick up date is not in september 2019. That's the
reason for those 2 conditions using the function DATE_PART() in the above query.*/
select count(*)
from green_taxi_trips gtt
where DATE_PART('MONTH', gtt."lpep_pickup_datetime") != '09'
	or DATE_PART('YEAR', gtt."lpep_pickup_datetime") != '2019'
-- RESULT: 64