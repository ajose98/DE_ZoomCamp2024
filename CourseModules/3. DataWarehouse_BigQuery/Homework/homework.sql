CREATE OR REPLACE EXTERNAL TABLE green-taxi-rides.green_taxi_2022.external_green_tripdata
OPTIONS (
format = 'PARQUET',
uris = ['gs://green-tripdata/green_tripdata_2022-*.parquet']
);

CREATE OR REPLACE TABLE green-taxi-rides.green_taxi_2022.green_tripdata_non_partitoned AS
SELECT * FROM green-taxi-rides.green_taxi_2022.external_green_tripdata;

-- Question 1: What is count of records for the 2022 Green Taxi Data?
Select count(*)
From green-taxi-rides.green_taxi_2022.green_tripdata_non_partitoned;
-- Answer: 840 402

-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
Select count(distinct PULocationID)
From green-taxi-rides.green_taxi_2022.external_green_tripdata;
-- Answer: 0 MB for the external table
Select count(distinct PULocationID)
From green-taxi-rides.green_taxi_2022.green_tripdata_non_partitoned;
-- Answer: 6.41 MB for the materialized table

-- Question 3: How many records have a fare_amount of 0?
Select count(*)
From green-taxi-rides.green_taxi_2022.green_tripdata_non_partitoned
Where fare_amount = 0;
-- Answer: 1 622

-- Question 4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? 
-- (Create a new table with this strategy)
CREATE OR REPLACE TABLE green-taxi-rides.green_taxi_2022.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM green-taxi-rides.green_taxi_2022.external_green_tripdata;
-- Answer: Partition by lpep_pickup_datetime and Cluster by PUlocationID

-- Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?
Select distinct PULocationID
From green-taxi-rides.green_taxi_2022.green_tripdata_non_partitoned
Where DATE(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';
-- Answer: 12.82 MB
Select distinct PULocationID
From green-taxi-rides.green_taxi_2022.green_tripdata_partitoned_clustered
Where DATE(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';
-- Answer: 1.12 MB

-- Question 6: Where is the data stored in the External Table you created?
-- Answer: GCP Bucket

-- Question 7: It is best practice in Big Query to always cluster your data:
-- Answer: False

-- Question 8: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
Select count(*)
From green-taxi-rides.green_taxi_2022.green_tripdata_non_partitoned;
-- Answer: because the result is cached (from question 1)