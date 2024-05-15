-- CREATE A ML TABLE WITH APPROPRIATE TYPE
CREATE OR REPLACE TABLE `taxi-data-project.ny_taxi.yellow_tripdata_ml` (
`passenger_count` INTEGER,
`trip_distance` FLOAT64,
`PULocation_ID` STRING,
`DOLocation_ID` STRING,
`payment_type` STRING,
`fare_amount` FLOAT64,
`tolls_amount` FLOAT64,
`tip_amount` FLOAT64
) AS (
SELECT passenger_count, trip_distance, cast(PULocation_ID AS STRING), CAST(DOLocation_ID AS STRING),
CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount
FROM `taxi-data-project.ny_taxi.external_yellow_tripdata_partitioned` WHERE fare_amount != 0
);

-- CREATE MODEL WITH DEFAULT SETTING
CREATE OR REPLACE MODEL `taxi-data-project.ny_taxi.tip_model`
OPTIONS
(model_type='linear_reg',
input_label_cols=['tip_amount'],
DATA_SPLIT_METHOD='AUTO_SPLIT') AS
SELECT
*
FROM
`taxi-data-project.ny_taxi.yellow_tripdata_ml`
WHERE
tip_amount IS NOT NULL;

-- CHECK FEATURES
SELECT * FROM ML.FEATURE_INFO(MODEL `taxi-data-project.ny_taxi.tip_model`);

-- EVALUATE THE MODEL
SELECT *
FROM ML.EVALUATE(MODEL `taxi-data-project.ny_taxi.tip_model`,
(
SELECT *
FROM `taxi-data-project.ny_taxi.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL
));


-- PREDICT THE MODEL
SELECT *
FROM ML.PREDICT(MODEL `taxi-data-project.ny_taxi.tip_model`,
(
SELECT *
FROM `taxi-data-project.ny_taxi.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL
));

-- PREDICT AND EXPLAIN
SELECT *
FROM ML.EXPLAIN_PREDICT(MODEL `taxi-data-project.ny_taxi.tip_model`,
(
SELECT *
FROM `taxi-data-project.ny_taxi.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL
), STRUCT(3 as top_k_features));

-- HYPER PARAM TUNNING
CREATE OR REPLACE MODEL `taxi-data-project.ny_taxi.tip_hyperparam_model`
OPTIONS
(model_type='linear_reg',
input_label_cols=['tip_amount'],
DATA_SPLIT_METHOD='AUTO_SPLIT',
num_trials=5,
max_parallel_trials=2,
l1_reg=hparam_range(0, 20),
l2_reg=hparam_candidates([0, 0.1, 1, 10])) AS
SELECT *
FROM `taxi-data-project.ny_taxi.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL;