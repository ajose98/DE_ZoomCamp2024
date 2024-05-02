To perform the homework, two containers running in the same network were initated using docker-compose:

<p><img src="https://github.com/ajose98/DE_ZoomCamp2024/assets/55530285/6e9cef02-2110-4103-ab56-f650340d19ef" alt="drawing" width="700"/></p>

The pgdatabase container was running a Postgres database called "ny_taxi" that contains 2 tables:
- _green_taxi_trips_ (contains data made available by NYC Taxi and Limousine Commission (TLC) on the trips performed in September 2019)
- _zones_ (contains detailed information about the zones where the vehicles operate)
<p>data source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page</p>

The data in _green_taxi_trips_ table was inserted in the database using the script [ingest_data.py](https://github.com/ajose98/DE_ZoomCamp2024/blob/main/CourseModules/1.Docker_Terraform/(1)%20Docker_SQL/ingest_data.py). The original file is in parquet format and can be found [here](https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-09.parquet).

The data in _zones_ was inserted using a simplified version of that script (no need to load data in batches as it the file is much smaller; the file format is csv). It is available [here](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv).

The queries in [queries.sql](https://github.com/ajose98/DE_ZoomCamp2024/blob/main/CourseModules/1.Docker_Terraform/Homework/queries.sql) were tested in pgAdmin.
