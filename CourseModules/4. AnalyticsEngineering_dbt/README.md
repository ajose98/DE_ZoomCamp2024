This module is about Analytics Engineering. Dbt is the tool used.

Below is a brief overview of what is covered.

- Kimball's dimensional modeling
- Loading data from Big Query into dbt
- Creating a dbt model & Macros
- Defining variables & Loading existing packages
- Testing & Documenting dbt models
- Deploying dbt models using Continuous Integration (CI)
- Building a dashboard using Looker Studio (previous Google Data Studio)

The model created can be seen below. The "fhv" related modules were built as part of the homework.

A data mart with information on the revenue per month, location and service type was created (dm_monthly_zone_revenue).
![image](https://github.com/ajose98/DE_ZoomCamp2024/assets/55530285/438b5479-fcb0-4557-a374-361094802d97)

Dbt was directly connected to Git and in the folder "dbt_taxi_rides_ny" is saved all the work done in dbt.

A dashboard showing some insights on fact_trips was created and its downloaded version can be seen [here](https://github.com/ajose98/DE_ZoomCamp2024/blob/main/CourseModules/4.%20AnalyticsEngineering_dbt/LookerStudio).

The NYC taxi datasets were used (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
