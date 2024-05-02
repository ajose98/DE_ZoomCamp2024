This module is about Workflow Orchestration and Mage is the tool used.

Below is a brief overview of what is covered.

- Run Docker-Compose to create a container for Mage and one for Postgres
- Create pipelines with Data Load, Transformer and Data Export blocks
- Import data from an url & Export data to Postgres, Google Cloud Storage (GCS) and Amazon S3 (extra)
- Export data in partitions to GCS (using Pyarrow library)
- Create triggers to automate the run of a pipeline

The NYC taxi datasets were used (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Multiple datasets were concatenated into one and transformations such as data type conversion, renaming of columns (from Camel case to Snake case) and data filtering were performed.
