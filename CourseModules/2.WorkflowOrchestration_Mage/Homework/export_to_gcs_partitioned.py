# python block to export data as partitioned parquet files

import pyarrow as pa # pyarrow is in the docker image so it should be ok
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# the line below will tell pyarrow the file where gcp credentials are located
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/mage_keys.json'

bucket_name = 'mage-zoomcamp-sofiaj-1'
project_id = 'nyc-green-taxi-data-partitioned'

table_name = 'nyc_green_taxi_data'
root_path = f'{bucket_name}/{table_name}'


@data_exporter
def export_data(data, *args, **kwargs):
    # we need a date column (not datetime), let's create it
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # create pyarrow table
    table = pa.Table.from_pandas(data)

    # define google cloud storage object
    gcs = pa.fs.GcsFileSystem() # this allows us using our environment variable automatically

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )

    