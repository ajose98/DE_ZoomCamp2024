if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    common_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_'
    year_months_to_load = ['2020-10', '2020-11', '2020-12']
    
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fdw_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float

    }

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    for idx, year_month in enumerate(year_months_to_load):
        current_url = common_url + year_month + '.csv.gz'
        current_df = pd.read_csv(current_url, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)

        if idx == 0:
            df = current_df.copy()
        
        else:
            df = pd.concat([df, current_df])
    
    print(f'Homework question 1: shape of dataframe - {df.shape}')

    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
