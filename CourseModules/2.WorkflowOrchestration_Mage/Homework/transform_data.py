if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import re

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Remove rows where the passenger count is equal to 0 and the trip distance is equal to zero
    data = data[(data['passenger_count']>0)&(data['trip_distance']>0)]

    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date
    # Homework question 3:
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # Homework question 5 preparation:
    original_columns = data.columns

    # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id
    data.columns = (data.columns
                    .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                    .str.lower()
    )

    print(f'Homework question 2: nr of rows in the dataframe - {len(data)}')

    print(f'Homework question 4: unique values in vendor_id - {list(data.vendor_id.unique())}')

    # Homework question 5:
    columns_name_changed = set(original_columns) - set(data.columns) 
    print(f'Homework question 5: columns needing name change - {len(columns_name_changed)} ({columns_name_changed})')
   
    return data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output.columns, 'The vendor_id column does not exist'
    assert len(output[output['passenger_count']<=0]) == 0, 'There are still rows with passenger_count <= 0'
    assert len(output[output['trip_distance']<=0]) == 0, 'There are still rows with trip_distance <= 0'
