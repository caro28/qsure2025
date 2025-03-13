import json
import logging
import requests

from typing import List, Dict


log = logging.getLogger(__name__)

def _get_datastore_uuids(start_year: int, end_year: int):
    # Define the API endpoint
    url = "https://openpaymentsdata.cms.gov/api/1/metastore/schemas/dataset/items?show-reference-ids"

    # Send a GET request to the API endpoint
    response = requests.get(url)
    data = response.json()

    # Initialize a dictionary to hold the UUIDs for each year
    datastore_uuids = {}

    # Iterate through the datasets in the response
    for idx, dataset in enumerate(data):
        # Extract the title and identifier
        title = dataset['title']
        identifier = dataset['distribution'][0]['identifier']
    
        # Check if the title contains any of the years in the desired range
        for year in range(start_year, end_year + 1):
            if str(year) in title:
                log.info("Found datastore uuid for year: %s", year)
                datastore_uuids[title] = identifier
                continue
    
    return datastore_uuids


def _save_datastore_uuids():
    uuids = _get_datastore_uuids(2014, 2023)
    with open("data/reference/all_datastore_uuids.json", "w") as f:
        json.dump(uuids, f)


def _sql_query_by_col(
        cols: str,
        datastore_uuid: str,
        LIMIT: int,
        OFFSET: int,
        ) -> List[dict]:
    # Get all values for a column using SQL-like query

    base_url = "https://openpaymentsdata.cms.gov/api/1"

    query = "".join([
        f"[SELECT {cols} FROM {datastore_uuid}]",
        f"[LIMIT {LIMIT} OFFSET {OFFSET}]"
    ]
    )

    url = f"{base_url}/datastore/sql?query={query}&show_db_columns"
    log.info("URL: %s", url)

    response = requests.get(url)
    log.info("\nResponse status code: %s", response.status_code)

    if response.status_code == 200:
        data = response.json()
        log.info("\nNumber of records: %s", len(data))
        log.info("\nNumber of cols: %s", len(data[0].keys()))
        if data:
            return(data)
    else:
        print(f"\nError: {response.status_code}")
        print(f"Error message: {response.text}")


def _discover_drug_columns(uuid: str) -> list:
    """
    Discover all drug name columns in the dataset.
    Args:
        uuid: Dataset UUID from DATASTORE_UUIDS
    Returns:
        list: List of discovered drug column names
    """
    drug_col_prefix = "name_of_drug_or_biological_or_device_or_medical_supply_"
    col_num = 1
    drug_cols = []
    while True:
        try:
            # Try to fetch one record with the current drug column
            test_col = f"{drug_col_prefix}{col_num}"
            test_batch = _sql_query_by_col(
                cols=test_col,
                datastore_uuid=uuid,
                LIMIT=1,
                OFFSET=0
            )
            if not test_batch:  # No more columns found
                break   
            drug_cols.append(test_col)
            col_num += 1
        except Exception as e:
            log.info(f"No more drug columns found after {col_num-1} columns")
            break
    log.info(f"Found {len(drug_cols)} drug columns: {drug_cols}")
    return drug_cols