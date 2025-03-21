import pandas as pd


def get_row_ids(
        chunk_dir: str,
        drug_names: list[str],
) -> list[str]:
    """
    Get the row ids of all records with match drug names
    Params
        chunk_dir (str): File path to the dir with chunked parquet files
        drug_ref_file (str): File path to the Excel file with drug names
    Returns
        final_record_ids (list[str]): List of row ids
    """
    # 1. Get drug names

    # 2. Loop through chunk files:
        # Find rows with matching drug names and save to list
    
    # return row_ids


def download_dataset_slice(
          record_ids: list[str],
          pathout: str,
) -> pd.DataFrame:
    """
    Download the dataset slice using record ids and write to parquet
    Params
        record_ids (list[str]): List of record ids
        pathout (str): File path to save the dataset slice
    Returns
        dataset (pd.DataFrame): Dataset slice
    """
    # 1. Validate dataset size

    # 2. Loop through record_ids and pull full row from API using record_id

    # 3. Write to parquet and return dataset


def harmonize_columns(
        dataset: pd.DataFrame,
        col_mapping: dict,
) -> pd.DataFrame:
    """
    Harmonize the columns of the dataset
    Params
        dataset (pd.DataFrame): Dataset to harmonize
        col_mapping (dict): Dictionary of column mappings
    """
    # 1. Change column names

    # return dataset


def add_harmonized_drug_name_col(
        dataset: pd.DataFrame,
        drug_name_mapping: dict,
) -> pd.DataFrame:
    """
    Add column drug_name with generic names only
    """
    # Use mapping to extract and add generic name per row

    # return dataset


def add_drug_type(
        dataset: pd.DataFrame,
        drug_data: dict
) -> pd.DataFrame:
    """
    Add col with drug type based on "yellow" or "green" value in ProstrateDrugList.csv
    """

    # return dataset


def add_onc_prescriber(
        dataset: pd.DataFrame,
        drug_data: dict,
        npi_data: dict
) -> pd.DataFrame:
    """
    """


def add_npi(
        dataset: pd.DataFrame,
        npi_data: dict,
) -> pd.DataFrame:
    """
    Add NPI column
    """
    # Add NPI column from Covered Recipient Profile Supplement database using Covered Recipient Profile ID
    
    # return dataset


