import pandas as pd
import logging 
import os
from typing import List

from src._utils import (
    setup_logging,
    clean_brand_name,
    clean_generic_name,
)


setup_logging()
logger = logging.getLogger(__name__)

def get_ref_drug_names(ref_path):
    # Load ProstateDrugList.csv
    ref_df = pd.read_csv(ref_path)
    brand_cols = [col for col in ref_df.columns if col.startswith('Brand_name')]

    # convert values in brand_cols and Generic_name to list
    ref_drug_names = []
    for col in brand_cols:
        ref_drug_names.extend(ref_df[col].drop_duplicates().dropna().to_list())
    # clean brand names
    ref_drug_names = [clean_brand_name(name) for name in ref_drug_names]
    # get generic names
    generic_names = ref_df['Generic_name'].drop_duplicates().dropna().to_list()
    # clean generic names   
    generic_names = [clean_generic_name(name) for name in generic_names]
    # combine brand and generic names
    ref_drug_names.extend(generic_names)
    # double check for duplicates
    return list(set(ref_drug_names))

def get_op_raw_path(year, dataset_type):
    """
    Get the path to the raw Open Payments data for a given year and dataset type
    """
    acronym = "GNRL" if dataset_type == "general" else "RSRCH"
    parent_dir = "data/raw/"
    dataset_dir = f"{dataset_type}_payments/"
    prefix = f"OP_DTL_{acronym}_PGYR{year}"
    # Find the file in dataset_dir that starts with prefix
    for file in os.listdir(parent_dir + dataset_dir):
        if file.startswith(prefix):
            return os.path.join(parent_dir + dataset_dir, file)
    # log ValueError if not file found
    logger.error(f"No file found for {year} {dataset_type}_payments")
    raise ValueError(f"No file found for {year} {dataset_type}_payments")

def get_op_drug_columns(df: pd.DataFrame) -> List[str]:
    """Get columns that contain drug names, case-insensitive"""
    prefix = "name_of_drug_or_biological_or_device_or_medical_supply_"
    return [col for col in df.columns if col.lower().startswith(prefix.lower())]

def find_matches_op(chunk, drug_cols, ref_drug_names):
    chunk_row_idx = []
    chunk_matched_rows = 0
    # iterate through rows
    for idx, row in chunk.iterrows():
        # iterate through drug_cols
        for col in drug_cols:
            drug_name = str(row[col])
            if pd.isna(drug_name) or drug_name == 'nan':
                continue
            elif drug_name == '':
                continue
            # Apply clean_brand_name and then check against drug_names
            drug_name = clean_brand_name(drug_name)
            for tgt_name in ref_drug_names:
                if drug_name == tgt_name:
                    chunk_row_idx.append(idx)
                    chunk_matched_rows += 1
                    break  # Break out of tgt_name loop once we find a match
            if drug_name == tgt_name:  # If we found a match in drug_names loop
                break  # Break out of col loop to move to next row, else move to next column
    filtered_chunk = chunk.loc[chunk_row_idx] # using row labels, not positions, so changed from iloc to loc
    return filtered_chunk

def filter_open_payments(year, dataset_type, ref_path):
    """
    Filter Open Payments data for a given year and dataset type
    (General or Research) based on exact drug name matches
    """
    # get cleaned drug names (brand and generic) from ProstateDrugList.csv
    ref_drug_names = get_ref_drug_names(ref_path)

    # # Load OP data for year/type
    op_path = get_op_raw_path(year, dataset_type)
    logger.info("Raw data file: %s", op_path)
    # load csv in chunks
    chunksize = 100_000
    chunks = pd.read_csv(op_path, chunksize=chunksize, dtype=str)

    # Get drug columns
    op_drug_cols = get_op_drug_columns(pd.read_csv(op_path, nrows=1))
    # create dir_out if doesn't exist
    dir_out = f"data/filtered/{dataset_type}_payments/{year}_chunks/"
    os.makedirs(dir_out, exist_ok=True)

    logger.info("Looking for matches")
    total_matched_rows = 0
    # Filter each chunk using exact drug name matches
    for i, chunk in enumerate(chunks):
        logger.info("Processing chunk %s", i)
        filtered_chunk = find_matches_op(chunk, op_drug_cols, ref_drug_names)
        # Save to CSV if filtered chunk is not empty
        if not filtered_chunk.empty:
            filtered_chunk.to_csv(f"{dir_out}{dataset_type}_{year}_chunk_{i}.csv", index=False)
            logger.info("Saved chunk %s, found %s matches", i, len(filtered_chunk))
            total_matched_rows += len(filtered_chunk)
        else:
            logger.info("Didn't find any matches in chunk %s", i)

    logger.info("Matched %s rows for %s %s", total_matched_rows, year, dataset_type)


