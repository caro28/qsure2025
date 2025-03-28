import string
import re
import json
import os
import unicodedata
import logging
from datetime import datetime
import pandas as pd
import time
from functools import wraps
from typing import List
# from Levenshtein import distance

def setup_logging():
    """Configure logging to output to both file and console with timestamped filename."""
    # Create logs directory if it doesn't exist
    os.makedirs('data/logs', exist_ok=True)
    
    # Create timestamp for filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'data/logs/op_cleaner_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()  # This will also print to console
        ]
    )

setup_logging()
logger = logging.getLogger(__name__)


# def filter_open_payments_levenshtein(year, dataset_type, max_distance=1):
#     """
#     Filter Open Payments data for a given year and dataset type
#     (General or Research) based on drug name matches with 
#     Levenshtein distance <= 1
#     """
#     # get cleaned drug names (brand and generic) from ProstateDrugList.csv
#     ref_drug_names = get_ref_drug_names()

#     # # Load OP data for year/type
#     op_path = get_op_raw_path(year, dataset_type)
#     # Get drug columns
#     op_drug_cols = get_op_drug_columns(pd.read_csv(op_path, nrows=1))
    
#     # load csv in chunks
#     chunksize = 100_000
#     chunks = pd.read_csv(op_path, chunksize=chunksize)
    
#     matching_record_ids = set()
#     matched_rows = 0
#     # Filter each chunk using Levenshtein distance <= 1
#     for i, chunk in enumerate(chunks):
#         distances = []
#         # Iterate through each row
#         for idx, row in chunk.iterrows():
#             # Check each drug column
#             for col in op_drug_cols:
#                 drug_name = str(row[col])
#                 if pd.isna(drug_name) or drug_name == 'nan':
#                     continue
#                 elif drug_name == '':
#                     continue
#                 # Apply clean_brand_name and then check against each reference drug
#                 drug_name = clean_brand_name(drug_name)
#                 for ref_name in ref_drug_names:
#                     dist = distance(drug_name, ref_name)
#                     if dist <= max_distance:
#                         matching_record_ids.add(row['Record_ID'])
#                         distances.append(dist)
#                         # Break both loops once we find a match
#                         break
#                 else:
#                     continue
#                 break

#         filtered_chunk = chunk[chunk['Record_ID'].isin(matching_record_ids)]
#         logger.info("Processed %s chunks, Matched %s rows for %s %s", i+1, len(filtered_chunk), year, dataset_type)
#         matched_rows += len(filtered_chunk)

#         # Save to CSV if filtered chunk is not empty
#         if not filtered_chunk.empty:
#             # Create directory if it doesn't exist
#             os.makedirs(f"data/filtered/{dataset_type}_payments/chunks/", exist_ok=True)
#             # add column for distance
#             filtered_chunk['levenshtein_distance'] = distances
#             filtered_chunk.to_csv(f"data/filtered/{dataset_type}_payments/chunks/{dataset_type}_{year}_chunk_{i+1}.csv", index=False)
#             logger.info("Saved chunk %s for %s %s", i+1, year, dataset_type)

#     logger.info("Matched %s rows for %s %s", matched_rows, year, dataset_type)



# def save_chunk_to_parquet(chunk: list, chunk_num: int, output_dir: str, file_prefix: str):
#     """
#     Save a chunk of data to a numbered parquet file.
#     Args:
#         chunk: List of dictionaries containing the chunk data
#         chunk_num: Chunk number for filename
#         output_dir: Directory to save the file
#         file_prefix: Prefix for the filename (e.g., 'general_payments_2023')
#     Returns:
#         str: Path to saved file
#     """
#     df = pd.DataFrame(chunk)
#     filename = f"{file_prefix}_chunk_{chunk_num:03d}.parquet"
#     filepath = os.path.join(output_dir, filename)
#     df.to_parquet(filepath, compression='snappy')
#     return filepath


def clean_brand_name(token: str) -> str:
    if not isinstance(token, str):
        return ""
    # Normalize the token using NFKC (this handles full-width characters)
    token = unicodedata.normalize('NFKD', token)
    # Remove all combining diacritical marks
    token = ''.join(c for c in token if not unicodedata.combining(c))
    # remove punctuation
    token = token.translate(str.maketrans('', '', string.punctuation))
    # Strip leading and trailing whitespace
    token = token.strip()
    # Convert to lowercase
    token = token.lower()
    # Normalize internal whitespace (replace multiple spaces with single space)
    token = re.sub(r'\s+', ' ', token)
    # remove internal whitespace
    token = token.replace(" ", "")
    # remove any internal tabs
    token = token.replace("\t", "")
    # remove any internal newlines
    token = token.replace("\n", "")
    return token


def clean_generic_name(token: str) -> str:
    # First check if input is valid
    if not isinstance(token, str):
        return ""
    # Normalize the token using NFKC (this handles full-width characters)
    token = unicodedata.normalize('NFKD', token)
    # Remove all combining diacritical marks
    token = ''.join(c for c in token if not unicodedata.combining(c))
    # remove punctuation
    token = token.translate(str.maketrans('', '', string.punctuation))
    # Strip leading and trailing whitespace
    token = token.strip()
    # Normalize internal whitespace (replace multiple spaces with single space)
    token = re.sub(r'\s+', ' ', token)
    # Convert to lowercase before handling trailing tokens
    token = token.lower()
    # Define trailing substrings to remove (in lowercase since we converted the token)
    trailing_tokens = [" y po", " po", " iv", " im", " subq"]
    # Remove any trailing substring if present
    for trailing in trailing_tokens:
        if token.endswith(trailing):
            token = token[:-len(trailing)]
            token = token.strip()  # Strip again if any extra spaces remain
    # remove any trailing spaces
    token = token.strip()
    # remove internal whitespace
    token = token.replace(" ", "")
    # remove any internal tabs
    token = token.replace("\t", "")
    # remove any internal newlines
    token = token.replace("\n", "")
    return token


# def get_drug_data(drug_ref_file="data/reference/ProstateDrugList.csv", 
#                  output_json="data/reference/drug_data.json") -> dict:
#     """
#     From ProstateDrugList.csv, get:
#     - drug names: cleaned brand and generic names
#     - brand2generic: map of cleaned brand name to generic name
#     - brand2color: map of cleaned brand name to drug type color (yellow, green, empty string)

#     Params
#         drug_ref_file (str): File path to the Excel file with drug names
#         output_json (str): File path to save the processed drug data as JSON
#     Returns
#         drug_data (dict): {"cleaned_brand_name": {"generic": ..., "drug_type_color": ...}}
#     """
#     # Load CSV file into pandas
#     df = pd.read_csv(drug_ref_file)
    
#     # Initialize drug data dictionary
#     drug_data = {}
    
#     # Get brand name columns (those that start with "Brand_name")
#     brand_cols = [col for col in df.columns if col.startswith('Brand_name')]
    
#     # Process each row
#     for _, row in df.iterrows():
#         # Process each brand name column
#         for brand_col in brand_cols:
#             if pd.notna(row[brand_col]):
#                 # Clean brand name
#                 brand_name = clean_brand_name(row[brand_col])
                
#                 # Clean generic name
#                 generic_name = clean_generic_name(row['Generic_name'])
                
#                 # Get drug type color (empty string if column missing or NaN)
#                 drug_type_color = ""
#                 if 'Yellow_Green' in df.columns and pd.notna(row['Yellow_Green']):
#                     drug_type_color = row['Yellow_Green']
                
#                 # Add to dictionary
#                 drug_data[brand_name] = {
#                     "generic": generic_name,
#                     "drug_type_color": drug_type_color
#                 }
    
#     # Create output directory if it doesn't exist
#     os.makedirs(os.path.dirname(output_json), exist_ok=True)
    
#     # Save to JSON file
#     with open(output_json, 'w') as f:
#         json.dump(drug_data, f, indent=2)
    
#     return drug_data


# def count_unique_brand_names(drug_ref_file="data/reference/ProstateDrugList.csv") -> int:
#     """
#     Count unique brand names from ProstateDrugList.csv.
#     A brand name is any non-empty string in a column that starts with 'Brand_name'.
    
#     Returns:
#         int: Number of unique brand names after cleaning
#     """
#     # Load CSV file into pandas
#     df = pd.read_csv(drug_ref_file)
    
#     # Get brand name columns
#     brand_cols = [col for col in df.columns if col.startswith('Brand_name')]
    
#     # Collect all non-empty brand names
#     unique_brands = set()
#     for col in brand_cols:
#         # Get non-null values from this column
#         brands = df[col].dropna().tolist()
#         # Add to set (automatically handles duplicates)
#         unique_brands.update(brands)
    
#     return len(unique_brands)


# def load_drug_data(path="data/reference/drug_data.json") -> dict:
#     try:
#         with open(path) as f:
#             drug_data_dict = json.load(f)
#     except FileNotFoundError:
#         print("Warning: drug_data.json not found. Run get_drug_data() to generate it.")
#         drug_data_dict = {}
#     return drug_data_dict

# def validate_drug_data(drug_data_dict: dict) -> dict:
#     """
#     Verify that DRUG_DATA length matches unique brand names in CSV
#     """
#     try:
#         csv_brand_count = count_unique_brand_names()
#         if len(drug_data_dict) != csv_brand_count:
#             print(f"Warning: DRUG_DATA length ({len(drug_data_dict)}) does not match unique brand names in CSV ({csv_brand_count})")
#     except FileNotFoundError:
#         print("Warning: drug_data.json not found. Run get_drug_data() to generate it.")
#         drug_data_dict = {}
#     print(len(drug_data_dict))


# def timer(func):
#     """Decorator to time function execution"""
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         start_time = time.time()
#         result = func(*args, **kwargs)
#         end_time = time.time()
#         duration = end_time - start_time
#         logger.info(f"Total execution time for {func.__name__}: {duration:.2f} seconds")
#         return result
#     return wrapper


def concatenate_chunks(chunks_dir, fileout):
    """
    Concatenate all chunks vertically into a single CSV
    """
    # Get all chunk files
    chunks = os.listdir(chunks_dir)

    # If no chunks exist, create empty output file with headers
    if not chunks:
        pd.DataFrame().to_csv(fileout, index=False)
        return

    rows_per_chunk = 0
    # Write first chunk with header
    logger.info(f"Processing file {os.path.join(chunks_dir, chunks[0])}")
    df = pd.read_csv(os.path.join(chunks_dir, chunks[0]), encoding='latin-1')
    rows_per_chunk = len(df)
    df.to_csv(fileout, index=False)
    
    # Append all other chunks without headers
    for idx, chunk in enumerate(chunks[1:]):
        logger.info(f"Processing file {os.path.join(chunks_dir, chunks[idx+1])}")
        df = pd.read_csv(os.path.join(chunks_dir, chunk), encoding='latin-1')
        rows_per_chunk += len(df)
        df.to_csv(fileout, mode='a', header=False, index=False)
    logger.info(f"Finished concatenating {rows_per_chunk} rows")
    assert rows_per_chunk == len(pd.read_csv(fileout, encoding='latin-1'))


def find_matches(chunk, drug_cols, ref_drug_names):
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
    
