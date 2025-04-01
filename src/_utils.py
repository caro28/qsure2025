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


def concatenate_chunks(chunks_dir, fileout):
    """
    Concatenate all chunks vertically into a single CSV
    """
    # Get all chunk files
    chunks = os.listdir(chunks_dir)

    # If no chunks exist, raise error
    if not chunks:
        raise FileNotFoundError(f"No chunks found in {chunks_dir}")

    rows_per_chunk = 0
    # Write first chunk with header
    logger.info(f"Processing file {os.path.join(chunks_dir, chunks[0])}")
    df = pd.read_csv(os.path.join(chunks_dir, chunks[0]), encoding='latin-1', dtype=str)
    rows_per_chunk = len(df)
    df.to_csv(fileout, index=False)
    
    # Append all other chunks without headers
    for idx, chunk in enumerate(chunks[1:]):
        logger.info(f"Processing file {os.path.join(chunks_dir, chunks[idx+1])}")
        df = pd.read_csv(os.path.join(chunks_dir, chunk), encoding='latin-1', dtype=str)
        rows_per_chunk += len(df)
        df.astype(str).to_csv(fileout, mode='a', header=False, index=False)
    logger.info("Finished concatenating %s rows", rows_per_chunk)
    assert rows_per_chunk == len(pd.read_csv(fileout, encoding='latin-1', dtype=str))


