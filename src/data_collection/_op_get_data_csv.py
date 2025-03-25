import json
import os
import logging
import unicodedata
from typing import List, Set, Tuple, Dict
from Levenshtein import distance
import numpy as np
import time

import pandas as pd

from src.data_collection._utils import (
    setup_logging, 
    clean_brand_name,
    timer,
    get_drug_columns,
)

setup_logging()
logger = logging.getLogger(__name__)



def log_distances(filtered_chunk: pd.DataFrame) -> None:
    """
    Log the most dissimilar matches found
    Args:
        filtered_chunk: df containing 'distances' column and 'drug_col_int' column
    """            
    # Filter for non-exact matches (distance > 0)
    non_exact_matches = filtered_chunk[filtered_chunk['distances'] > 0]
    
    if len(non_exact_matches) == 0:
        logger.info("No non-exact matches found")
        return
        
    # Sort all non-exact matches by distance
    sorted_rows = non_exact_matches.sort_values('distances', ascending=False)
    
    logger.warning(f"\nAll non-exact matches ({len(sorted_rows)} total), sorted by distance:")
    for idx, row in sorted_rows.iterrows():
        logger.warning(
            f"Row {idx}, Distance {row['distances']}, OP Drug: {row['matched_drug_name']}, Source: {row['mask_source']}"
        )

def find_matches(values: pd.Series, search_terms: Set[str], found_terms: Set[str]) -> Tuple[pd.Series, Dict]:
    """
    Find matches between values and search terms, tracking distances for non-exact matches
    
    Args:
        values: Series of cleaned values to search in
        search_terms: Set of terms to search for
        found_terms: Set to track which terms were found (modified in place)
    
    Returns:
        Tuple of (mask of matching rows, dictionary of distance calculations for non-exact matches)
    """
    mask = pd.Series(False, index=values.index)
    distances = {}
    
    for term in search_terms:
        if term == '':  # Skip empty search terms
            continue
            
        # Find matches where value contains term
        matches = values.str.contains(term, regex=False, na=False)
        # Remove empty values from matches
        matches = matches & (values != '')
        
        if matches.any():
            found_terms.add(term)
            mask = mask | matches
            
            # Calculate distances for matches
            for idx in values[matches].index:
                value = values[idx]
                if value.lower() == term.lower():
                    distances[idx] = 0
                else:
                    dist = distance(value, term)
                    distances[idx] = dist
    
    return mask, distances


def process_chunk(chunk: pd.DataFrame, op_drug_cols: List[str], drug_data: dict, found_drugs: Set[str]) -> Tuple[pd.DataFrame, int, int]:
    """
    Process a single chunk of data
    
    Args:
        chunk: DataFrame chunk to process
        drug_cols: List of columns containing drug names
        drug_data: Dictionary of drug data to match against
        found_drugs: Set to track which drug names were found (modified in place)
    
    Returns:
        Tuple of (filtered_chunk, total_rows, matched_rows)
    """
    tgt_drug_names = set(drug_data.keys())
    chunk_mask = pd.Series(False, index=chunk.index)
    
    # Initialize columns
    chunk['distances'] = np.nan
    chunk['mask_source'] = ''
    chunk['matched_drug_name'] = ''  # OpenPayments drug name
    chunk['target_drug_name'] = ''   # Drug name from our target list
    
    # For each drug column
    for col_idx, col in enumerate(op_drug_cols):
        # Clean values
        op_drug_names = chunk[col].fillna('').astype(str).apply(clean_brand_name)
        
        # First direction: does value contain any drug name?
        mask1, distances1 = find_matches(op_drug_names, tgt_drug_names, found_drugs)
        
        # Second direction: does any drug name contain the value?
        tgt_series = pd.Series(list(tgt_drug_names))
        mask2, distances2 = find_matches(tgt_series, set(op_drug_names), found_drugs)

        # Combine masks and distances
        current_mask = mask1 | mask2
        chunk_mask = chunk_mask | current_mask
        distances1.update(distances2)  # Merge the two dictionaries
        
        # Only set values for matching rows in this column
        if current_mask.any():
            # Get matching indices
            matching_idx = chunk.index[current_mask]
            # Set values only for matching rows
            for idx in matching_idx:
                if idx in distances1:
                    chunk.loc[idx, 'distances'] = distances1[idx]
                    # Store both the OpenPayments and target drug names
                    if mask1[idx]:
                        chunk.loc[idx, 'matched_drug_name'] = chunk.loc[idx, col]
                        # Find which target drug name matched
                        for term in tgt_drug_names:
                            if term in op_drug_names[idx].lower():
                                chunk.loc[idx, 'target_drug_name'] = term
                                break
                    else:  # mask2 match
                        chunk.loc[idx, 'matched_drug_name'] = chunk.loc[idx, col]
                        # Find which target drug name contained this value
                        op_value = op_drug_names[idx]
                        for term in tgt_drug_names:
                            if op_value in term.lower():
                                chunk.loc[idx, 'target_drug_name'] = term
                                break
                    # Set mask source
                    if mask1[idx]:
                        chunk.loc[idx, 'mask_source'] = 'mask1'
                    else:
                        chunk.loc[idx, 'mask_source'] = 'mask2'
    
    # Filter chunk using mask
    filtered_chunk = chunk[chunk_mask]

    # Log distance information for non-exact matches
    log_distances(filtered_chunk)
    
    return filtered_chunk, len(chunk), len(filtered_chunk)


def save_results(result: pd.DataFrame, pathout: str, total_rows: int, matched_rows: int):
    """Save results and log summary statistics"""
    logger.info(f"Total rows processed: {total_rows}")
    logger.info(f"Total matching rows found: {matched_rows} ({matched_rows/total_rows*100:.2f}%)")
    
    # Save to parquet
    result.to_parquet(pathout)
    logger.info(f"Saved filtered data to {pathout}")


def log_unused_drugs(drug_data_keys: Set[str], found_drugs: Set[str]):
    """Log statistics about unused drug names"""
    unused_drugs = drug_data_keys - found_drugs
    if unused_drugs:
        logger.warning(f"Found {len(unused_drugs)} drug names in drug_data that were not found in the CSV:")
        for drug in sorted(unused_drugs):
            logger.warning(f"  - {drug}")
    else:
        logger.info("All drug names in drug_data were found in the CSV")


def cleanup_temp_files(chunk_files: List[str]):
    """Clean up temporary chunk files after processing
    
    Args:
        chunk_files: List of temporary parquet file paths to remove
    """
    for f in chunk_files:
        try:
            os.remove(f)
            logger.debug(f"Removed temporary file {f}")
        except OSError as e:
            logger.warning(f"Error removing temporary file {f}: {e}")


def save_filtered_chunk(filtered_chunk: pd.DataFrame, pathout: str, chunk_num: int, chunk_matches: int) -> str:
    """Save a filtered chunk to a temporary parquet file
    
    Args:
        filtered_chunk: DataFrame chunk to save
        pathout: Base path for output file
        chunk_num: Chunk number (for filename)
        chunk_matches: Number of matching rows in chunk (for logging)
    
    Returns:
        str: Path to saved chunk file
    """
    chunk_file = f"{pathout}.chunk_{chunk_num:03d}.parquet"
    
    # Convert zip code column to string if it exists
    if 'Recipient_Zip_Code' in filtered_chunk.columns:
        filtered_chunk['Recipient_Zip_Code'] = filtered_chunk['Recipient_Zip_Code'].astype(str)
    
    filtered_chunk.to_parquet(chunk_file)
    logger.info(f"Found {chunk_matches} matching rows in chunk {chunk_num}, saved to {chunk_file}")
    return chunk_file


@timer
def get_data_slice(
        payments_csv_path: str,
        pathout: str,
        drug_data: dict,
) -> pd.DataFrame:
    """
    Load csv in chunks and only keep rows with drug names that are in drug_data
    
    Parameters:
        payments_csv_path (str): Path to large csv that must be loaded in chunks
        pathout (str): Path to save the filtered data
        drug_data (dict): {"cleaned_brand_name": {"generic": ..., "drug_type_color": ...}}
    
    Returns:
        pd.DataFrame: Combined filtered data
    """
    start_time = time.time()
    logger.info(f"Starting to process {payments_csv_path}")

    # get drug columns
    drug_cols = get_drug_columns(pd.read_csv(payments_csv_path, nrows=1))
    logger.info(f"Drug columns: {drug_cols}")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(pathout), exist_ok=True)
    
    # Initialize tracking variables
    total_rows = 0
    matched_rows = 0
    found_drugs = set()
    chunk_files = []
    chunk_times = []
    
    # Load csv in chunks
    chunksize = 100000
    chunks = pd.read_csv(payments_csv_path, chunksize=chunksize)
    
    # Process each chunk
    for i, chunk in enumerate(chunks):
        chunk_start_time = time.time()
        chunk_start = i * chunksize
        logger.info(f"Processing chunk {i+1}, rows {chunk_start} to {chunk_start + len(chunk)}")
        
        # Process chunk
        filtered_chunk, chunk_total, chunk_matches = process_chunk(chunk, drug_cols, drug_data, found_drugs)
        
        # Update counts
        total_rows += chunk_total
        matched_rows += chunk_matches
        
        # If we found any matching rows, save them to a temporary parquet file
        if chunk_matches > 0:
            chunk_file = save_filtered_chunk(filtered_chunk, pathout, i+1, chunk_matches)
            chunk_files.append(chunk_file)
        
        # Log chunk timing
        chunk_duration = time.time() - chunk_start_time
        chunk_times.append(chunk_duration)
        logger.info(f"Chunk {i+1} processed in {chunk_duration:.2f} seconds")

        break
    
    # # Log unused drug names (updated when calling find_matches)
    # log_unused_drugs(set(drug_data.keys()), found_drugs)
    
    # Combine all chunks if we found any matches
    if chunk_files:
        combine_start_time = time.time()
        logger.info(f"Combining {len(chunk_files)} chunk files...")
        # Read and combine all chunks
        chunks_to_combine = [pd.read_parquet(f) for f in chunk_files]
        result = pd.concat(chunks_to_combine, ignore_index=True)
        
        # Save final results and log statistics
        save_results(result, pathout, total_rows, matched_rows)
        
        # Clean up temporary files
        cleanup_temp_files(chunk_files)
        
        combine_duration = time.time() - combine_start_time
        logger.info(f"Chunk combination completed in {combine_duration:.2f} seconds")
    else:
        logger.warning("No matching rows found in any chunk")
        result = pd.DataFrame()
    
    # Log timing statistics
    end_time = time.time()
    total_duration = end_time - start_time
    avg_chunk_time = sum(chunk_times) / len(chunk_times) if chunk_times else 0
    
    logger.info(f"\nTiming Statistics:")
    logger.info(f"Total execution time: {total_duration:.2f} seconds")
    logger.info(f"Number of chunks processed: {len(chunk_times)}")
    logger.info(f"Average time per chunk: {avg_chunk_time:.2f} seconds")
    logger.info(f"Fastest chunk: {min(chunk_times):.2f} seconds")
    logger.info(f"Slowest chunk: {max(chunk_times):.2f} seconds")
    
    return result

        
