import json
import logging
import time
import os
import pandas as pd
from src.data_collection._op_api_utils import _sql_query_by_col, _discover_drug_columns
from src.data_collection._utils import setup_logging, save_chunk_to_parquet

setup_logging()
log = logging.getLogger(__name__)

def load_datastore_uuids():
    with open("data/reference/all_datastore_uuids.json", "r") as f:
        return json.load(f)

DATASTORE_UUIDS = load_datastore_uuids()

def get_last_chunk_info(output_dir: str, file_prefix: str):
    """
    Get information about the last saved chunk in the directory.
    
    Returns:
        tuple: (last_chunk_num, last_offset, is_chunk_complete)
    """
    # Get all chunk files
    chunk_files = sorted([f for f in os.listdir(output_dir) if f.startswith(file_prefix) and f.endswith('.parquet')])
    
    if not chunk_files:
        return 0, 0, True
    
    last_file = chunk_files[-1]
    # Extract chunk number from filename (e.g., general_payments_2023_chunk_005.parquet -> 5)
    last_chunk_num = int(last_file.split('_chunk_')[1].split('.')[0])
    
    # Read the last chunk and check if it's complete
    df = pd.read_parquet(os.path.join(output_dir, last_file))
    is_chunk_complete = len(df) == 100_000  # True if chunk is full
    
    # Calculate the offset for the next batch
    total_records = (last_chunk_num - 1) * 100_000 + len(df)
    next_offset = total_records
    
    return last_chunk_num, next_offset, is_chunk_complete

def validate_chunk(df: pd.DataFrame, chunk_num: int):
    """
    Validate a chunk of data and log results.
    
    Args:
        df: DataFrame containing chunk data
        chunk_num: Chunk number for logging
    """
    # Log first 10 rows of first chunk only
    if chunk_num == 1:
        log.info("\nFirst 10 rows of first chunk:")
        log.info("\n" + str(df.head(10)))
    
    # Check for unique record_ids
    chunk_total = len(df)
    unique_records = df['record_id'].nunique()
    log.info(f"\nChunk {chunk_num} validation:")
    log.info(f"Records in chunk: {chunk_total}")
    log.info(f"Unique record_ids in chunk: {unique_records}")
    if chunk_total == unique_records:
        log.info(f"✓ All record_ids in chunk {chunk_num} are unique")
    else:
        log.warning(f"! Found {chunk_total - unique_records} duplicate record_ids in chunk {chunk_num}")

def save_and_validate_chunk(current_chunk: list, chunk_num: int, output_dir: str, file_prefix: str) -> str:
    """
    Save chunk to parquet file and validate its contents.
    
    Args:
        current_chunk: List of records to save
        chunk_num: Current chunk number
        output_dir: Directory to save the chunk
        file_prefix: Prefix for the filename
    
    Returns:
        str: Path to saved file
    """
    log.info(f"Saving chunk {chunk_num} with {len(current_chunk)} records")
    filepath = save_chunk_to_parquet(current_chunk, chunk_num, output_dir, file_prefix)
    
    # Load and validate the saved chunk
    df = pd.read_parquet(filepath)
    validate_chunk(df, chunk_num)
    
    return filepath

def initialize_download_session(dataset: str, file_prefix: str):
    """
    Initialize a new download session or resume an existing one.
    
    Args:
        dataset: Dataset name
        file_prefix: Prefix for files
    
    Returns:
        tuple: (output_dir, start_offset, start_chunk_num, uuid, columns_to_fetch)
    """
    uuid = DATASTORE_UUIDS[dataset]
    
    # Discover columns to fetch
    base_cols = ["record_id"]
    drug_cols = _discover_drug_columns(uuid)
    cols_to_fetch = ",".join(base_cols + drug_cols)
    
    # Setup output directory
    output_dir = f"data/reference/{file_prefix}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Check for existing chunks and get last state
    last_chunk_num, offset, is_last_chunk_complete = get_last_chunk_info(output_dir, file_prefix)
    
    if last_chunk_num > 0:
        log.info(f"Found existing chunks. Last chunk: {last_chunk_num}")
        log.info(f"Last chunk is {'complete' if is_last_chunk_complete else 'incomplete'}")
        log.info(f"Will resume from offset: {offset}")
        
        if not is_last_chunk_complete:
            log.info(f"Last chunk was incomplete. Will re-download chunk {last_chunk_num}")
            # Remove the incomplete chunk
            last_chunk_file = f"{file_prefix}_chunk_{last_chunk_num:03d}.parquet"
            os.remove(os.path.join(output_dir, last_chunk_file))
            # Adjust offset to re-download this chunk
            offset = (last_chunk_num - 1) * 100_000
            chunk_num = last_chunk_num
        else:
            chunk_num = last_chunk_num + 1
    else:
        offset = 0
        chunk_num = 1
    
    return output_dir, offset, chunk_num, uuid, cols_to_fetch

def get_data_slice(dataset: str, file_prefix: str):
    """
    Fetch record_ids and drug names from the dataset.
    
    Args:
        dataset: key from DATASTORE_UUIDS, e.g. "2023 General Payment Data"
        file_prefix: Prefix for output files and directory (e.g., 'general_payments_2023')
    Returns:
        str: Path to directory containing parquet files
    """
    start_time = time.time()
    
    # Initialize or resume session
    output_dir, offset, chunk_num, uuid, cols_to_fetch = initialize_download_session(dataset, file_prefix)
    
    batch_size = 500  # API limit
    total_records = offset  # Start counting from where we left off
    current_chunk = []
    records_per_file = 100_000
    saved_files = []

    while True:
        try:
            log.info(f"Fetching batch with offset {offset}")
            time.sleep(0.5)  # 500ms delay to avoid overwhelming the API

            batch = _sql_query_by_col(
                cols=cols_to_fetch,
                datastore_uuid=uuid,
                LIMIT=batch_size,
                OFFSET=offset,
            )
            
            # If no data returned, we've reached the end
            if not batch or not batch[0]:
                # Save any remaining records in the last chunk
                if current_chunk:
                    filepath = save_and_validate_chunk(current_chunk, chunk_num, output_dir, file_prefix)
                    saved_files.append(filepath)
                log.info("No more data available, ending fetch")
                break

            # Add batch to current chunk
            current_chunk.extend(batch)
            total_records += len(batch)

            # If current chunk is big enough, save it
            if len(current_chunk) >= records_per_file:
                filepath = save_and_validate_chunk(current_chunk, chunk_num, output_dir, file_prefix)
                saved_files.append(filepath)
                current_chunk = []
                chunk_num += 1
            
            offset += batch_size
            
        except Exception as e:
            log.error(f"Error fetching batch at offset {offset}: {str(e)}")
            # Save any records we have so far in case of error
            if current_chunk:
                filepath = save_and_validate_chunk(current_chunk, chunk_num, output_dir, file_prefix)
                saved_files.append(filepath)
            break
    
    # Log summary statistics
    total_time = time.time() - start_time
    log.info(f"Total records retrieved in this run: {total_records - offset}")
    log.info(f"Total records overall: {total_records}")
    log.info(f"Total chunks saved: {chunk_num - 1}")
    log.info(f"Total execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    log.info(f"Data saved to directory: {output_dir}")
    log.info("Files saved in this run:")
    for filepath in saved_files:
        log.info(f"  {filepath}")
    
    return output_dir

def main():
    file_prefix = "general_payments_2023"
    output_dir = get_data_slice("2023 General Payment Data", file_prefix)
    
    # Example of loading and checking the first chunk:
    first_chunk_path = os.path.join(output_dir, f"{file_prefix}_chunk_001.parquet")
    if os.path.exists(first_chunk_path):
        # Set pandas display options to show all data
        pd.set_option('display.max_columns', None)  # Show all columns
        pd.set_option('display.width', None)        # Don't wrap wide tables
        pd.set_option('display.max_rows', 5)        # Show 5 rows
        pd.set_option('display.max_colwidth', None) # Show full content of each cell
        
        df = pd.read_parquet(first_chunk_path)
        log.info(f"\nLoaded chunk 1 with {len(df):,} records")
        log.info("\nColumns in dataset:")
        for i, col in enumerate(df.columns, 1):
            log.info(f"{i}. {col}")
        log.info("\nFirst 5 rows with all columns:")
        log.info("\n" + str(df.head()))

if __name__ == "__main__":
    main()