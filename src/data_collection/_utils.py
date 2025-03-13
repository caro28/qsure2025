import os
import logging
from datetime import datetime
import pandas as pd


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


def save_chunk_to_parquet(chunk: list, chunk_num: int, output_dir: str, file_prefix: str):
    """
    Save a chunk of data to a numbered parquet file.
    Args:
        chunk: List of dictionaries containing the chunk data
        chunk_num: Chunk number for filename
        output_dir: Directory to save the file
        file_prefix: Prefix for the filename (e.g., 'general_payments_2023')
    Returns:
        str: Path to saved file
    """
    df = pd.DataFrame(chunk)
    filename = f"{file_prefix}_chunk_{chunk_num:03d}.parquet"
    filepath = os.path.join(output_dir, filename)
    df.to_parquet(filepath, compression='snappy')
    return filepath


