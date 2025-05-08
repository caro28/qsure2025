import string
import re
import os
import unicodedata
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

setup_logging()
logger = logging.getLogger(__name__)



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


