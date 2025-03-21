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


def clean_brand_name(token: str) -> str:
    if not isinstance(token, str):
        return ""
    # Normalize the token using NFKC
    token = unicodedata.normalize('NFKC', token)
    # Replace non-ASCII (non-Roman) characters using regex (keep only Roman letters and spaces)
    # This regular expression will allow only letters a-z, A-Z, and spaces
    token = re.sub(r'[^a-zA-Z ]', '', token)
    # Strip leading and trailing whitespace
    token = token.strip()
    # Convert to lowercase
    token = token.lower()
    return token


def clean_generic_name(token: str) -> str:
    """Apply the cleaning from clean_brand_name and then remove trailing PO, IV, IM, SUBQ"""
    token = clean_brand_name(token)

    # Define trailing substrings to remove (already lowercased)
    trailing_tokens = [" po", " iv", " im", " subq"]
    # Remove any trailing substring if present
    for trailing in trailing_tokens:
        if token.endswith(trailing):
            token = token[:-len(trailing)]
            token = token.strip()  # Strip again if any extra spaces remain
            
    return token


def get_drug_data(drug_ref_file="data/reference/ProstateDrugList.csv", 
                 output_json="data/reference/drug_data.json") -> dict:
    """
    From ProstateDrugList.csv, get:
    - drug names: cleaned brand and generic names
    - brand2generic: map of cleaned brand name to generic name
    - brand2color: map of cleaned brand name to drug type color (yellow, green, empty string)

    Params
        drug_ref_file (str): File path to the Excel file with drug names
        output_json (str): File path to save the processed drug data as JSON
    Returns
        drug_data (dict): {"cleaned_brand_name": {"generic": ..., "drug_type_color": ...}}
    """
    # Load CSV file into pandas
    df = pd.read_csv(drug_ref_file)
    
    # Initialize drug data dictionary
    drug_data = {}
    
    # Get brand name columns (those that start with "Brand_name")
    brand_cols = [col for col in df.columns if col.startswith('Brand_name')]
    
    # Process each row
    for _, row in df.iterrows():
        # Process each brand name column
        for brand_col in brand_cols:
            if pd.notna(row[brand_col]):
                # Clean brand name
                brand_name = clean_brand_name(row[brand_col])
                
                # Clean generic name
                generic_name = clean_generic_name(row['Generic_name'])
                
                # Get drug type color (empty string if column missing or NaN)
                drug_type_color = ""
                if 'Yellow_Green' in df.columns and pd.notna(row['Yellow_Green']):
                    drug_type_color = row['Yellow_Green']
                
                # Add to dictionary
                drug_data[brand_name] = {
                    "generic": generic_name,
                    "drug_type_color": drug_type_color
                }
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_json), exist_ok=True)
    
    # Save to JSON file
    with open(output_json, 'w') as f:
        json.dump(drug_data, f, indent=2)
    
    return drug_data


def count_unique_brand_names(drug_ref_file="data/reference/ProstateDrugList.csv") -> int:
    """
    Count unique brand names from ProstateDrugList.csv.
    A brand name is any non-empty string in a column that starts with 'Brand_name'.
    
    Returns:
        int: Number of unique brand names after cleaning
    """
    # Load CSV file into pandas
    df = pd.read_csv(drug_ref_file)
    
    # Get brand name columns
    brand_cols = [col for col in df.columns if col.startswith('Brand_name')]
    
    # Collect all non-empty brand names
    unique_brands = set()
    for col in brand_cols:
        # Get non-null values from this column
        brands = df[col].dropna().tolist()
        # Add to set (automatically handles duplicates)
        unique_brands.update(brands)
    
    return len(unique_brands)


def load_drug_data(path="data/reference/drug_data.json") -> dict:
    try:
        with open(path) as f:
            drug_data_dict = json.load(f)
    except FileNotFoundError:
        print("Warning: drug_data.json not found. Run get_drug_data() to generate it.")
        drug_data_dict = {}
    return drug_data_dict

def validate_drug_data(drug_data_dict: dict) -> dict:
    """
    Verify that DRUG_DATA length matches unique brand names in CSV
    """
    try:
        csv_brand_count = count_unique_brand_names()
        if len(drug_data_dict) != csv_brand_count:
            print(f"Warning: DRUG_DATA length ({len(drug_data_dict)}) does not match unique brand names in CSV ({csv_brand_count})")
    except FileNotFoundError:
        print("Warning: drug_data.json not found. Run get_drug_data() to generate it.")
        drug_data_dict = {}
    print(len(drug_data_dict))


def timer(func):
    """Decorator to time function execution"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Total execution time for {func.__name__}: {duration:.2f} seconds")
        return result
    return wrapper


def get_drug_columns(df: pd.DataFrame) -> List[str]:
    """Get columns that contain drug names, case-insensitive"""
    prefix = "name_of_drug_or_biological_or_device_or_medical_supply_"
    return [col for col in df.columns if col.lower().startswith(prefix.lower())]

