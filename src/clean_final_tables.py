import pandas as pd
import os
import json
import logging
from src._utils import (
    setup_logging,
    clean_brand_name,
    clean_generic_name,
)

setup_logging()
logger = logging.getLogger(__name__)    


def build_map_year2cols(dataset_type):
    """
    Build a map of year to columns
    """
    year2cols = {}
    grace_cols = pd.read_csv(f"data/reference/col_names/{dataset_type}_payments/grace_cols.csv")
    years = grace_cols.columns
    for year in years:
        year2cols[year] = grace_cols[year].dropna().to_list()
    return year2cols

def build_ref_data_maps():
    # load ProstateDrugList.csv
    ref_df = pd.read_csv("data/reference/ProstateDrugList.csv")
    brand2generic = {}
    brand2color = {}
    # keys: values in column 'Generic_name', values: value in Brand_name1, Brand_name2, Brand_name3, Brand_name4 for that row
    for idx, row in ref_df.iterrows():
        generic_name = row['Generic_name']
        generic_name = clean_generic_name(generic_name)
        brand_names = [row['Brand_name1'], row['Brand_name2'], row['Brand_name3'], row['Brand_name4']]
        brand_names = [name for name in brand_names if name is not None and name != 'nan']
        # remove empty strings
        brand_names = [name for name in brand_names if name != '']
        # clean brand names
        brand_names = [clean_brand_name(name) for name in brand_names]

        # keys: brand names, value: generic name
        for brand_name in brand_names:
            brand2generic[brand_name] = generic_name
            brand2color[brand_name] = row['Color']

        # add generic names to both maps
        brand2generic[generic_name] = generic_name
        brand2color[generic_name] = row['Color']

    return brand2generic, brand2color

def harmonize_col_names(df, year, dataset_type):
    """
    Harmonize column names using a map of year to columns from grace_cols.csv
    """
    # Get map of year2cols
    year2cols = build_map_year2cols(dataset_type)
    df.columns = year2cols[str(year)]
    return df

def get_harmonized_drug_cols(df):
    prefix = "Drug_Biological_Device_Med_Sup_"
    return [col for col in df.columns if col.lower().startswith(prefix.lower())]


def get_prostate_drug_type(drug_name, brand2color):
    """
    Determine if a drug is a prostate drug type based on its color coding
    
    Args:
        drug_name (str): The name of the drug
        brand2color (dict): Mapping of drug names to their color codes
        
    Returns:
        int: 1 if the drug is a prostate drug (yellow), 0 otherwise
    """
    return 1 if brand2color[drug_name] == 'yellow' else 0


def is_onc_prescriber(prostate_drug_type, recipient_npi, npi_set):
    """
    Determine if a recipient is an oncology prescriber based on drug type and NPI
    
    Args:
        prostate_drug_type (int): Whether the drug is a prostate drug (1) or not (0)
        recipient_npi: The NPI of the covered recipient
        npi_set: Set of NPIs that are considered oncology prescribers
        
    Returns:
        int: 1 if the recipient is an oncology prescriber for prostate drugs, 0 otherwise
    """
    return 1 if prostate_drug_type == 1 and recipient_npi in npi_set else 0


def add_new_columns(df, drug_cols, npi_set):
    brand2generic, brand2color = build_ref_data_maps()

    for idx, row in df.iterrows():
        # iterate through drug_cols
        for col in drug_cols:
            drug_name = str(row[col])
            if pd.isna(drug_name) or drug_name == 'nan':
                continue
            elif drug_name == '':
                continue

            drug_name = clean_brand_name(drug_name)
            
            # skip drug names not in our target set
            try:
                generic_name = brand2generic[drug_name]
            except KeyError:
                continue

            # add Drug_Name
            df.at[idx, 'Drug_Name'] = generic_name

            # add Prostate_Drug_Type
            df.at[idx, 'Prostate_Drug_Type'] = get_prostate_drug_type(drug_name, brand2color)

            # add Onc_Prescriber col:
            result = is_onc_prescriber(df.at[idx, 'Prostate_Drug_Type'], df.at[idx, 'Covered_Recipient_NPI'], npi_set)
            df.at[idx, 'Onc_Prescriber'] = result

    return df

def clean_op_data_gnrl(filepath, fileout, year, npi_set, dataset_type):
    """
    Clean and enhance Open Payments data
    Harmonize column names 
    Add columns
        Prostate_drug_type (0/1 based on Color)
        Drug_Name (generic name)
        Onc_Prescriber (1 if Prostate_drug_type == 1 AND Covered_Recipient_NPI is in npi_set)
    """
    df = pd.read_csv(filepath, dtype=str)

    # 1. Clean df: drop rows where Covered_Recipient_NPI is nan
    npi_missing = df[df['Covered_Recipient_NPI'].isna()]
    # save dropped rows to csv
    filename = fileout.split("/")[-1]
    npi_missing.to_csv(f"data/final_files/general_payments/missing_npis/{filename}", index=False)

    # Drop nan NPI rows from the original DataFrame
    df = df[df['Covered_Recipient_NPI'].notna()]
    df['Covered_Recipient_NPI'] = df['Covered_Recipient_NPI'].astype(float).astype(int).astype(str)

    # 2. Harmonize column names
    df = harmonize_col_names(df, year, dataset_type)

    # 3. Add Columns: Drug_Name, Prostate_Drug_Type, Onc_Prescriber
    drug_cols = get_harmonized_drug_cols(df)
    logger.info("Adding new columns to %s", fileout)
    df = add_new_columns(df, drug_cols, npi_set)
    assert 'Drug_Name' in df.columns
    assert 'Prostate_Drug_Type' in df.columns
    assert 'Onc_Prescriber' in df.columns

    # Remove decimals from cols
    df['Prostate_Drug_Type'] = df['Prostate_Drug_Type'].astype(float).astype(int).astype(str)
    df['Onc_Prescriber'] = df['Onc_Prescriber'].astype(float).astype(int).astype(str)
    df['Covered_Recipient_Profile_ID'] = df['Covered_Recipient_Profile_ID'].astype(float).astype(int).astype(str)

    # fill all nan with ''
    df.fillna('', inplace=True)
    
    # 4. Save to CSV (save all cols as string)
    df.astype(str).to_csv(fileout, index=False)


def clean_op_data_rsrch(filepath, fileout, year, npi_set, dataset_type):
    """
    Clean and enhance Open Payments data
    Harmonize column names 
    Add columns
        Prostate_drug_type (0/1 based on Color)
        Drug_Name (generic name)
        Onc_Prescriber (1 if Prostate_drug_type == 1 AND Covered_Recipient_NPI is in npi_set)
    """
    df = pd.read_csv(filepath, dtype=str)

    # 1. Clean df: drop rows where NPI val is nan in all NPI cols
    npi_cols = df.filter(regex=r'^Principal_Investigator_\d+_NPI$').columns.to_list()
    npi_cols.append('Covered_Recipient_NPI')
    rows_all_na = df[npi_cols].isna().all(axis=1)
    npi_missing = df[rows_all_na]
    # save dropped rows to csv
    filename = fileout.split("/")[-1]
    npi_missing.to_csv(f"data/final_files/research_payments/missing_npis/{filename}", index=False)

    # Drop nan NPI rows from the original DataFrame
    df = df.dropna(subset=npi_cols, how='all')
    # convert nan to empty str
    for col in npi_cols:
        df[col] = df[col].astype(float).astype(int).astype(str)

    # TODO: is Grace's rsrch sas code doing something else to col names?
    # 2. Harmonize column names
    df = harmonize_col_names(df, year, dataset_type)

    # TODO: need to look in all NPI cols for NPI - edit add_new_columns with a conditional if rsrch?
    # 3. Add Columns: Drug_Name, Prostate_Drug_Type, Onc_Prescriber
    drug_cols = get_harmonized_drug_cols(df)
    logger.info("Adding new columns to %s", fileout)
    df = add_new_columns(df, drug_cols, npi_set)
    assert 'Drug_Name' in df.columns
    assert 'Prostate_Drug_Type' in df.columns
    assert 'Onc_Prescriber' in df.columns

    # Remove decimals from cols
    df['Prostate_Drug_Type'] = df['Prostate_Drug_Type'].astype(float).astype(int).astype(str)
    df['Onc_Prescriber'] = df['Onc_Prescriber'].astype(float).astype(int).astype(str)
    df['Covered_Recipient_Profile_ID'] = df['Covered_Recipient_Profile_ID'].astype(float).astype(int).astype(str)

    # fill all nan with ''
    df.fillna('', inplace=True)
    
    # 4. Save to CSV (save all cols as string)
    df.astype(str).to_csv(fileout, index=False)


def run_op_cleaner(file_to_clean, dataset_type, year, year2npis_path):
    # load year2npis_path (json)
    with open(year2npis_path, 'r') as f:
        year2npis = json.load(f)
    # get npi_set for year
    year_str = str(year)
    npi_set = year2npis[year_str]

    fileout = f"data/final_files/{dataset_type}_payments/{dataset_type}_{year}.csv"
    
    if dataset_type == "general":
        clean_op_data_gnrl(
            file_to_clean,
            fileout,
            year,
            npi_set
            )
    
    else:
        clean_op_data_rsrch(
            file_to_clean,
            fileout,
            year,
            npi_set
            )
