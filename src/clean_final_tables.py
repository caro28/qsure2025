import os
import pandas as pd
import json
import logging
import numpy as np
from src._utils import (
    setup_logging,
    clean_brand_name,
    clean_generic_name,
)

setup_logging()
logger = logging.getLogger(__name__)    


def build_map_year2cols(dataset_type, path_to_cols):
    """
    Build a map of year to column names for column harmonization. Uses 
    Args:
        dataset_type (str): "general" or "research"
    Returns:
        dict: year to list of column names
    """
    year2cols = {}
    grace_cols = pd.read_csv(path_to_cols)
    years = grace_cols.columns
    for year in years:
        year2cols[year] = grace_cols[year].dropna().to_list()
    return year2cols

def build_ref_data_maps(ref_path):
    """
    Build maps of brand names to generic names and color (green/yellow). Preps 
    drug names for matching by calling clean_generic_name and clean_brand_name.
    Args:
        ref_path (str): path to ProstateDrugList.csv
    Returns:
        tuple (brand2generic, brand2color): dicts of brand names to generic names and color
    """
    # load ProstateDrugList.csv
    ref_df = pd.read_csv(ref_path)
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

def harmonize_col_names(df, year, dataset_type, path_to_harmonized_cols):
    """
    Harmonize column names using a map of year to columns from grace_cols.csv
    Args:
        df (pd.DataFrame): OP df to harmonize
        year (str): OP file data year
        dataset_type (str): general or research
        path_to_harmonized_cols (str): path to grace_cols.csv (different for 
            general vs research)
    Returns:
        pd.DataFrame: df with column names changed to harmonized names
    """
    # Get map of year2cols
    year2cols = build_map_year2cols(dataset_type, path_to_harmonized_cols)
    df.columns = year2cols[str(year)]
    return df

def get_harmonized_drug_cols(df):
    """
    Get a list of drug column names from df after harmonization of column names
    Args:
        df (pd.DataFrame): OP df to get drug columns from
    Returns:
        list: list of harmonized drug column names
    """
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


def is_onc_prescriber(prostate_drug_type, recipient_npis, npi_set):
    """
    Determine value for new column 'Onc_Prescriber'. 1 if prostate_drug_type 
    is 1 AND prescriber NPI is in npi_set, 0 otherwise.
    Args:
        prostate_drug_type (int): value in prostate_drug_type column (0/1)
        recipient_npis (list): prescriber NPI. For research OP files, we check 
            multiple NPIs from multiple NPI columns. For general OP files,
            recipient_npis contains 1 NPI only.
        npi_set: Set of NPIs that are considered oncology prescribers
    Returns:
        int: 1 or 0
    """
    if int(prostate_drug_type) == 0:
        return 0
    elif int(prostate_drug_type) == 1:
        for npi in recipient_npis:
            if npi in npi_set:
                return 1
            else:
                continue
        # finished the loop, so we checked all npi's and none are in npi_set
        return 0
    else:
        raise ValueError("Unsupported value type in 'Prostate_Drug_Type' (1/0)")


def add_new_columns(df, drug_cols, npi_set, dataset_type):
    """
    Adds new columns to filtered OP file: Drug_Name, Prostate_Drug_Type, Onc_Prescriber.
    Applies clean_brand_name to drug_name in drug_cols for each row of df.
    Args:
        df (pd.DataFrame): filtered OP file
        drug_cols (list): list of OP file's drug column names
        npi_set (list): unique NPIs gathered from prescribers database
        dataset_type (str): "general" or "research"
    Returns:
        pd.DataFrame: filtered OP df with new columns added 
    """
    brand2generic, brand2color = build_ref_data_maps("data/reference/ProstateDrugList.csv")

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

            recipient_npis= []
            # add Onc_Prescriber col:
            if dataset_type == "general":
                recipient_npis.append(df.at[idx, 'Covered_Recipient_NPI'])
            else:
                new_npi_cols = [
                    'Covered_Recipient_NPI',
                    'PI_1_NPI',
                    'PI_2_NPI',
                    'PI_3_NPI',
                    'PI_4_NPI',
                    'PI_5_NPI'
                ]
                for npi_col in new_npi_cols:
                    recipient_npis.append(df.at[idx, npi_col])
                # remove empty strings
                recipient_npis = [npi for npi in recipient_npis if npi != '']
            result = is_onc_prescriber(df.at[idx, 'Prostate_Drug_Type'], recipient_npis, npi_set)
            df.at[idx, 'Onc_Prescriber'] = result
        
            # break loop because we are keeping the first drug name found in our target list
            break
    return df

def prep_general_data(df, filename, dir_missing_npis):
    """
    Drops rows where Covered_Recipient_NPI is nan and cleans NPIs by removing 
    any decimals if present. Saves dropped rows to csv in dir_missing_npis.
    Args:
        df (pd.DataFrame): OP df to prep
        filename (str): filename to use when saving dropped rows to csv
        dir_missing_npis (str): directory to save dropped rows to
    Returns:
        pd.DataFrame: OP df with rows dropped where Covered_Recipient_NPI is nan
    """
    # Drop rows where Covered_Recipient_NPI is nan
    npi_missing = df[df['Covered_Recipient_NPI'].isna()]
    # save dropped rows to csv
    npi_missing.to_csv(f"{dir_missing_npis}{filename}", index=False)
    
    # Drop nan NPI rows from the original DataFrame and create a copy
    df = df[df['Covered_Recipient_NPI'].notna()].copy()
    # remove the decimal if present in NPIs
    df['Covered_Recipient_NPI'] = df['Covered_Recipient_NPI'].astype(float).astype(int).astype(str)
    return df

def prep_research_data(df, filename, dir_missing_npis):
    """
    Drops rows where NPI val is nan in all NPI cols and cleans NPIs by removing 
    any decimals if present. Saves dropped rows to csv in dir_missing_npis.
    Args:
        df (pd.DataFrame): OP df to prep
        filename (str): filename to use when saving dropped rows to csv
        dir_missing_npis (str): directory to save dropped rows to
    Returns:
        pd.DataFrame: OP df with rows dropped where NPI val is nan in all NPI cols
    """
    # 1. Clean df: drop rows where NPI val is nan in all NPI cols
    npi_cols = df.filter(regex=r'^Principal_Investigator_\d+_NPI$').columns.to_list()
    npi_cols.append('Covered_Recipient_NPI')
    rows_all_na = df[npi_cols].isna().all(axis=1)
    npi_missing = df[rows_all_na]
    # save dropped rows to csv
    npi_missing.to_csv(f"{dir_missing_npis}{filename}", index=False)

    # Drop nan NPI rows from the original DataFrame
    df = df.dropna(subset=npi_cols, how='all').copy()
    # remove the decimal if present in NPIs
    for col in npi_cols:
        df[col] = df[col].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() != '' else x
            )
    return df


def merge_cols_2014_2015(df):
    """
    Merges drug columns following Grace's logic from .sas files:
        if VAR[X_1]="" then Drug_Biological_Device_Med_Sup_1=VAR[Y_1];
		    else if VAR[X_1] ne "" then Drug_Biological_Device_Med_Sup_1=VAR[X_1];
        Because drug names are split across two sets of columns, we keep the 
        column that contains a value. Two sets of columns are: 
            'Name_of_Associated_Covered_Drug_or_BiologicalX',
            'Name_of_Associated_Covered_Device_or_Medical_SupplyY'
    Args:
        df (pd.DataFrame): OP df that needs drug columns merged
    Returns:
        pd.DataFrame: OP df with drug columns merged
    """
    # 1. Merge drug columns
    for i in range(1, 6):
        df[f"Drug_Biological_Device_Med_Sup_{i}"] = df[f"Name_of_Associated_Covered_Drug_or_Biological{i}"].replace("", pd.NA).fillna(
            df[f"Name_of_Associated_Covered_Device_or_Medical_Supply{i}"]
        )
        # 2. Drop original columns
        df.drop([f"Name_of_Associated_Covered_Drug_or_Biological{i}", f"Name_of_Associated_Covered_Device_or_Medical_Supply{i}"], axis=1, inplace=True)
    return df


def add_npis_2014(df, dataset_type, profile_id_cols, providers_npis_ids):
    """
    Add NPIs to 2014 OP df (general and research) on Covered_Recipient_Profile_ID
      using CMS' Covered Recipient Profile Supplement:
      https://openpaymentsdata.cms.gov/dataset/23160558-6742-54ff-8b9f-cac7d514ff4e
      For Research files, we add one NPI column per profile_id_col:
        [Covered_Recipient_NPI, PI_1_NPI, PI_2_NPI, PI_3_NPI, PI_4_NPI, PI_5_NPI]
    Args:
        df (pd.DataFrame): OP df to add NPIs to
        dataset_type (str): "general" or "research"
        profile_id_cols (list): list of column names to merge on to get NPIs
          (there are multiple profile_id_cols in OP research files but only 1 in OP general files)
    Returns:
        pd.DataFrame: OP df with NPIs added
    """
    # 4. Add NPIs to df using Profile ID
    for col in profile_id_cols:
        npi_df = providers_npis_ids.copy()
        # rename Covered_Recipient_Profile_ID to col in providers_npis_ids for merge
        current_columns = npi_df.columns.tolist()
        current_columns[0] = col
        npi_df.columns = current_columns

        # Convert to string type before merging
        npi_df[col] = npi_df[col].fillna('').astype(str)
        df[col] = df[col].fillna('').astype(str)


        # sort df and providers_npi_ids by provider ID
        df.sort_values(by=col, inplace=True)
        npi_df.sort_values(by=col, inplace=True)

        # check the IDs are in the same format
        df[col] = df[col].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() != '' else x
            )
        
        if dataset_type == "general":
            return df.merge(npi_df, on=col, how="left").copy()
        
        else:
            if col == "Covered_Recipient_Profile_ID":
                # don't add suffix and don't rename new col from merge
                df = df.merge(npi_df, on=col, how="left").copy()
            else:
                pi_npi_num = col.split("_")[1]
                df = df.merge(npi_df, on=col, how="left", suffixes=('', f'_{pi_npi_num}')).copy()
                # rename new col from merge to: PI_1_NPI, PI_2_NPI, PI_3_NPI, PI_4_NPI, PI_5_NPI
                df.rename(columns={f"Covered_Recipient_NPI_{pi_npi_num}": f"PI_{pi_npi_num}_NPI"}, inplace=True)
    # fill all nan with ''
    df = df.fillna('')
    return df


# def prep_2016_2023_data(filepath, year, dataset_type):
#     df = pd.read_csv(filepath, dtype=str)

#     # 2. Harmonize column names
#     df = harmonize_col_names(df, year, dataset_type)


def clean_op_data(
        filepath, 
        fileout, 
        year, 
        npi_set, 
        dataset_type, 
        path_to_harmonized_cols, 
        path_providers_npis_ids, 
        dir_missing_npis
        ):
    """
    Clean and enhance Open Payments data
    Harmonize column names 
    Add columns
        Prostate_drug_type (0/1 based on Color)
        Drug_Name (generic name)
        Onc_Prescriber (1 if Prostate_drug_type == 1 AND Covered_Recipient_NPI is in npi_set)
    Args:
        filepath (str): path to OP file to clean
        fileout (str): path to save cleaned OP file
        year (int): year of OP file
        npi_set (list): unique list of NPIs to check against for Onc_Prescriber value
        dataset_type (str): "general" or "research"
        path_to_harmonized_cols (str): path to grace_cols.csv (different for 
            general vs research)
        path_providers_npis_ids (str): path to providers_npis_ids.csv
        dir_missing_npis (str): directory to save rows dropped due to missing NPIs
    """
    df = pd.read_csv(filepath, dtype=str)
    
    providers_npis_ids = pd.read_csv(path_providers_npis_ids, dtype=str)

    # Harmonize column names (and prep 2014-2015)
    if int(year) < 2016:
        df = merge_cols_2014_2015(df)
        df = harmonize_col_names(df, year, dataset_type, path_to_harmonized_cols)
        if int(year) == 2014:
            if dataset_type == "research":
                profile_id_cols = [
                'Covered_Recipient_Profile_ID',
                'PI_1_Profile_ID',
                'PI_2_Profile_ID',
                'PI_3_Profile_ID',
                'PI_4_Profile_ID',
                'PI_5_Profile_ID'
                ]
            else:
                profile_id_cols = ['Covered_Recipient_Profile_ID']
            df = add_npis_2014(df, dataset_type, profile_id_cols, providers_npis_ids)
    else:
        df = harmonize_col_names(df, year, dataset_type, path_to_harmonized_cols)
    
    # Drop rows where NPI is nan and clean string cols formatting
    filename = fileout.split("/")[-1]
    if dataset_type == "general":
        df = prep_general_data(df, filename, dir_missing_npis)
    else:
        df = prep_research_data(df, filename, dir_missing_npis)

    # Add Columns: Drug_Name, Prostate_Drug_Type, Onc_Prescriber
    drug_cols = get_harmonized_drug_cols(df)
    logger.info("Adding new columns to %s", fileout)
    df = add_new_columns(df, drug_cols, npi_set, dataset_type)
    assert 'Drug_Name' in df.columns
    assert 'Prostate_Drug_Type' in df.columns
    assert 'Onc_Prescriber' in df.columns

    # Remove decimals from cols
    df['Prostate_Drug_Type'] = df['Prostate_Drug_Type'].astype(float).astype(int).astype(str)
    df['Onc_Prescriber'] = df['Onc_Prescriber'].astype(float).astype(int).astype(str)
    # df['Covered_Recipient_Profile_ID'] = df['Covered_Recipient_Profile_ID'].astype(float).astype(int).astype(str)
    df['Covered_Recipient_Profile_ID'] = df['Covered_Recipient_Profile_ID'].apply(
        lambda x: str(int(x)) if pd.notna(x) else x
        )

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

    # fileout = f"data/final_files/{dataset_type}_payments/{dataset_type}_{year}.csv"
    fileout = f"data/final_files/{dataset_type}_payments/{dataset_type}_{year}_apr14.csv"
    path_to_harmonized_cols =f"data/reference/col_names/{dataset_type}_payments/grace_cols.csv"
    path_providers_npis_ids = "data/reference/providers_npis_ids.csv"
    dir_missing_npis = f"data/final_files/{dataset_type}_payments/missing_npis/"
    
    clean_op_data(
        file_to_clean,
        fileout,
        year,
        npi_set,
        dataset_type,
        path_to_harmonized_cols,
        path_providers_npis_ids,
        dir_missing_npis
        )
    
