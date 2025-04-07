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


# def is_onc_prescriber(prostate_drug_type, recipient_npi, npi_set):
#     """
#     Determine if a recipient is an oncology prescriber based on drug type and NPI
    
#     Args:
#         prostate_drug_type (int): Whether the drug is a prostate drug (1) or not (0)
#         recipient_npi: The NPI of the covered recipient
#         npi_set: Set of NPIs that are considered oncology prescribers
        
#     Returns:
#         int: 1 if the recipient is an oncology prescriber for prostate drugs, 0 otherwise
#     """
#     return 1 if prostate_drug_type == 1 and recipient_npi in npi_set else 0


def is_onc_prescriber(prostate_drug_type, recipient_npis, npi_set):
    """
    Determine if a recipient is an oncology prescriber based on drug type and NPI
    
    Args:
        prostate_drug_type (int): Whether the drug is a prostate drug (1) or not (0)
        recipient_npis: All NPIs from the row -> ANY works for the condition
        npi_set: Set of NPIs that are considered oncology prescribers
        
    Returns:
        int: 1 if the recipient is an oncology prescriber for prostate drugs, 0 otherwise
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
            logger.info("Found targetdrug name in col: %s", col)
            break

    return df

def prep_general_data(df, fileout):
    # Drop rows where Covered_Recipient_NPI is nan
    npi_missing = df[df['Covered_Recipient_NPI'].isna()]
    # save dropped rows to csv
    filename = fileout.split("/")[-1]
    npi_missing.to_csv(f"data/final_files/general_payments/missing_npis/{filename}", index=False)
    
    # Drop nan NPI rows from the original DataFrame
    df = df[df['Covered_Recipient_NPI'].notna()]
    # remove the decimal if present in NPIs
    df['Covered_Recipient_NPI'] = df['Covered_Recipient_NPI'].astype(float).astype(int).astype(str)
    return df

def prep_research_data(df, fileout):
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
    return df

def clean_op_data(filepath, fileout, year, npi_set, dataset_type):
    """
    Clean and enhance Open Payments data
    Harmonize column names 
    Add columns
        Prostate_drug_type (0/1 based on Color)
        Drug_Name (generic name)
        Onc_Prescriber (1 if Prostate_drug_type == 1 AND Covered_Recipient_NPI is in npi_set)
    """
    df = pd.read_csv(filepath, dtype=str)

    # Harmonize column names (and prep 2014-2015)
    if int(year) < 2016:
        df = merge_cols_2014_2015(df)
        df = harmonize_col_names(df, year, dataset_type)
        if int(year) == 2014:
            df = add_npis_2014(df, year, dataset_type)
    else:
        df = harmonize_col_names(df, year, dataset_type)
    
    # Drop rows where NPI is nan and clean string cols formatting
    if dataset_type == "general":
        df = prep_general_data(df, fileout)
    else:
        df = prep_research_data(df, fileout)

    # # Drop rows where Covered_Recipient_NPI is nan
    # npi_missing = df[df['Covered_Recipient_NPI'].isna()]
    # # save dropped rows to csv
    # filename = fileout.split("/")[-1]
    # npi_missing.to_csv(f"data/final_files/general_payments/missing_npis/{filename}", index=False)

    # # Drop nan NPI rows from the original DataFrame
    # df = df[df['Covered_Recipient_NPI'].notna()]
    # # remove the decimal if present in NPIs
    # df['Covered_Recipient_NPI'] = df['Covered_Recipient_NPI'].astype(float).astype(int).astype(str)

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


def merge_cols_2014_2015(df):
    # 1. Merge drug columns
    for i in range(1, 6):
        df[f"Drug_Biological_Device_Med_Sup_{i}"] = df[f"Name_of_Associated_Covered_Drug_or_Biological{i}"].replace("", pd.NA).fillna(
            df[f"Name_of_Associated_Covered_Device_or_Medical_Supply{i}"]
        )

        # 2. Drop original columns
        df.drop([f"Name_of_Associated_Covered_Drug_or_Biological{i}", f"Name_of_Associated_Covered_Device_or_Medical_Supply{i}"], axis=1, inplace=True)
    return df


def add_npis_2014(df, year, dataset_type):
    # df = pd.read_csv(filepath, dtype=str)

    # # 1. Merge drug columns
    # for i in range(1, 6):
    #     df[f"Drug_Biological_Device_Med_Sup_{i}"] = df[f"Name_of_Associated_Covered_Drug_or_Biological{i}"].replace("", pd.NA).fillna(
    #         df[f"Name_of_Associated_Covered_Device_or_Medical_Supply{i}"]
    #     )

    #     # 2. Drop original columns
    #     df.drop([f"Name_of_Associated_Covered_Drug_or_Biological{i}", f"Name_of_Associated_Covered_Device_or_Medical_Supply{i}"], axis=1, inplace=True)

    # # 3. Harmonize column names
    # df = harmonize_col_names(df, year, dataset_type)

    # 4. Add NPIs to df using Profile ID
    providers_npis_ids = pd.read_csv("data/reference/providers_npis_ids.csv", dtype=str)

    # sort df and providers_npi_ids by provider ID
    df.sort_values(by="Covered_Recipient_Profile_ID", inplace=True)
    providers_npis_ids.sort_values(by="Covered_Recipient_Profile_ID", inplace=True)

    # check the IDs are in the same format
    df['Covered_Recipient_Profile_ID'] = df['Covered_Recipient_Profile_ID'].apply(
        lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() != '' else x
        )
    
    return df.merge(providers_npis_ids, on="Covered_Recipient_Profile_ID", how="left")


def prep_2016_2023_data(filepath, year, dataset_type):
    df = pd.read_csv(filepath, dtype=str)

    # 2. Harmonize column names
    df = harmonize_col_names(df, year, dataset_type)



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

    # 2. Harmonize column names
    df = harmonize_col_names(df, year, dataset_type)

    # 3. Add Columns: Drug_Name, Prostate_Drug_Type, Onc_Prescriber
    drug_cols = get_harmonized_drug_cols(df)
    logger.info("Adding new columns to %s", fileout)
    df = add_new_columns(df, drug_cols, npi_set, dataset_type)
    assert 'Drug_Name' in df.columns
    assert 'Prostate_Drug_Type' in df.columns
    assert 'Onc_Prescriber' in df.columns

    # Remove decimals from cols
    df['Prostate_Drug_Type'] = df['Prostate_Drug_Type'].astype(float).astype(int).astype(str)
    df['Onc_Prescriber'] = df['Onc_Prescriber'].astype(float).astype(int).astype(str)
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
    fileout = f"data/final_files/{dataset_type}_payments/{dataset_type}_{year}_0404.csv"
    
    clean_op_data(
        file_to_clean,
        fileout,
        year,
        npi_set,
        dataset_type
        )
    
    # else:
    #     clean_op_data_rsrch(
    #         file_to_clean,
    #         fileout,
    #         year,
    #         npi_set,
    #         dataset_type
    #         )
