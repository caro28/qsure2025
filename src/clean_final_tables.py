import pandas as pd
import os

from src._utils import (
    setup_logging,
    clean_brand_name,
    clean_generic_name,
)

from src.filter_prescribers import (
    get_set_npis,
)


def build_map_year2cols():
    """
    Build a map of year to columns
    """
    year2cols = {}
    grace_cols = pd.read_csv("data/reference/col_names/general_payments/grace_cols.csv")
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

def harmonize_col_names(df, year):
    """
    Harmonize column names using a map of year to columns from grace_cols.csv
    """
    # Get map of year2cols
    year2cols = build_map_year2cols()
    df.columns = year2cols[year]
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



# TODO: goal is to only run this for our target drugs, so we should get out of the loop if drug_name is not in brand2generic?
def add_new_columns(df, drug_cols):
    brand2generic, brand2color = build_ref_data_maps()
    npi_path="data/filtered/prescribers/prescribers_final_npis.csv"
    npi_set = get_set_npis(npi_path)

    # drop rows where Covered_Recipient_NPI is nan and save to csv
    # Split out the rows with NaN
    npi_missing = df[df['Covered_Recipient_NPI'].isna()]
    npi_missing.to_csv("data/final_files/general_payments/missing_npis/npi_missing.csv", index=False)

    # Drop those rows from the original DataFrame
    df = df[df['Covered_Recipient_NPI'].notna()]
    df['Covered_Recipient_NPI'] = df['Covered_Recipient_NPI'].astype(float).astype(int).astype(str)
    import pdb; pdb.set_trace()

    df = df.loc[df['Covered_Recipient_NPI']=='1922277268'] ################################ TESTING
    print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    for idx, row in df.iterrows():
        # iterate through drug_cols
        for col in drug_cols:
            print(f"Processing col {col}")
            drug_name = str(row[col])
            if pd.isna(drug_name) or drug_name == 'nan':
                continue
            elif drug_name == '':
                continue
            drug_name = clean_brand_name(drug_name)

            # add Prostate_Drug_Type
            ## TODO: test new functions
            df.at[idx, 'Prostate_Drug_Type'] = get_prostate_drug_type(drug_name, brand2color)
            print("added value to Prostate_Drug_Type")

            # add Onc_Prescriber col: 1 if Prostate_Drug_Type == 1 AND Covered_Recipient_NPI is in npi_set
            result = is_onc_prescriber(df.at[idx, 'Prostate_Drug_Type'], df.at[idx, 'Covered_Recipient_NPI'], npi_set)
            df.at[idx, 'Onc_Prescriber'] = result
            print("added value to Onc_Prescriber")

            # add generic_name to df
            try:
                generic_name = brand2generic[drug_name]
            except KeyError:
                continue
            df.at[idx, 'Drug_Name'] = generic_name
            print("added value to Drug_Name")

    return df

def clean_op_data(filepath, fileout, year):
    """
    Clean and enhance Open Payments data
    Harmonize column names 
    Add columns
        Prostate_drug_type, (0/1 based on Color)
        Drug_Name, (generic name)
        Onc_Prescriber, (1 if Prostate_drug_type == 1 AND Covered_Recipient_NPI is in npi_set)
    """
    # 1. Harmonize column names
    df = pd.read_csv(filepath)
    df = harmonize_col_names(df, year)

    # 2. Add Columns: Drug_Name, Prostate_drug_type, Onc_Prescriber
    drug_cols = get_harmonized_drug_cols(df)
    df = add_new_columns(df, drug_cols)
    # assert 'Drug_Name' in df.columns
    # assert 'Prostate_Drug_Type' in df.columns
    # assert 'Onc_Prescriber' in df.columns
    
    # 3. Save to CSV (save all cols as string)
    df.astype(str).to_csv(fileout, index=False)


def run_op_cleaner(files_dir, dataset_type):
    # load npi_list
    npi_path="data/filtered/prescribers/prescribers_final_npis.csv"
    npi_df = pd.read_csv(npi_path)
    npi_set = npi_df['Prscrbr_NPI']
    assert len(npi_set.value_counts().unique()) == 1

    # For each payment file in files_dir
    for file in os.listdir(files_dir):
        # check if this is a file and not a directory
        if os.path.isfile(os.path.join(files_dir, file)):
            # get year from filename
            year = file.split("_")[-1].split(".")[0]
            fileout = f"data/final_files/{dataset_type}_payments/{dataset_type}_{year}.csv"
            
            clean_op_data(
                os.path.join(files_dir, file),
                npi_set,
                fileout,
                year
                )
