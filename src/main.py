import pandas as pd
import os
import logging
from Levenshtein import distance


from src._utils import (
    setup_logging,
    get_op_drug_columns,
    clean_brand_name,
    clean_generic_name,
)

setup_logging()
logger = logging.getLogger(__name__)


# Step 1a: Open Payments Filtering
def get_op_raw_path(year, dataset_type):
    """
    Get the path to the raw Open Payments data for a given year and dataset type
    """
    acronym = "GNRL" if dataset_type == "general" else "RSRCH"
    parent_dir = "data/raw/"
    dataset_dir = f"{dataset_type}_payments/"
    prefix = f"OP_DTL_{acronym}_PGYR{year}"
    # Find the file in dataset_dir that starts with prefix
    for file in os.listdir(parent_dir + dataset_dir):
        if file.startswith(prefix):
            return os.path.join(parent_dir + dataset_dir, file)
    # log ValueError if not file found
    logger.error(f"No file found for {year} {dataset_type}_payments")
    raise ValueError(f"No file found for {year} {dataset_type}_payments")


def get_ref_drug_names():
    # Load ProstateDrugList.csv
    ref_path = "data/reference/ProstateDrugList.csv"
    ref_df = pd.read_csv(ref_path)
    brand_cols = [col for col in ref_df.columns if col.startswith('Brand_name')]

    # convert values in brand_cols and Generic_name to list
    ref_drug_names = []
    for col in brand_cols:
        ref_drug_names.extend(ref_df[col].drop_duplicates().dropna().to_list())
    # clean brand names
    ref_drug_names = [clean_brand_name(name) for name in ref_drug_names]
    # get generic names
    generic_names = ref_df['Generic_name'].drop_duplicates().dropna().to_list()
    # clean generic names   
    generic_names = [clean_generic_name(name) for name in generic_names]
    # combine brand and generic names
    ref_drug_names.extend(generic_names)
    # double check for duplicates
    return list(set(ref_drug_names))

    
def filter_open_payments_levenshtein(year, dataset_type, max_distance=1):
    """
    Filter Open Payments data for a given year and dataset type
    (General or Research) based on drug name matches with 
    Levenshtein distance <= 1
    """
    # get cleaned drug names (brand and generic) from ProstateDrugList.csv
    ref_drug_names = get_ref_drug_names()

    # # Load OP data for year/type
    op_path = get_op_raw_path(year, dataset_type)
    # Get drug columns
    op_drug_cols = get_op_drug_columns(pd.read_csv(op_path, nrows=1))
    
    # load csv in chunks
    chunksize = 100_000
    chunks = pd.read_csv(op_path, chunksize=chunksize)
    
    matching_record_ids = set()
    matched_rows = 0
    # Filter each chunk using Levenshtein distance <= 1
    for i, chunk in enumerate(chunks):
        distances = []
        # Iterate through each row
        for idx, row in chunk.iterrows():
            # Check each drug column
            for col in op_drug_cols:
                drug_name = str(row[col])
                if pd.isna(drug_name) or drug_name == 'nan':
                    continue
                elif drug_name == '':
                    continue
                # Apply clean_brand_name and then check against each reference drug
                drug_name = clean_brand_name(drug_name)
                for ref_name in ref_drug_names:
                    dist = distance(drug_name, ref_name)
                    if dist <= max_distance:
                        matching_record_ids.add(row['Record_ID'])
                        distances.append(dist)
                        # Break both loops once we find a match
                        break
                else:
                    continue
                break

        filtered_chunk = chunk[chunk['Record_ID'].isin(matching_record_ids)]
        logger.info("Processed %s chunks, Matched %s rows for %s %s", i+1, len(filtered_chunk), year, dataset_type)
        matched_rows += len(filtered_chunk)

        # Save to CSV if filtered chunk is not empty
        if not filtered_chunk.empty:
            # Create directory if it doesn't exist
            os.makedirs(f"data/filtered/{dataset_type}_payments/chunks/", exist_ok=True)
            # add column for distance
            filtered_chunk['levenshtein_distance'] = distances
            filtered_chunk.to_csv(f"data/filtered/{dataset_type}_payments/chunks/{dataset_type}_{year}_chunk_{i+1}.csv", index=False)
            logger.info("Saved chunk %s for %s %s", i+1, year, dataset_type)

    logger.info("Matched %s rows for %s %s", matched_rows, year, dataset_type)


def filter_open_payments(year, dataset_type):
    """
    Filter Open Payments data for a given year and dataset type
    (General or Research) based on exact drug name matches
    """
    # get cleaned drug names (brand and generic) from ProstateDrugList.csv
    ref_drug_names = get_ref_drug_names()

    # # Load OP data for year/type
    op_path = get_op_raw_path(year, dataset_type)
    # load csv in chunks
    chunksize = 100_000
    chunks = pd.read_csv(op_path, chunksize=chunksize)
    
    # Filter each chunk using exact drug name matches
    total_matched_rows = 0
    # Get drug columns
    op_drug_cols = get_op_drug_columns(pd.read_csv(op_path, nrows=1))
    dir_out = f"data/filtered/{dataset_type}_payments/chunks/"
    for i, chunk in enumerate(chunks):
        matched_rows, row_idx = find_matches(chunk, op_drug_cols, ref_drug_names)
        logger.info("Matched %s rows", matched_rows)
        filtered_chunk = chunk.iloc[row_idx]
        # Save to CSV if filtered chunk is not empty
        if not filtered_chunk.empty:
            filtered_chunk.to_csv(f"{dir_out}{dataset_type}_{year}_chunk_{i+1}.csv", index=False)
            logger.info("Saved chunk %s, found %s matches", i+1, matched_rows)
            total_matched_rows += matched_rows

    logger.info("Matched %s rows for %s %s", total_matched_rows, year, dataset_type)


def concatenate_chunks(chunks_dir, fileout):
    """
    Concatenate all chunks vertically into a single CSV
    """
    # Get all chunk files
    chunks = os.listdir(chunks_dir)
    
    # Write first chunk with header
    pd.read_csv(os.path.join(chunks_dir, chunks[0])).to_csv(fileout, index=False)
    
    # Append all other chunks without headers
    for chunk in chunks[1:]:
        pd.read_csv(os.path.join(chunks_dir, chunk)).to_csv(
            fileout, 
            mode='a', 
            header=False, 
            index=False
        )


def find_matches(df, drug_cols, drug_names):
    row_idx = []
    matched_rows = 0
    # iterate through rows
    for idx, row in df.iterrows():
        # iterate through drug_cols
        for col in drug_cols:
            drug_name = str(row[col])
            if pd.isna(drug_name) or drug_name == 'nan':
                continue
            elif drug_name == '':
                continue
            # Apply clean_brand_name and then check against drug_names
            drug_name = clean_brand_name(drug_name)
            for tgt_name in drug_names:
                if drug_name == tgt_name:
                    row_idx.append(idx)
                    matched_rows += 1
                    break
                else:
                    continue
                break

    return matched_rows, row_idx


# Step 1b: Prescribers Filtering
def filter_prescribers_by_drug_names(path_in, fileout):
    """
    Filter Prescribers data to find qualifying NPIs
    NB: Early years are missing NPIs; get those from https://openpaymentsdata.cms.gov/dataset/23160558-6742-54ff-8b9f-cac7d514ff4e
    """
    drug_names = ['bicalutamide', 'abiraterone', 'enzalutamide', 'apalutamide', 'darolutamide']

    # Chunk the df prescribers_filtered_type into 100_000 rows, then filter each chunk
    chunksize = 100_000
    chunks = pd.read_csv(path_in, chunksize=chunksize)
    # Filter rows with drug names in Brnd_Name or Gnrc_Name
    total_matched_rows = 0
    drug_cols = ['Brnd_Name', 'Gnrc_Name']
    dir_out = "data/filtered/prescribers/chunks/"
    for i, chunk in enumerate(chunks):
        matched_rows, row_idx = find_matches(chunk, drug_cols, drug_names)
        logger.info("Matched %s rows", matched_rows)
        filtered_chunk = chunk.iloc[row_idx]
        # save filtered chunk to csv if not empty
        if not filtered_chunk.empty:
            filtered_chunk.to_csv(f"{dir_out}prescribers_chunk_{i+1}.csv", index=False)
            logger.info("Saved chunk %s, found %s matches", i+1, matched_rows)
            total_matched_rows += matched_rows

    logger.info("Matched %s rows for prescribers", total_matched_rows)

    # Concatenate all chunks vertically into a single CSV
    concatenate_chunks(dir_out, fileout)


# Step 2: Group by id and get sorted unique years where target_names appeared
def has_three_consecutive_years(group):
    years = sorted(group['Year'].unique())
    # Check for any 3 consecutive years
    for i in range(len(years) - 2):
        if int(years[i + 2]) - int(years[i]) == 2:
            return True
    return False

def get_final_npis(filtered_prescribers):
    df_grouped_npi = filtered_prescribers.groupby('Prscrbr_NPI')
    valid_npis = (
        df_grouped_npi
        .filter(has_three_consecutive_years)
        .Prscrbr_NPI
        .unique()
    )
    # Step 4: Filter original 'filtered' DataFrame based on valid_ids
    final_df = filtered_prescribers[filtered_prescribers['Prscrbr_NPI'].isin(valid_npis)].drop_duplicates(subset=['Prscrbr_NPI'])
    # Keep only columns: Prscrbr_NPI, Prscrbr_Type, Brnd_Name, Gnrc_Name
    final_df = final_df[['Prscrbr_NPI', 'Prscrbr_Type', 'Brnd_Name', 'Gnrc_Name']]
    # save to csv
    final_df.to_csv("data/filtered/prescribers/prescribers_final_npis.csv", index=False)


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


def get_set_npis():
    # load npi_list
    npi_path="data/filtered/prescribers/prescribers_final_npis.csv"
    npi_df = pd.read_csv(npi_path)
    npi_set = npi_df['Prscrbr_NPI']
    assert len(npi_set.value_counts().unique()) == 1
    return npi_set

# TODO: goal is to only run this for our target drugs, so we should get out of the loop if drug_name is not in brand2generic?
def add_new_columns(df, drug_cols):
    brand2generic, brand2color = build_ref_data_maps()
    npi_set = get_set_npis()
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
            
            # add generic_name to df
            try:
                generic_name = brand2generic[drug_name]
            except KeyError:
                continue
            df.at[idx, 'Drug_Name'] = generic_name
            print("added value to Drug_Name")

            # add Prostate_Drug_Type
            df.at[idx, 'Prostate_Drug_Type'] = 1 if brand2color[drug_name] == 'yellow' else 0
            print("added value to Prostate_Drug_Type")

            # add Onc_Prescriber col: 1 if Prostate_Drug_Type == 1 AND Covered_Recipient_NPI is in npi_set
            df.at[idx, 'Onc_Prescriber'] = 1 if df.at[idx, 'Prostate_Drug_Type'] == 1 and df.at[idx, 'Covered_Recipient_NPI'] in npi_set else 0
            print("added value to Onc_Prescriber")
    return df
    

# Step 2: Data Cleaning & Enhancement
def clean_op_data(filepath, npi_set, fileout, year):
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
    df = add_new_columns(df, drug_cols, npi_set)
    assert 'Drug_Name' in df.columns
    assert 'Prostate_drug_type' in df.columns
    assert 'Onc_Prescriber' in df.columns
    
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
    



def main():
    # # 1. Filter Open Payments (2014-2023)
    years = range(2023, 2024) # Use 2023 only for now to draft code
    dataset_types = ["general"]
    # Filter in chunks and save intermediary files
    for dataset_type in dataset_types:
        for year in years:
            filter_open_payments(year, dataset_type)
            print(f"Finished filtering {dataset_type} payments for {year}")
    
    # Concatenate intermediary files
    for dataset_type in dataset_types:
        op_chunks_dirs = f"data/filtered/{dataset_type}_payments/chunks/"
        for year in years:
            output_file = f"data/filtered/{dataset_type}_payments/chunks/{dataset_type}_{year}.csv"
            concatenate_chunks(op_chunks_dirs, output_file)
            print(f"Finished concatenating {dataset_type} payments for {year}")

    # 2. Filter Prescribers
    # Concatenate raw prescribers chunks (already filtered by prescriber type)
    prescribers_chunks_dir = "data/filtered/prescribers/chunks/"
    prescribers_filtered_prscrb_type = "data/filtered/prescribers/prescribers_filtered_prscrb_type.csv"
    concatenate_chunks(prescribers_chunks_dir, prescribers_filtered_prscrb_type)
    print("Finished concatenating prescribers chunks")
    # Filter Prescribers by drug names
    prescribers_filtered_drug_names = "data/filtered/prescribers/prescribers_filtered_type_drug_names.csv"
    filter_prescribers_by_drug_names(prescribers_filtered_prscrb_type, prescribers_filtered_drug_names)
    print("Finished filtering prescribers by drug names")
    
    # 3. Get target set of NPIs and save to CSV
    get_final_npis(prescribers_filtered_drug_names)
    print("Finished getting final npis")
    
    # 4. Harmonize Open Payments data and Add columns/flags
    for year in years:
        # for dataset_type in ["general", "research"]:
        for dataset_type in dataset_types:
            filtered_op_files_dir = f"data/filtered/{dataset_type}_payments/"
            run_op_cleaner(filtered_op_files_dir, dataset_type)
            print(f"Finished cleaning {dataset_type} payments for {year}")



if __name__ == "__main__":
    main()
