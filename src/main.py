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

    
def filter_open_payments(year, dataset_type, max_distance=1):
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


def concatenate_chunks(chunks_dir, dataset_type, year):
    """
    Concatenate all chunks vertically into a single CSV
    """
    # Get all chunk files
    chunks = os.listdir(chunks_dir)
    output_file = f"data/filtered/{dataset_type}_payments/{dataset_type}_{year}.csv"
    
    # Write first chunk with header
    pd.read_csv(os.path.join(chunks_dir, chunks[0])).to_csv(output_file, index=False)
    
    # Append all other chunks without headers
    for chunk in chunks[1:]:
        pd.read_csv(os.path.join(chunks_dir, chunk)).to_csv(
            output_file, 
            mode='a', 
            header=False, 
            index=False
        )
    
    logger.info("Saved %s %s", dataset_type, year)



# Step 1b: Prescribers Filtering
# TODO: process in chunks - slow for just 3 years
def filter_prescribers_by_drug_names(path_in, path_out):
    """
    Filter Prescribers data to find qualifying NPIs
    NB: Early years may be missing NPIs; get those from https://openpaymentsdata.cms.gov/dataset/23160558-6742-54ff-8b9f-cac7d514ff4e
    """
    drug_names = ['bicalutamide', 'abiraterone', 'enzalutamide', 'apalutamide', 'darolutamide']

    # Load prescriber data
    prescribers_filtered_type = pd.read_csv(path_in)
    
    # Filter rows with drug names in Brnd_Name or Gnrc_Name
    drug_cols = ['Brnd_Name', 'Gnrc_Name']
    
    row_idx = []
    matched_rows = 0
    # iterate through rows
    for idx, row in prescribers_filtered_type.iterrows():
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
    
    logger.info("Matched %s rows", matched_rows)
    filtered_prescribers = prescribers_filtered_type.iloc[row_idx]
    #save to csv
    filtered_prescribers.to_csv(path_out, index=False)


# TODO: check if this is correct
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
    final_df = filtered_prescribers[filtered_prescribers['Prscrbr_NPI'].isin(valid_npis)].drop_duplicates(subset=['Prscrbr_NPI', 'Prscrbr_Type'])
    # save to csv
    final_df.to_csv("data/filtered/prescribers/prescribers_final_npis.csv", index=False)
    return final_df



# Step 2: Data Cleaning & Enhancement
def clean_op_data(filepath):
    """
    Clean and enhance Open Payments data
    """
    # Harmonize column names
    # Add Prostate_drug_type (0/1 based on Color)
    # Add Drug_Name (generic names)
    # return cleaned_df
    pass

def run_op_cleaner():
    # For each payment file
    # Apply cleaning functions
    # Save intermediate results
    pass

# Step 3: Data Merging
def merge_datasets(op_data, npi_list):
    """
    Add Onc_prescriber column based on NPI matches
    """
    # Add Onc_prescriber column
    # 1 if Prostate_drug_type=1 and NPI in list
    # 0 otherwise
    # return merged_df
    pass



def main():
    # # Filter Open Payments (2014-2023)
    # years = range(2023, 2024) # Use 2023 only for now to draft code
    # for year in years:
    #     filter_open_payments(year, "general")
    # #     filter_open_payments(year, "research")
    
    # # Concatenate 2023
    # concatenate_chunks("data/filtered/general_payments/chunks/", "general", 2023)
    
    # # load 2023 filtered OP df
    # final_df = pd.read_csv("data/filtered/general_payments/general_2023.csv")
    # print(final_df["Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1"].value_counts())
    # print(final_df.head())
    # print(final_df.shape)
    # print(final_df['levenshtein_distance'].unique())

    # # Filter Prescribers
    # path_in = "data/filtered/prescribers/prescribers_filtered_prscrb_type.csv"
    # path_out = "data/filtered/prescribers/prescribers_filtered_type_drug_names.csv"
    # filter_prescribers_by_drug_names(path_in, path_out)

    # load prescribers filtered by type and drug names
    path_in = "data/filtered/prescribers/prescribers_filtered_type_drug_names.csv"
    prescribers_filtered = pd.read_csv(path_in)
    
    # get final npis
    final_npis = get_final_npis(prescribers_filtered)
    print(final_npis.shape)
    
    # # Clean and merge all files
    # for year in years:
    #     for dataset_type in ["general", "research"]:
    #         final_data = run_op_cleaner()
    #         merged_data = merge_datasets(final_data, npi_list)
    #         merged_data.to_csv(f"final_{dataset_type}_{year}.csv")



if __name__ == "__main__":
    main()
