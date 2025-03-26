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
    return final_df


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


# Step 2: Data Cleaning & Enhancement
def clean_op_data(filepath, year2cols, brand2generic, brand2color, npi_set, fileout, year):
    """
    Clean and enhance Open Payments data
    """
    # 1. Harmonize column names
    df = pd.read_csv(filepath)
    # drop levenshtein_distance column
    df.drop(columns='levenshtein_distance', inplace=True)
    # rename columns
    df.columns = year2cols[year]

    # 2. Add Prostate_drug_type (0/1 based on Color)
    # 3. Normalize drug names by adding Drug_Name col with generic names
    # instantiate drug_cols using range: Drug_Biological_Device_Med_Sup_1 through Drug_Biological_Device_Med_Sup_5
    prefix = "Drug_Biological_Device_Med_Sup_"
    drug_cols = [col for col in df.columns if col.lower().startswith(prefix.lower())]
    
    for idx, row in df.iterrows():
        # iterate through drug_cols
        for col in drug_cols:
            print(f"Processing col {col}")
            # get drug_name
            drug_name = str(row[col])
            if pd.isna(drug_name) or drug_name == 'nan':
                continue
            elif drug_name == '':
                continue
            drug_name = clean_brand_name(drug_name)
            
            # TODO: goal is to only run this for our target drugs, so we should get out of the loop if drug_name is not in brand2generic
            # TODO: is this correct?
            try:
                # get generic_name
                generic_name = brand2generic[drug_name]
            except KeyError:
                continue
            
            # add generic_name to df
            df.at[idx, 'Drug_Name'] = generic_name
            print("added value to Drug_Name")

            # add Prostate_Drug_Type
            df.at[idx, 'Prostate_Drug_Type'] = 1 if brand2color[drug_name] == 'yellow' else 0
            print("added value to Prostate_Drug_Type")

            # add Onc_Prescriber col: 1 if Prostate_Drug_Type == 1 AND Covered_Recipient_NPI is in npi_set
            df.at[idx, 'Onc_Prescriber'] = 1 if df.at[idx, 'Prostate_Drug_Type'] == 1 and df.at[idx, 'Covered_Recipient_NPI'] in npi_set else 0
            print("added value to Onc_Prescriber")

    # save to csv
    df.to_csv(fileout, index=False)


def run_op_cleaner(files_dir, dataset_type):
    # Get map of year2cols
    year2cols = build_map_year2cols()
    brand2generic, brand2color = build_ref_data_maps()
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
                year2cols,
                brand2generic,
                brand2color,
                npi_set,
                fileout,
                year
                )
    



def main():
    # # Filter Open Payments (2014-2023)
    years = range(2023, 2024) # Use 2023 only for now to draft code
    for year in years:
        filter_open_payments(year, "general")
        print(f"Finished filtering general payments for {year}")
    #     filter_open_payments(year, "research")
    
    # Concatenate 2023
    concatenate_chunks("data/filtered/general_payments/chunks/", "general", 2023)
    print("Finished concatenating general payments for 2023")
    # load 2023 filtered OP df
    final_df = pd.read_csv("data/filtered/general_payments/general_2023.csv")

    # Filter Prescribers
    path_in = "data/filtered/prescribers/prescribers_filtered_prscrb_type.csv"
    path_out = "data/filtered/prescribers/prescribers_filtered_type_drug_names.csv"
    filter_prescribers_by_drug_names(path_in, path_out)
    print("Finished filtering prescribers by drug names")
    # load prescribers filtered by type and drug names
    path_in = "data/filtered/prescribers/prescribers_filtered_type_drug_names.csv"
    prescribers_filtered = pd.read_csv(path_in)
    
    # get final npis
    final_npis = get_final_npis(prescribers_filtered)
    print("Finished getting final npis")
    
    # Clean  all files
    for year in years:
        # for dataset_type in ["general", "research"]:
        for dataset_type in ["general"]:
            files_dir = f"data/filtered/{dataset_type}_payments_0326/"
            run_op_cleaner(files_dir, dataset_type)
            print(f"Finished cleaning {dataset_type} payments for {year}")


if __name__ == "__main__":
    main()
