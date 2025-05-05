import pandas as pd
import logging
import os
import json

from collections import defaultdict

from src._utils import (
    setup_logging,
    clean_brand_name,
    concatenate_chunks,
)

setup_logging()
logger = logging.getLogger(__name__)


def add_years_to_raw_prescriber_chunks(dir_in, dir_out):
    """
    Modify all csv files in dir_in to add a Year column
    Args:
        dir_in (str): path to input directory
        dir_out (str): path to output directory
    Input files: 
        Manually downloaded from https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug
        Filenames: {year}_{specialty}.csv
        Years: 2013-2022
        Specialties: Radiation Oncology, Hematology-Oncology, Medical Oncology, Hematology, Urology
        Columns: Prscrbr_NPI,Prscrbr_Type,Brnd_Name,Gnrc_Name
    Output files:
        Location: dir_out
        Filenames: {year}_{specialty}.csv
        Columns: Prscrbr_NPI,Prscrbr_Type,Brnd_Name,Gnrc_Name,Year
        Rows: same as input files
    """
    for file in os.listdir(dir_in):
        year = file.split('_')[0]
        df = pd.read_csv(os.path.join(dir_in, file), encoding='latin-1')
        df['Year'] = str(year)
        df.to_csv(os.path.join(dir_out, file), index=False)


def find_matches_prescribers(chunk, drug_cols, ref_drug_names):
    """
    Filters a single csv chunk to find rows with drug names in ref_drug_names.
    Uses clean_brand_name to prep drug names in chunk row before checking 
    against ref_drug_names.
    Args:
        chunk (pd.DataFrame)
        drug_cols (list): cols to check for drug names [Brnd_Name,Gnrc_Name]
        ref_drug_names (list): drug names to check for
    Returns:
        filtered_chunk: pd.DataFrame
    """
    chunk_row_idx = []
    chunk_matched_rows = 0
    # iterate through rows
    for idx, row in chunk.iterrows():
        # iterate through drug_cols
        for col in drug_cols:
            drug_name = str(row[col])
            if pd.isna(drug_name) or drug_name == 'nan':
                continue
            elif drug_name == '':
                continue
            # Apply clean_brand_name and then check against drug_names
            drug_name = clean_brand_name(drug_name)
            for tgt_name in ref_drug_names:
                if tgt_name in drug_name:
                    chunk_row_idx.append(idx)
                    chunk_matched_rows += 1
                    break  # Break out of tgt_name loop once we find a match
            if tgt_name in drug_name:  # If we found a match in drug_names loop
                break  # Break out of col loop to move to next row, else move to next column
    
    filtered_chunk = chunk.loc[chunk_row_idx] # using row labels, not positions, so changed from iloc to loc
    return filtered_chunk


def filter_prescribers_by_drug_names(path_in, dir_out):
    """
    Filter Prescribers data to find qualifying NPIs. Saves filtered
    chunks (with matches) to individual csv files.
    Args:
        path_in (str): path to input file
            Filename: data/filtered/prescribers/prescribers_filtered_prscrb_type.csv
            Cols: [Prscrbr_NPI,Prscrbr_Type,Brnd_Name,Gnrc_Name,Year]
        dir_out (str): path to directory where filtered chunks are saved, 
            ending with "/"
            Filenames: dir_out/prescribers_chunk_{i+1}.csv
    """
    drug_names = ['bicalutamide', 'abiraterone', 'enzalutamide', 'apalutamide', 'darolutamide']

    # Chunk the df prescribers_filtered_type into 100_000 rows, then filter each chunk
    chunksize = 100_000
    chunks = pd.read_csv(path_in, chunksize=chunksize, encoding='latin-1')
    # Filter rows with drug names in Brnd_Name or Gnrc_Name
    total_matched_rows = 0
    drug_cols = ['Brnd_Name', 'Gnrc_Name']

    for i, chunk in enumerate(chunks):
        filtered_chunk = find_matches_prescribers(chunk, drug_cols, drug_names)
        # save filtered chunk to csv if not empty
        if not filtered_chunk.empty:
            filtered_chunk.to_csv(f"{dir_out}prescribers_chunk_{i+1}.csv", index=False)
            logger.info("Saved chunk %s, found %s matches", i+1, len(filtered_chunk))
            total_matched_rows += len(filtered_chunk)
    
    logger.info("Matched %s rows for prescribers", total_matched_rows)


# Step 2: Group by id and get sorted unique years where target_names appeared
def get_final_npis(pathin_filtered_prescribers, pathout_final_npis):
    """
    Get set of NPIs, per year, of prescribers who prescribed any of the target drugs in
    previous 3 consecutive years.
    Args:
        pathin_filtered_prescribers (str): path to csv of prescribers filtered 
            by prescriber type and drug names
            Filename: data/filtered/prescribers/prescribers_filtered_type_drug_names.csv
            Cols: [Prscrbr_NPI,Prscrbr_Type,Brnd_Name,Gnrc_Name,Year]
        pathout_final_npis (str): path to output json with final set of NPIs per year
            Filename: data/filtered/prescribers/prescribers_year2npis.json
            Format: {year: [npis]}
    """
    df = pd.read_csv(pathin_filtered_prescribers, dtype=str)
    npi_groups = df.groupby('Prscrbr_NPI')
    npi_years = npi_groups.agg({'Year': list}).reset_index()

    year2npis = defaultdict(list)

    for idx, row in npi_years.iterrows():
        years = row['Year']
        years = sorted(set(years))
        npi = row['Prscrbr_NPI']
    
        years_int = set(int(y) for y in years)

        for year in range(2014, 2024):
            if year == 2014:
                if 2013 in years_int: # for 2014, years = ['2013']
                    year2npis[str(year)].append(npi)
            elif year == 2015:
                if 2013 in years_int and 2014 in years_int: # for 2015, years = [2013', '2014']
                    year2npis[str(year)].append(npi)
            else:
                if all(prev in years_int for prev in [year - 1, year - 2, year - 3]):
                    year2npis[str(year)].append(npi)

    # deduplicate npis
    for year in year2npis.keys():
        year2npis[year] = list(set(year2npis[year]))
    
    # Convert defaultdict to dict for saving to json
    year2npis = dict(year2npis)
    with open(pathout_final_npis, 'w') as f:
        json.dump(year2npis, f)



def main():
    # Add Year column to raw prescriber chunks
    add_years_to_raw_prescriber_chunks("data/raw/prescribers/chunks/", "data/raw/prescribers/with_years/")
    print("Finished adding years to raw prescriber chunks")
    # Concatenate raw prescribers chunks (already filtered by prescriber type)
    concatenate_chunks("data/raw/prescribers/with_years/", "data/filtered/prescribers/prescribers_filtered_prscrb_type.csv")
    print("Finished concatenating prescribers chunks")
    
    # Filter Prescribers by drug names, then save in chunks
    dir_prescribers_filtered_chunks = "data/filtered/prescribers/chunks/"
    filter_prescribers_by_drug_names("data/filtered/prescribers/prescribers_filtered_prscrb_type.csv", dir_prescribers_filtered_chunks)
    print("Finished filtering prescribers by drug names")
    # Concatenate filtered prescribers chunks
    concatenate_chunks(dir_prescribers_filtered_chunks, "data/filtered/prescribers/prescribers_filtered_type_drug_names.csv")
    print("Finished concatenating filtered prescribers chunks")
    
    # # 3. Get target set of NPIs and save to CSV
    year2npis_path = "data/filtered/prescribers/prescribers_year2npis.json"
    get_final_npis("data/filtered/prescribers/prescribers_filtered_type_drug_names.csv", year2npis_path)
    print("Finished getting final npis")


if __name__ == "__main__":
    main()