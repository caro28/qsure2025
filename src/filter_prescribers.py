import pandas as pd
import logging

from src._utils import (
    setup_logging,
    concatenate_chunks,
    find_matches,
)

setup_logging()
logger = logging.getLogger(__name__)



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


def get_set_npis():
    # load npi_list
    npi_path="data/filtered/prescribers/prescribers_final_npis.csv"
    npi_df = pd.read_csv(npi_path)
    npi_set = npi_df['Prscrbr_NPI']
    assert len(npi_set.value_counts().unique()) == 1
    return npi_set