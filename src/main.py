import logging
import os
import time

from src._utils import (
    setup_logging,
    concatenate_chunks,
)

from src.filter_op import (
    filter_open_payments,
)

from src.filter_prescribers import (
    filter_prescribers_by_drug_names,
    get_final_npis,
    add_years_to_raw_prescriber_chunks,
)

from src.clean_final_tables import (
    run_op_cleaner,
)

setup_logging()
logger = logging.getLogger(__name__)    



def main():
    start_time = time.time()
    
    # # 1. Filter Open Payments (2014-2023)
    years = range(2023, 2024) # Use 2023 only for now to draft code
    dataset_types = ["general"]
    # # Filter in chunks and save intermediary files
    # for dataset_type in dataset_types:
    #     for year in years:
    #         filter_open_payments(year, dataset_type)
    #         print(f"Finished filtering {dataset_type} payments for {year}")
    
    # # Concatenate intermediary files
    # for dataset_type in dataset_types:
    #     op_chunks_dirs = f"data/filtered/{dataset_type}_payments/chunks/"
    #     for year in years:
    #         output_file = f"data/filtered/{dataset_type}_payments/full_files/{dataset_type}_{year}.csv"
    #         concatenate_chunks(op_chunks_dirs, output_file)
    #         print(f"Finished concatenating {dataset_type} payments for {year}")

    ############### Do not need to re-run with every new year and dataset_type ###############
    # TODO: Add to a function, then add a check to see if the files already exist, and skip if yes
    # 2. Filter Prescribers
    # Add Year column to raw prescriber chunks
    # add_years_to_raw_prescriber_chunks("data/raw/prescribers/chunks/", "data/raw/prescribers/with_years/")
    # print("Finished adding years to raw prescriber chunks")
    # # Concatenate raw prescribers chunks (already filtered by prescriber type)
    # concatenate_chunks("data/raw/prescribers/with_years/", "data/filtered/prescribers/prescribers_filtered_prscrb_type.csv")
    # print("Finished concatenating prescribers chunks")
    
    # # Filter Prescribers by drug names, then save in chunks
    # dir_prescribers_filtered_chunks = "data/filtered/prescribers/chunks/"
    # filter_prescribers_by_drug_names("data/filtered/prescribers/prescribers_filtered_prscrb_type.csv", dir_prescribers_filtered_chunks)
    # print("Finished filtering prescribers by drug names")
    # # Concatenate filtered prescribers chunks
    # concatenate_chunks(dir_prescribers_filtered_chunks, "data/filtered/prescribers/prescribers_filtered_type_drug_names.csv")
    # print("Finished concatenating filtered prescribers chunks")
    
    # # 3. Get target set of NPIs and save to CSV
    # npis_pathout = "data/filtered/prescribers/prescribers_final_npis.csv"
    # get_final_npis("data/filtered/prescribers/prescribers_filtered_type_drug_names.csv", npis_pathout)
    # print("Finished getting final npis")
    ###########################################################################################
    
    # 4. Harmonize Open Payments data and Add columns/flags
    for year in years:
        # for dataset_type in ["general", "research"]:
        for dataset_type in dataset_types:
            filtered_op_files_dir = f"data/filtered/{dataset_type}_payments/full_files/"
            run_op_cleaner(filtered_op_files_dir, dataset_type)
            print(f"Finished cleaning {dataset_type} payments for {year}")

    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f"Total execution time: {elapsed_time:.2f} seconds")



if __name__ == "__main__":
    main()
