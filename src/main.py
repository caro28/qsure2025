import logging

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
)

from src.clean_final_tables import (
    run_op_cleaner,
)

setup_logging()
logger = logging.getLogger(__name__)    



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

    # # 2. Filter Prescribers
    # # Concatenate raw prescribers chunks (already filtered by prescriber type)
    # prescribers_chunks_dir = "data/filtered/prescribers/chunks/"
    # prescribers_filtered_prscrb_type = "data/filtered/prescribers/prescribers_filtered_prscrb_type.csv"
    # concatenate_chunks(prescribers_chunks_dir, prescribers_filtered_prscrb_type)
    # print("Finished concatenating prescribers chunks")
    # # Filter Prescribers by drug names
    # prescribers_filtered_drug_names = "data/filtered/prescribers/prescribers_filtered_type_drug_names.csv"
    # filter_prescribers_by_drug_names(prescribers_filtered_prscrb_type, prescribers_filtered_drug_names)
    # print("Finished filtering prescribers by drug names")
    
    # # 3. Get target set of NPIs and save to CSV
    # get_final_npis(prescribers_filtered_drug_names)
    # print("Finished getting final npis")
    
    # # 4. Harmonize Open Payments data and Add columns/flags
    # for year in years:
    #     # for dataset_type in ["general", "research"]:
    #     for dataset_type in dataset_types:
    #         filtered_op_files_dir = f"data/filtered/{dataset_type}_payments/"
    #         run_op_cleaner(filtered_op_files_dir, dataset_type)
    #         print(f"Finished cleaning {dataset_type} payments for {year}")



if __name__ == "__main__":
    main()
