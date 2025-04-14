import logging
import time

from src._utils import (
    setup_logging,
    concatenate_chunks,
)

from src.filter_op import (
    filter_open_payments,
)

from src.clean_final_tables import (
    run_op_cleaner,
)

setup_logging()
logger = logging.getLogger(__name__)    



def main():
    # 1. Filter Prescribers: one-time filtering; done separately using filter_prescribers.py
    year2npis_path = "data/filtered/prescribers/prescribers_year2npis.json"
    
    # 2. Filter Open Payments (2014-2023) in chunks by target drug names
    prostate_drug_list_path = "data/reference/ProstateDrugList.csv"
    years = range(2014, 2024)
    dataset_types = ["general", "research"]
    # Filter in chunks and save intermediary files
    for dataset_type in dataset_types:
        for year in years:
            start_time = time.time()
            logger.info("Processing %s, %s", dataset_type, year)
            filter_open_payments(year, dataset_type, prostate_drug_list_path)
            logger.info("Finished filtering %s payments for %s", dataset_type, year)
            # Concatenate filtered chunks and save to full file
            op_chunks_dirs = f"data/filtered/{dataset_type}_payments/{year}_chunks/"
            filtered_op_file = f"data/filtered/{dataset_type}_payments/full_files/{dataset_type}_{year}.csv"
            concatenate_chunks(op_chunks_dirs, filtered_op_file)
            logger.info("Finished concatenating %s payments for %s", dataset_type, year)

            # 3. Clean Open Payments data and Save to csv
            logger.info(f"Cleaning {dataset_type} payments for {year}")
            run_op_cleaner(filtered_op_file, dataset_type, year, year2npis_path)
            logger.info("Finished cleaning %s payments for year %s")

            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.info("Total execution time for %s, %s: %.2f seconds", dataset_type, year, elapsed_time)



if __name__ == "__main__":
    main()
