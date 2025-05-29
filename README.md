# Project Summary
* Get filtered data from two CMS databases ([Open Payments](https://openpaymentsdata.cms.gov/) and [Medicare Part D Prescribers - by Provider and Drug](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug))
* Clean and merge the datasets for every year (2014-2023)
* Add new columns for downstream analysis

## Filtering Conditions
### Open Payments (OP)
* Get all rows with a drug name matching any of the generic and brand names in data/reference/ProstateDrugList.csv
* OP drug name columns start with:
   * 2014-2015: "Name_of_Associated_Covered_Drug_or_Biological", "Name_of_Associated_Covered_Device_or_Medical_Supply"
   * 2016-2023: "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_"
* Filter two OP databases: General and Research (not Ownership)

### Prescribers
* Filter by prescriber type: Radiation Oncology, Hematology-Oncology, Medical Oncology, Hematology, Urology
* Then filter for these drugs: bicalutamide, abiraterone, enzalutamide, apalutamide, or darolutamide
* Of these, get the unique NPIs, per year, for which the following is true:
    * Any of the following drugs was prescribed in 3 consecutive years: bicalutamide, abiraterone, enzalutamide, apalutamide, or darolutamide
    * E.g. for 2022, get the NPIs that prescribed at least one of the five drugs above in 2019, 2020, and 2021. (Does not need to be the same drug across the three years.)

## Final Dataset
* 1 table per year (2014-2023)
* New columns:
    * Prostate_Drug_Type: 1/0 (drugs listed as "yellow" in data/reference/ProstateDrugList.csv)
    * Onc_Prescriber: 1/0 (Prostate_Drug_Type = 1 AND the prescriber's NPI is in the "List of Unique NPIs" for that year)
    * Drug_Name: Generic name (following map in data/reference/ProstateDrugList.csv)

# Data
1. Open Payments
Manually [downloaded](https://www.cms.gov/priorities/key-initiatives/open-payments/data/dataset-downloads) full csv files per year. (Filtering functionalities with API is limited and relatively slow.)
2. Prescribers
Manually [downloaded](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug) in chunks by prescriber type (Radiation Oncology, Hematology-Oncology, Medical Oncology, Hematology, Urology) because of limited filtering functionality through API.

# Code (in folder src)
1. main.py

Summary: Filters OP csv files, then adds columns using the filtered Prescriber Part D data and saves final files to csv.

Inputs: 
* Raw annual General and Research OP csv files (manually downloaded)
* Filtered Prescribers Part D data outputted by filter_prescribers.py (data/filtered/prescribers/prescribers_year2npis.json)

Steps:
* For every annual General and Research csv file:
  * In 100k chunks, filters the csv file keeping rows that match the OP filtering condition. Any matching rows per chunks are saved to csv as chunks are processed.
  * Concatenates the filtered chunks into 1 file per year.
  * Cleans and enhances the file by harmonizing column names and adding three new columns (Prostate_Drug_Type, Onc_Prescriber, Drug_Name)
  * Saves the final file to csv

Output: csv files containing the final annual dataset

2. filter_prescribers.py

Summary: Filters Prescriber Part D data and produces a JSON file saving NPIs by year

Input: Prescriber chunks by prescriber type (manually downloaded)

Steps:
* Add year column to each chunk, then concatenate into one file
* Filter the full file (in chunks of 100k rows) keeping rows with values in columnd 'Brnd_Name' or 'Gnrc_Name' that match any of 'bicalutamide', 'abiraterone', 'enzalutamide', 'apalutamide', 'darolutamide'
* Get all NPIs that match the Prescribers filtering condition.

Output: JSON file mapping "Year" : List of unique NPIs

3. filter_op.py

Contains all functions used for filtering OP data. Runner function called in main.py is filter_open_payments.

4. clean_final_tables.py

Contains all functions used for cleaning and enhancing the filtered OP data files. Runner function called in main.py is run_op_cleaner.

5. fix_final_generic_names.py

Fixes the formatting of the generic names in the Drug_Name column added in main.py.

6. get_providers.py

Gets data needed to add missing NPIs for 2014 from CMS' [Covered Recipient Profile Supplement](https://openpaymentsdata.cms.gov/dataset/23160558-6742-54ff-8b9f-cac7d514ff4e)

Input: manually downloaded raw data

Output: csv with 2 columns, 'Covered_Recipient_Profile_ID', 'Covered_Recipient_NPI'

7. _utils.py

General helper functions used across all files

