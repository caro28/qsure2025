# Project Summary
* Get filtered data from two CMS databases (Open Payments and Part D Prescribers)
* Clean and merge the datasets for every year (2014-2023)
* Add new columns for downstream analysis

## Filtering Conditions
### Open Payments (OP)
* Get all rows with drug names matching those in data/reference/ProstateDrugList.csv
* Filter two OP databases: General and Research

### Prescribers
* Filter by prescriber type: Radiation Oncology, Hematology-Oncology, Medical Oncology, Hematology, Urology
* Then filter for these drugs: bicalutamide, abiraterone, enzalutamide, apalutamide, or darolutamide
* Of these, get the unique NPIs, per year, for which the following is true:
    * Any of the following drugs was prescribed in 3 consecutive years: bicalutamide, abiraterone, enzalutamide, apalutamide, or darolutamide
* For every year, build a map of "Year" : "List of unique NPIs" that meet this condition

## Final Dataset
* 1 table per year (2014-2023)
* New columns:
    * Prostate_Drug_Type: 1/0 (drugs listed as "yellow" in data/reference/ProstateDrugList.csv)
    * Onc_Prescriber: 1/0 (Prostate_Drug_Type = 1 AND the prescriber's NPI is in the "List of Unique NPIs" for that year)
    * Drug_Name: Generic name (following map in data/reference/ProstateDrugList.csv)

# Code
## Filtering, Cleaning, Merging
1. filter_prescribers.py
2. filter_op.py
3. clean_final_tables.py

## Others
1. _utils.py

2. fix_final_generic_names.py
* Will be added to clean_final_tables.py and then deleted

3. _op_eda.py
