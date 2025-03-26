# Project Summary
* Get filtered data from two CMS databases (Open Payments and Part D Prescribers)
* Clean the data and merge the tables
* Add new columns

# Filtering conditions
## OP
* Match on drug names in ProstateDrugList.csv (already shortened to include only the highlighted rows in original spreadsheet)
## Prescribers
* Get NPIs of providers who prescribed any one or more of the following drugs during 3 consecutive calendar years: bicalutamide, abiraterone, enzalutamide, apalutamide, or darolutamide.
* To meet this requirement, which drug can be different across the 3 years. Eg., bicalutamide in 2017, bicalutamide in 2018, and abiraterone in 2019 would be sufficient.

# 1. Filter and Get Data
## Open Payments Data (OP)
* Get all rows from OP on matching drug names with ProstateDrugList.csv
* In OP, drug names are in any column that begins with name_of_drug_or_biological_or_device_or_medical_supply_ (camel case in OP)
* In ProstateDrugList.csv, brand names are in Brand_name1 -> Brand_name4 and generic names are in Generic_name
* Do this for every year from 2014-2023 and for two OP dataset types: General Open Payments and Research Open Payments
* Check for duplicate rows using record_id?
* Save filtered data in individual csv files for each year and OP dataset type

## Prescribers
* For every year, filter by "Prscrbr_Type" == any of: Radiation Oncology, Hematology-Oncology, Medical Oncology, Hematology, Urology
* Of these, get the NPIs who prescribed any one or more of the following drugs during 3 consecutive calendar years: bicalutamide, abiraterone, enzalutamide, apalutamide, or darolutamide (all generic names).
* Note that the recipient NPI is named differently in the Open Payments general vs. research datasets. In the research payments files, look for an NPI match in either the recipient NPI field or any of the multiple "principal investigator NPI" fields.
* Save table with a column for NPIs and a column for specialty type

# 2. Clean OP tables
## Harmonize column names
* See .sas files for mapping

## Add column "Prostate_drug_type"
* Use values in "Color" column
* green -> 0, yellow -> 1

## Add column, Drug_Name
* Generic name for any brand name drug, minus the PO/IV/IM/SUBQ suffix

# 3. Merge Datasets
## Add column, "Onc_prescriber"
* Onc_prescriber = 1 if Prostate_drug_type = 1 AND the recipient NPI is in the set of NPIs created from Prescribers
* Onc_prescriber = 0 if not

# 4. Save csv files
* One csv file per year, per OP dataset type

######################## IMPLEMENTATION PLAN ###############################

# Step 1a: Open Payments Filtering
def filter_open_payments(year, dataset_type):
    """
    Filter Open Payments data for a given year and dataset type
    (General or Research)
    """
    # Load ProstateDrugList.csv
    # Load OP data for year/type
    # Filter based on drug name matches
    # Save to CSV: f"op_{dataset_type}_{year}.csv"

# Step 1b: Prescribers Processing
def filter_prescribers(years):
    """
    Filter Prescribers data to find qualifying NPIs
    NB: Early years may be missing NPIs; get those from https://openpaymentsdata.cms.gov/dataset/23160558-6742-54ff-8b9f-cac7d514ff4e
    """
    # Load prescriber data for each year
    # Filter by specialty types
    # Find NPIs with 3 consecutive years of target drugs
    # Save qualifying NPIs and specialties

# Step 2: Data Cleaning & Enhancement
def clean_op_data(filepath):
    """
    Clean and enhance Open Payments data
    """
    # Harmonize column names
    # Add Prostate_drug_type (0/1 based on Color)
    # Add Drug_Name (generic names)
    return cleaned_df

def run_op_cleaner():
    # For each payment file
    # Apply cleaning functions
    # Save intermediate results

# Step 3: Data Merging
def merge_datasets(op_data, npi_list):
    """
    Add Onc_prescriber column based on NPI matches
    """
    # Add Onc_prescriber column
    # 1 if Prostate_drug_type=1 and NPI in list
    # 0 otherwise
    return merged_df

# Main
def main():
    # Filter Open Payments (2014-2023)
    years = range(2014, 2024)
    for year in years:
        filter_open_payments(year, "general")
        filter_open_payments(year, "research")
    
    # Filter Prescribers
    npi_list = filter_prescribers(years)
    
    # Clean and merge all files
    for year in years:
        for dataset_type in ["general", "research"]:
            final_data = run_op_cleaner()
            merged_data = merge_datasets(final_data, npi_list)
            merged_data.to_csv(f"final_{dataset_type}_{year}.csv")



##############
column-wise, vectorized filter_chunk function from cursor:

from Levenshtein import distance
import numpy as np

def filter_chunk(chunk, op_drug_cols, ref_drug_names, max_distance=1):
    """Filter chunk based on drug name matches using vectorized operations"""
    # Initialize array to track matches
    matches = np.zeros(len(chunk), dtype=bool)
    
    # Clean and prepare drug data once
    drug_data = chunk[op_drug_cols].fillna('').astype(str).apply(lambda x: x.str.lower())
    ref_names = [name.lower() for name in ref_drug_names]
    
    # Vectorized operations on columns
    for col in op_drug_cols:
        col_values = drug_data[col].values
        for ref_name in ref_names:
            # Vectorized distance calculation for entire column
            distances = np.vectorize(lambda x: distance(x, ref_name))(col_values)
            matches = matches | (distances <= max_distance)
            
    return chunk[matches]