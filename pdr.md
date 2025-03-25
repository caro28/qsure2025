# Project Summary
* Get filtered data from two CMS databases (Open Payments and Part D Prescribers)
* Clean the data
* Add new columns

# Open Payments Data (OP)
1. Filter and Get data
* Get all rows from OP on matching drug names with ProstateDrugList.csv
* In OP, the drug names can be in any column that begins with Name_of_drug_or_biological_or_device_or_medical_supply_
* In ProstateDrugList.csv, the drug names can be in any column that begins with Brand_name
* Do this for every year from 2014-2023 and for the three OP dataset types: General, Research, Ownership Interests
* Save filtered data in individual csv files for each year and OP dataset type

