import requests
import pandas as pd
import time

# Define the mapping of years to their dataset UUIDs
year_uuids = {
    2022: 'b101b457-ffa4-49bb-8fd9-27c1266086e2',
    # 2021: 'f68114ed-f854-4ffc-9c6e-ed78b5e2f8d0',
    # 2020: '7795fe20-e80e-435a-a9ed-d2d65e05feeb',
    # 2019: '2a6705e6-7a1e-460c-ba22-35249a531918',
    # 2018: '802fe556-311f-4962-8d75-d5f4ff405884',
    # 2017: '05f108dd-76c4-49f4-9fdc-788d8f4251ec',
    # 2016: '25106f9d-0eb8-4ba7-b237-486ee87d910a',
    # 2015: '1d650894-8afe-4056-ba31-a85cb0e3cee6',
    # 2014: '0779bc8d-18dd-40b8-9d61-7addc8b0daf1',
    # 2013: 'c6905d43-45de-470d-897c-9ed8e75e256d'
}

# drug_names = ['bicalutamide', 'abiraterone', 'enzalutamide', 'apalutamide', 'darolutamide']
drug_names = ['bicalutamide', 'abiraterone',]
prscrb_type = ['Radiation Oncology', 'Hematology-Oncology', 'Medical Oncology', 'Hematology', 'Urology']

# Base parameters for the API request
base_params = {
    'column': 'Prscrbr_NPI,Prscrbr_Type,Brnd_Name,Gnrc_Name',
    'size': 1000  # Increased page size for efficiency
}

def fetch_all_pages(base_url, initial_params, year, drug):
    """
    Fetch all pages of data for a given drug and year combination
    """
    all_records = []
    offset = 0
    total_records = 0
    
    while True:
        # Add offset to parameters
        params = initial_params.copy()
        params['offset'] = offset
        
        try:
            # Add delay to avoid rate limiting
            time.sleep(0.1)
            
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                if not data:  # No more data available
                    break
                    
                # Add year column to each record
                for record in data:
                    record['Year'] = year
                
                records_count = len(data)
                all_records.extend(data)
                total_records += records_count
                print(f"Fetched {records_count} records for {drug} ({year}), total: {total_records}")
                
                if records_count < params['size']:  # Last page
                    break
                    
                offset += records_count
            else:
                print(f"Failed to fetch data for {year}, {drug}: {response.status_code}")
                break
                
        except Exception as e:
            print(f"Error fetching data for {year}, {drug}: {str(e)}")
            break
    
    return all_records

all_data = []

# Iterate through each year and drug combination
for year, uuid in year_uuids.items():
    base_url = f'https://data.cms.gov/data-api/v1/dataset/{uuid}/data'
    print(f"\nProcessing year {year}...")
    
    # for drug in drug_names:
    for drug in drug_names:
        params = base_params.copy()
        params['filter[drug-group][group][conjunction]'] = 'OR'

        # Brand name condition in the OR group
        params['filter[brd_name][condition][path]'] = 'Brnd_Name'
        params['filter[brd_name][condition][operator]'] = 'CONTAINS'
        params['filter[brd_name][condition][value]'] = drug
        params['filter[brd_name][condition][memberOf]'] = 'drug-group'
        
        # Generic name condition in the OR group
        params['filter[gnc_name][condition][path]'] = 'Gnrc_Name'
        params['filter[gnc_name][condition][operator]'] = 'CONTAINS'
        params['filter[gnc_name][condition][value]'] = drug
        params['filter[gnc_name][condition][memberOf]'] = 'drug-group'



        # Fetch all pages for this drug and year
        year_drug_data = fetch_all_pages(base_url, params, year, drug)
        all_data.extend(year_drug_data)
        
        print(f"Total records for {drug} in {year}: {len(year_drug_data)}")

# Convert to DataFrame
data = pd.DataFrame(all_data)

if not data.empty:
    # Display summary statistics
    print("\nData Collection Summary:")
    print("------------------------")
    print(f"Total records: {len(data)}")
    
    # Show records by year
    print("\nRecords by year:")
    print(data['Year'].value_counts().sort_index())
    
    # Show records by drug
    print("\nRecords by Brand Name:")
    print(data['Brnd_Name'].value_counts())
    print("\nRecords by Generic Name:")
    print(data['Gnrc_Name'].value_counts())

    # Create output filename with year range and drugs
    output_file = "medicare_data_2013-2022_full.json"
    
    # Save to JSON
    data.to_json(output_file, orient='records', indent=2)
    print(f"\nData saved to {output_file}")
    
else:
    print("No data found for any year or drug name")
