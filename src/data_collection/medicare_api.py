import requests
import pandas as pd
import time
import os
import csv


def fetch_all_pages(base_url, initial_params, year, prscrb_type):
    """
    Fetch all pages of data for a given prscrb_type and year combination
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
                print(f"Fetched {records_count} records for {prscrb_type} ({year}), total: {total_records}")
                
                if records_count < params['size']:  # Last page
                    break
                    
                offset += records_count
            else:
                print(f"Failed to fetch data for {year}, {prscrb_type}: {response.status_code}")
                break
                
        except Exception as e:
            print(f"Error fetching data for {year}, {prscrb_type}: {str(e)}")
            break
    
    return all_records


def concatenate_prescribers(chunks_dir):
    """
    Concatenate all files in dir, add column for year
    """
    output_file = "data/filtered/prescribers/prescribers_filtered_prscrb_type.csv"
    
    # get all files in data/filtered/prescribers/chunks/
    files = os.listdir(chunks_dir)
    
    total_rows = 0

    # Write first file with header
    first_file = files[0]
    # get year from filename
    year = first_file.split("_")[0]
    df = pd.read_csv(f"{chunks_dir}/{first_file}")
    df['Year'] = year
    df.to_csv(output_file, index=False)
    total_rows += df.shape[0]
    # Append remaining files without headers
    for file in files[1:]:
        year = file.split("_")[0]
        df = pd.read_csv(f"{chunks_dir}/{file}")
        df['Year'] = year
        df.to_csv(output_file, mode='a', header=False, index=False)
        print(f"Appended {file}, shape: {df.shape}")  # Debug info
        total_rows += df.shape[0]
    
    # Verify final file
    try:
        final_df = pd.read_csv(output_file)
        print(f"Successfully read final file, shape: {final_df.shape}, total rows: {total_rows}")
    except Exception as e:
        print(f"Error reading final file: {str(e)}")
        # Try reading with error_bad_lines=False to see where the problem is
        final_df = pd.read_csv(output_file, on_bad_lines='warn')
        
    return final_df


def fetch_all_data(year_uuids, prscrb_types, base_params):
    all_data = []

    # Iterate through each year and prescriber type combination
    for year, uuid in year_uuids.items():
        base_url = f'https://data.cms.gov/data-api/v1/dataset/{uuid}/data'
        print(f"\nProcessing year {year}...")
        
        for prscrb_type in prscrb_types:
            print(f"\nProcessing prescriber type {prscrb_type}...")
            params = base_params.copy()

            params['filter[condition][path]'] = 'Prscrbr_Type'
            params['filter[condition][operator]'] = 'EQUALS'
            params['filter[condition][value]'] = prscrb_type

            # Fetch all pages for this prescriber type and year
            year_prscrb_type_data = fetch_all_pages(base_url, params, year, prscrb_type)
            all_data.extend(year_prscrb_type_data)
            
            print(f"Total records for {prscrb_type} in {year}: {len(year_prscrb_type_data)}")

            # Save to csv per year and prescriber type
            output_file = f"data/filtered/prescribers/prescribers_{year}_{prscrb_type}.csv"
            pd.DataFrame(all_data).to_csv(output_file, index=False)
            print(f"\nData saved to {output_file}")



def main():
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

    prscrb_types = ['Radiation Oncology', 'Hematology-Oncology', 'Medical Oncology', 'Hematology', 'Urology']

    # Base parameters for the API request
    base_params = {
        'column': 'Prscrbr_NPI,Prscrbr_Type,Brnd_Name,Gnrc_Name',
        'size': 1000  # Increased page size for efficiency
    }
    # # get rows by prescriber type and write to csv chunks
    # fetch_all_data(year_uuids, prscrb_types, base_params)
    
    # concatenate all prescribers chunks
    # concatenate_prescribers(chunks_dir = "data/filtered/prescribers/chunks/")

    # load prescribers 2022
    data = pd.read_csv("data/filtered/prescribers/prescribers_filtered_prscrb_type.csv")

    print(data.head())
    print(data.shape)
    print(data["Year"].value_counts())
    
    if not data.empty:
        # Display summary statistics
        print("\nData Collection Summary:")
        print("------------------------")
        print(f"Total records: {len(data)}")
        
        # Show records by year
        print("\nRecords by year:")
        print(data['Year'].value_counts().sort_index())
        
        # Show records by prscrb_type
        print("\nRecords by prscrb_type:")
        print(data['Prscrbr_Type'].value_counts())
        # Create output filename with year range and drugs
        output_file = "prescribers_2022.csv"
        
        # Save to csv
        data.to_csv(output_file, index=False)
        print(f"\nData saved to {output_file}")
    
    else:
        print("No data found for any year or prescriber type")




if __name__ == "__main__":
    main()
