import pandas as pd

from src.data_collection._utils import (
    load_drug_data,
    validate_drug_data,
)

from src.data_collection._op_get_data_csv import (
    get_data_slice,
)

def main():
    # 1. get op csv file
    large_csv_path = "data/raw/general_payments/OP_DTL_GNRL_PGYR2023_P01302025_01212025.csv"
    
    # 2. load drug names data from ProstateDrugList.csv
    prostate_drug_data = load_drug_data()
    validate_drug_data(prostate_drug_data)

    # Load csv in chunks
    chunksize = 100000
    chunks = pd.read_csv(large_csv_path, chunksize=chunksize)
    
    # Process each chunk
    for i, chunk in enumerate(chunks):
        op_drug_names = chunk["Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1"].unique()
        for op_drug_name in op_drug_names:
            print(op_drug_name)
        break


    # # 4. get filtered data
    # pathout = "data/filtered/general_payments/general_payments_2023.parquet"
    # filtered_df = get_data_slice(large_csv_path, pathout, prostate_drug_data)

    # 3. clean data

    # 4. save to parquet file


if __name__ == "__main__":
    main()