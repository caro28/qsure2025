import os
import glob
from tqdm import tqdm
import pandas as pd



def main():
    # get dir where raw op csv's saved 
    dir_in = "data/raw/general_payments/"

    # get dir where we'll save the sample files

    dir_out = "data/sample/10k/general_payments/"
    
    for path in tqdm(glob.iglob(dir_in+"*.csv")):
        filename = os.path.basename(path).split(".csv")[0]
        output_path = dir_out + filename + ".parquet"

        # Skip if sample file already exists
        if os.path.isfile(output_path):  # isfile() specifically checks for files
            print(f"Skipping {filename}, already exists")
            continue

        print(f"Starting to load file {filename} to extract 10k sample")
        
        # take random sample of 100k rows
        op_df = pd.read_csv(path)
        print("Finished loading file")
        
        sample = op_df.sample(n=10000)
        sample = sample.fillna('').astype(str)
        # save to parquet file
        sample.to_parquet(output_path)

        
        
        


if __name__ == "__main__":
    main()