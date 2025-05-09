import os
import pandas as pd

from src._utils import clean_generic_name



def get_final_generic_names(ref_drug_names_path):
    ref_df = pd.read_csv(ref_drug_names_path)
    generic_names = ref_df['Generic_name']
    generic_names_cleaned = [clean_generic_name(name) for name in generic_names]
    generic_names_final = []
    trailing_tokens = [" Y PO", " PO", " IV", " IM", " SUBQ"]
    for name in generic_names:
        for trailing in trailing_tokens:
            if name.endswith(trailing):
                name = name[:-len(trailing)]
                name = name.strip()  # Strip again if any extra spaces remain
                generic_names_final.append(name)
                break
    # add one name without the trailing
    generic_names_final.append("PSMA-Lutetium-177")
    generics_cleaned2final = dict(zip(generic_names_cleaned, generic_names_final))
    return generics_cleaned2final


def replace_generic_names(df, generics_cleaned2final):
    df_copy = df.copy()
    for idx, row in df_copy.iterrows():
        generic_cleaned = row['Drug_Name']
        try:
            final_generic = generics_cleaned2final[generic_cleaned]
            df_copy.at[idx, 'Drug_Name'] = final_generic
        except KeyError:
            raise KeyError("Unsupported value in 'Drug_Name'.")
    return df_copy


def get_final_files(file_path, generics_cleaned2final, dir_out):
    df = pd.read_csv(file_path)
    filename = os.path.basename(file_path)
    new_filename = f"{filename.split('.csv')[0]}_final.csv"

    final_df = replace_generic_names(df, generics_cleaned2final)
    print(f"Saving corrected file to {os.path.join(dir_out + new_filename)}")
    final_df.to_csv(os.path.join(dir_out, new_filename), index=False)



def main():
    # dataset_types = ["general, research"]
    dataset_types = ["research"]
    parent_dir = "data/final_files/"
    generics_cleaned2final = get_final_generic_names("data/reference/ProstateDrugList.csv")
    dir_out = "data/final_files/final_generics/"
    for dataset_type in dataset_types:
        dataset_dir = f"{dataset_type}_payments/"
        for file in os.listdir(parent_dir + dataset_dir):
            # ignore missing_npis dir
            file_path = os.path.join(parent_dir + dataset_dir, file)
            if os.path.isfile(file_path) and file_path.endswith(".csv"):
                print(f"Processing file {file_path}")
                get_final_files(file_path, generics_cleaned2final, dir_out)


if __name__ == '__main__':
    main()