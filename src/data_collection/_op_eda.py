import logging
import os
import glob
from typing import List, Set
from tqdm import tqdm

import pandas as pd
import numpy as np

import seaborn as sns
import matplotlib.pyplot as plt
from rapidfuzz.distance import Levenshtein

from src.data_collection._utils import get_drug_columns
from src.data_collection._utils import (setup_logging, clean_brand_name)


setup_logging()
logger = logging.getLogger(__name__)


def plot_save_hist(datalist, fileout, xlabel, ylabel, title):    
    plt.figure()
    sns.histplot(data=datalist)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.savefig(fileout)
    plt.close()


def plot_save_violin(datalist, fileout, xlabel, title, xtick_step):    
    plt.figure(figsize=(10, 6))
    sns.violinplot(x=datalist)

    # Set tick frequency
    data_min, data_max = min(datalist), max(datalist)
    ticks = np.arange(data_min, data_max + 1, xtick_step)
    plt.xticks(ticks)

    plt.xlabel(xlabel)
    plt.title(title)
    plt.savefig(fileout)
    plt.close()


def get_drug_data(df, drug_cols, filename, name_lengths, drug_names_uniques, drug_names_all):
    for col in drug_cols:
        # get lengths of all values (after applying cleaning function)
        lengths = df[col].apply(clean_brand_name).str.len().tolist()
        lengths = [x for x in lengths if x > 0]
        name_lengths.extend(lengths)

        # get names after cleaning and removing empty strings
        names = df[col].dropna().apply(clean_brand_name).values.tolist()
        names = [x for x in names if x != '']
        drug_names_all.extend(names) # get all names
        drug_names_uniques.update(set(names)) # get uniques
    
    print(f"Found {len(name_lengths)} new non-empty values by length") # stats for entire file
    print(f"Found {len(drug_names_all)} drug names by name") # stats for entire file
    return name_lengths, drug_names_uniques, drug_names_all


def save_drug_data(drug_data, name_lengths, drug_names_uniques, drug_names_all):
    drug_names_uniques.discard('')
    
    drug_data['len_all_drug_names'] = name_lengths
    drug_data['unique_drug_names'] = drug_names_uniques
    drug_data['all_drug_names'] = drug_names_all

    return drug_data


def get_op_drug_data(dataset):
    dir_samples = f"data/sample/{dataset}/"

    drug_data = {}
    name_lengths = [] # need all occurrences of a drug name in OP datasets for distribution plot
    drug_names_uniques = set()
    drug_names_all = []

    for path in tqdm(glob.iglob(dir_samples+"*.parquet")):
        filename = os.path.basename(path).split(".parquet")[0]
        print(f"Reading file: {filename}")
        df = pd.read_parquet(path)

        # get drug_cols
        drug_cols = get_drug_columns(df)

        # get unique names and all lengths
        name_lengths, drug_names_uniques, drug_names_all = get_drug_data(df, drug_cols, filename, name_lengths, drug_names_uniques, drug_names_all)

    # save to dict
    return save_drug_data(drug_data, name_lengths, drug_names_uniques, drug_names_all)


def get_ref_drug_data():
    filename = "data/reference/ProstateDrugList.csv"
    print(f"Reading file: {filename}")

    drug_data = {}
    name_lengths = []
    drug_names_uniques = set()
    drug_names_all = []

    df = pd.read_csv(filename)

    # get drug cols
    prefix = "Brand_name"
    drug_cols = [col for col in df.columns if col.lower().startswith(prefix.lower())]

    # get unique names and all lengths
    name_lengths, drug_names_uniques, drug_names_all = get_drug_data(df, drug_cols, filename, name_lengths, drug_names_uniques, drug_names_all)
    
    # save to dict
    return save_drug_data(drug_data, name_lengths, drug_names_uniques, drug_names_all)


def get_levenshtein_distances(ref_names: Set, op_names: Set) -> List:
    distances = []

    # # Compute Levenshtein distance for every combination
    # for word1 in tqdm(op_names, desc="Computing distances"):
    #     distances.extend([(word1, word2, Levenshtein.distance(word1, word2)) for word2 in ref_names])

    # compute and log distances around a threshold
    for word1 in tqdm(op_names, desc="Computing distances"):
        results = [
            (word1, word2, dist)
            for word2 in ref_names
            if (dist := Levenshtein.distance(word1, word2)) < 7
        ]
        distances.extend(results)

    # # Now print the results
    # for word1, word2, dist in distances:
    #     logger.info("Ref_name,OP_name,Distance\n%s,%s,%s", word1, word2, dist)
    
    # logger.info("Number of pairs with distance < 2: %s", len(results))

    # Convert to a NumPy array for analysis
    distances = np.array(distances)

    return distances


def runner(dataset):
    op_drug_data = get_op_drug_data(dataset)
    print(f"Summary statistics on OP Gnrl length of all drug names:\n{pd.Series(op_drug_data['len_all_drug_names']).describe()}")
    print(f"Number of all OP drug names: {len(op_drug_data['all_drug_names'])}")
    # plot_save_violin(
    #     datalist=op_drug_data["len_all_drug_names"],
    #     fileout="data/OP_Gnrl_Drug_Lengths_2013-2023_10k.png",
    #     xlabel="Number Characters",
    #     title="Lengths of All OP Rsrch Drug Names (2013-2023, 10k sample)",
    #     xtick_step=5
    # )

    ref_drug_data = get_ref_drug_data()
    print(f"Summary statistics on Ref length of all drug names:\n{pd.Series(ref_drug_data['len_all_drug_names']).describe()}")
    print(f"Number of all Ref drug names: {len(ref_drug_data['all_drug_names'])}")
    # plot_save_violin(
    #     datalist=ref_drug_data["len_all_drug_names"],
    #     fileout="data/Ref_Drug_Lengths_2013-2023_10k.png",
    #     xlabel="Number Characters",
    #     title="Lengths of All Reference Drug Names (2013-2023, 10k sample)",
    #     xtick_step=5
    # )

    # uniques_distances = get_levenshtein_distances(ref_drug_data["unique_drug_names"], op_drug_data["unique_drug_names"])
    all_distances = get_levenshtein_distances(ref_drug_data["all_drug_names"], op_drug_data["all_drug_names"])
    # plot_save_violin(
    #     datalist=all_distances,
    #     fileout="data/levenshtein_distances_2013-2023_Gnrl_10k.png",
    #     xlabel="Levenshtein Distance",
    #     title="Levenshtein Distances: OP Gnrl and Ref Drug Names (all, 10k sample, 2013-2023)",
    #     xtick_step=5
    # )

    logger.info("Length of distances: %s", len(all_distances))

    return op_drug_data, ref_drug_data, all_distances

    # dist0 = 0
    # dist1 = 0
    # dist2 = 0
    # dist3 = 0
    # dist4 = 0
    # dist5 = 0
    # dist6 = 0
    
    # for dist in all_distances:
    #     if int(dist[2]) == 0:
    #         dist0 += 1
    #     elif int(dist[2]) == 1:
    #         dist1 += 1
    #     elif int(dist[2]) == 2:
    #         dist2 += 1
    #     elif int(dist[2]) == 3:
    #         dist3 += 1
    #     elif int(dist[2]) == 4:
    #         dist4 += 1
    #     elif int(dist[2]) == 5:
    #         dist5 += 1
    #     elif int(dist[2]) == 6:
    #         dist6 += 1

    # logger.info("Number of pairs with dist less than 1: %s", dist0)
    # logger.info("Number of pairs with dist between 1 (inclusive) and 2: %s", dist1)
    # logger.info("Number of pairs with dist between 2 (inclusive) and 3: %s", dist2)


def main():
    op_drug_data, ref_drug_data, all_distances = runner("10k/general_payments")


    
        
    



if __name__ == "__main__":
    main()